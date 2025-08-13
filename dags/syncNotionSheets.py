from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable

from notion_client import Client as NotionClient
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
# from google.oauth2.credentials import Credentials

import json, os, pickle
from datetime import datetime, timedelta

import time

# === 환경 변수 ===
NOTION_TOKEN = Variable.get('NOTION_TOKEN')
PROJECT_DB_ID = Variable.get('PROJECT_DB_ID')
GOOGLE_SHEET_ID = Variable.get('GOOGLE_SHEET_ID')
TEMPLATE_TAB_ID = int(Variable.get('TEMPLATE_TAB_ID'))

TOKEN_PICKLE = '/opt/airflow/temp/token.pickle'
# NOTION_PROJECTS_FILE = '/opt/airflow/temp/notion_projects.json'
SHEET_TABS_INFO = '/opt/airflow/temp/sheet_tabs_info.json'
PROJECT_UPDATE_INFO = '/opt/airflow/temp/project_update_info.json'
# GOOGLE_SHEETS_FILE = '/opt/airflow/temp/google_sheets.json'
CREDENTIALS_FILE = '/opt/airflow/temp/credentials.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# 구글 시트 연결
def get_sheets_service():
    creds = None
    if os.path.exists(TOKEN_PICKLE):
        with open(TOKEN_PICKLE, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PICKLE, 'wb') as token:
            pickle.dump(creds, token)

    return build('sheets', 'v4', credentials=creds)

def load_file_to_dict(path):
    if os.path.exists(path):
        with open(path, "r") as f:
            return dict(json.load(f))
    return dict()

def save_file_to_dict(path, cache):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=4)
        
# === 연결 ===
notion = NotionClient(auth=NOTION_TOKEN)

# 시트 생성
def create_sheets():
    # === 기존 기록 로드 ===
    tabs_dict = load_file_to_dict(SHEET_TABS_INFO)
    
    # if os.path.exists(SHEET_TABS_INFO):
    #     with open(SHEET_TABS_INFO, "r") as f:
    #         tabs_dict = dict(json.load(f))
    # else:
    #     tabs_dict = dict()

    # if os.path.exists(PROJECT_UPDATE_INFO):
    #    with open(PROJECT_UPDATE_INFO, "r") as f:
    #        update_dict = dict(json.load(f))
    # else:
    #    update_dict = dict()
    
    # === 구글 시트 연결
    sheets = get_sheets_service()
    
    # === 노션 데이터 불러오기
    results = notion.databases.query(database_id=PROJECT_DB_ID)["results"]
    
    # 신규 프로젝트
    new_pages = []
    
    # 신규 프로젝트 체크
    for page in results:
        sheet_url = page["properties"]["sheet url"]['url']
        if sheet_url == None:
            new_pages.append(page)
    
    # 최근 생성된 프로젝트일 수록 시트가 마지막에 생성되게끔 정열
    new_pages.reverse()
    # === 시트 복사 및 데이터 삽입
    for page in new_pages:
        props = page["properties"]
        project_name = props["프로젝트명"]["title"][0]["plain_text"]
        new_tab_title = f"{project_name}_결산"
        
        # 탭 복사
        copied_tab = sheets.spreadsheets().sheets().copyTo(
            spreadsheetId=GOOGLE_SHEET_ID,
            sheetId=TEMPLATE_TAB_ID,
            body={"destinationSpreadsheetId": GOOGLE_SHEET_ID}
        ).execute()
        
        # 새로 생긴 결산 탭 gid
        new_tab_id = copied_tab["sheetId"]

        # gid : tab_title 저장
        tabs_dict[new_tab_id] = new_tab_title

        # 탭 이름 변경
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=GOOGLE_SHEET_ID,
            body={
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": new_tab_id,
                                "title": new_tab_title
                            },
                            "fields": "title"
                        }
                    }
                ]
            }
        ).execute()
        
        # 노션에 링크 업데이트
        sheet_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit?gid={new_tab_id}#gid={new_tab_id}"
        
        notion.pages.update(
            page_id=page["id"],
            properties={
                "sheet url": {"url": sheet_url}
            }
        )
        notion.blocks.children.append(
            block_id=page["id"],  # 예: 페이지 ID
            children=[
                {
                    "object": "block",
                    "type": "embed",
                    "embed": {
                        "url": sheet_url  # 여기에 임베드하고 싶은 외부 URL 입력
                    }
                },
                {
                    "object": "block",
                    "type": "heading_2",
                    "heading_2": {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {
                                    "content": "공지사항"
                                }
                            }
                        ]
                    }
                }
            ]
        )
        time.sleep(3)
        
    # 기록 저장
    save_file_to_dict(SHEET_TABS_INFO, tabs_dict)
    # with open(SHEET_TABS_INFO, "w", encoding="utf-8") as f:
    #     json.dump(tabs_dict, f, ensure_ascii=False, indent=4)

    print("✅ 시트 생성 완료")
    time.sleep(5)

# 시트 업데이트
def update_sheets():
    # === 기존 기록 로드 ===
    tabs_dict = load_file_to_dict(SHEET_TABS_INFO)
    
    # === 업데이트  ===
    update_dict = load_file_to_dict(PROJECT_UPDATE_INFO)
    
    # 시트 연결
    sheets = get_sheets_service()
    
    # === 노션 데이터 불러오기
    results = notion.databases.query(database_id=PROJECT_DB_ID)["results"]
    
    # === 시트 복사 및 데이터 삽입
    for page in results:
        props = page["properties"]
        sheet_url = props["sheet url"]['url']
        last_edited_time = props["최종 편집 일시"]["last_edited_time"]
        page_id = page["id"]
        
        if page_id in update_dict and update_dict[page_id] == last_edited_time:
            continue
    
        # 시트 생성이 되지 않은 프로젝트
        if sheet_url == None:
            continue
        
        update_dict[page_id] = last_edited_time
        project_name = props["프로젝트명"]["title"][0]["plain_text"]
        tab_title = f"{project_name}_결산"
        gid = props["sheet url"]["url"].split('gid=')[-1]
        
        if tabs_dict[gid] != tab_title:
            # 탭 타이틀 최신화
            tabs_dict[gid] = tab_title
            
            # 시트 탭 이름 변경
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=GOOGLE_SHEET_ID,
                body={
                    "requests": [
                        {
                            "updateSheetProperties": {
                                "properties": {
                                    "sheetId": gid,
                                    "title": tab_title
                                },
                                "fields": "title"
                            }
                        }
                    ]
                }
            ).execute()
        
        
        project_info = {
            "project_name": project_name,
            "project_type": props["프로젝트 형태"]["select"]["name"] if props["프로젝트 형태"]["select"] else '-',
            "business_manager": props["영업 담당자"]["multi_select"][0]["name"] if len(props["영업 담당자"]["multi_select"]) != 0 else '-',
            "release_date": props["납품일"]["date"]["start"] if props["납품일"]["date"] else '',
            "catalog_no": props["Cat No."]["rich_text"][0]["plain_text"] if len(props["Cat No."]["rich_text"]) != 0 else '',
            "unit_quantity": props["unit quantity"]["number"] if props["unit quantity"]["number"] else 0,
            "extra_quantity": props["extra quantity"]["number"] if props["extra quantity"]["number"] else 0,
            "vinyl_set": props["vinyl set"]["select"]["name"] if props["vinyl set"]["select"] else '-',
        }
        # 값 삽입
        sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=GOOGLE_SHEET_ID,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": [
                    {"range": tab_title + "!D4", "values": [[project_info["project_name"]]]},
                    {"range": tab_title + "!D6", "values": [[project_info["catalog_no"]]]},
                    {"range": tab_title + "!D7", "values": [[project_info["project_type"]]]},
                    {"range": tab_title + "!D8", "values": [[project_info["business_manager"]]]},
                    {"range": tab_title + "!F6", "values": [[project_info["release_date"]]]},
                    {"range": tab_title + "!I5", "values": [[project_info["unit_quantity"]]]},
                    {"range": tab_title + "!I6", "values": [[project_info["extra_quantity"]]]},
                    {"range": tab_title + "!I7", "values": [[project_info["vinyl_set"]]]},
                ]
            }
        ).execute()
        time.sleep(3)
    
    # 기록 저장
    save_file_to_dict(SHEET_TABS_INFO, tabs_dict)
    save_file_to_dict(PROJECT_UPDATE_INFO, update_dict)
    
    # with open(SHEET_TABS_INFO, "w", encoding="utf-8") as f:
    #     json.dump(tabs_dict, f, ensure_ascii=False, indent=4)
    print("✅ 시트 업데이트 완료")

# === DAG 정의 ===
default_args = {
    'owner': 'joonghyeonan',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    dag_id='syncNotionSheets',
    default_args=default_args,
    schedule='10 9,14 * * 1-5',
    catchup=False,
    max_active_runs=1,
    tags=['notion', 'gsheet', 'automation', 'copy']
) as dag:
    create_task = PythonOperator(
        task_id='create_sheets',
        python_callable=create_sheets,
    )
    update_task = PythonOperator(
        task_id='update_sheets',
        python_callable=update_sheets,
    )
    create_task >> update_task
