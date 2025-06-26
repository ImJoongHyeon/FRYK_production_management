from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable

from notion_client import Client as NotionClient
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

import json, os, pickle
from datetime import datetime, timedelta

import time, glob

# === 환경 변수 ===
NOTION_TOKEN = Variable.get('NOTION_TOKEN')
PROJECT_DB_ID = Variable.get('PROJECT_DB_ID')
GOOGLE_SHEET_ID = Variable.get('GOOGLE_SHEET_ID')
TEMPLATE_TAB_ID = int(Variable.get('TEMPLATE_TAB_ID'))

TOKEN_PICKLE = '/opt/airflow/temp/token.pickle'
NOTION_PROJECTS_FILE = '/opt/airflow/temp/notion_projects.json'
GOOGLE_SHEETS_FILE = '/opt/airflow/temp/google_sheets.json'
CREDENTIALS_FILE = '/opt/airflow/temp/credentials.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

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


def create_sheets():
    # === 연결 ===
    notion = NotionClient(auth=NOTION_TOKEN)
    sheets = get_sheets_service()

    # === 기존 기록 로드 ===
    if os.path.exists(NOTION_PROJECTS_FILE):
        with open(NOTION_PROJECTS_FILE, "r") as f:
            notion_projects = set(json.load(f))
    else:
        notion_projects = set()

    # === 노션 데이터 불러오기
    results = notion.databases.query(database_id=PROJECT_DB_ID)["results"]

    # === 구글 시트 탭 목록 가져오기
    sheet_metadata = sheets.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
    existing_tabs = {s["properties"]["title"] for s in sheet_metadata.get("sheets", [])}

    new_pages = []
    for page in results:
        page_id = page["id"]
        project_name = page["properties"]["프로젝트명"]["title"][0]["plain_text"]
        tab_title = f"{project_name}_결산"

        if tab_title not in existing_tabs or page_id not in notion_projects:
            new_pages.append(page)
            notion_projects.add(page["id"])
            
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

        new_tab_id = copied_tab["sheetId"]

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
        
        time.sleep(5)
        
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
    with open(NOTION_PROJECTS_FILE, "w") as f:
        json.dump(list(notion_projects), f, ensure_ascii=False, indent=2)

    print("✅ 시트 생성 완료")


# === DAG 정의 ===
default_args = {
    'owner': 'joonghyeonan',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    dag_id='createSheets',
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
    create_task
