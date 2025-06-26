from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable

from notion_client import Client as NotionClient
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

import json, os, pickle
from datetime import datetime, timedelta, date

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


def update_gsheets():
    # === 연결 ===
    notion = NotionClient(auth=NOTION_TOKEN)
    sheets = get_sheets_service()

    # === 노션 데이터 불러오기
    results = notion.databases.query(database_id=PROJECT_DB_ID)["results"]

    # === 시트 복사 및 데이터 삽입
    for page in results:
        props = page["properties"]
        project_name = props["프로젝트명"]["title"][0]["plain_text"]
        tab_title = f"{project_name}_결산"
    
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
    dag_id='updateGsheets',
    default_args=default_args,
    schedule='20 9,14 * * 1-5',
    catchup=False,
    max_active_runs=1,
    tags=['notion', 'gsheet', 'automation', 'copy']
) as dag:
    update_task = PythonOperator(
        task_id='updateSheets',
        python_callable=update_gsheets,
    )
    update_task
