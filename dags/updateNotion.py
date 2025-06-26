# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.models.variable import Variable

# from notion_client import Client as NotionClient
# from google_auth_oauthlib.flow import InstalledAppFlow
# from googleapiclient.discovery import build
# from google.auth.transport.requests import Request
# from google.oauth2.credentials import Credentials

# import json, os, pickle
# from datetime import datetime, timedelta

# # === 환경 변수 ===
# NOTION_TOKEN = Variable.get('NOTION_TOKEN')
# NOTION_DB_ID = Variable.get('NOTION_DB_ID')
# GOOGLE_SHEET_ID = Variable.get('GOOGLE_SHEET_ID')
# TEMPLATE_TAB_ID = int(Variable.get('TEMPLATE_TAB_ID'))

# TOKEN_PICKLE = '/opt/airflow/temp/token.pickle'
# NOTION_PROJECTS_FILE = '/opt/airflow/temp/notion_projects.json'
# GOOGLE_SHEETS_FILE = '/opt/airflow/temp/google_sheets.json'
# CREDENTIALS_FILE = '/opt/airflow/temp/credentials.json'
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# def get_sheets_service():
#     creds = None
#     if os.path.exists(TOKEN_PICKLE):
#         with open(TOKEN_PICKLE, 'rb') as token:
#             creds = pickle.load(token)

#     if not creds or not creds.valid:
#         if creds and creds.expired and creds.refresh_token:
#             creds.refresh(Request())
#         else:
#             flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
#             creds = flow.run_local_server(port=0)
#         with open(TOKEN_PICKLE, 'wb') as token:
#             pickle.dump(creds, token)

#     return build('sheets', 'v4', credentials=creds)

# def update_notion():
#     # === 연결 ===
#     notion = NotionClient(auth=NOTION_TOKEN)
#     sheets = get_sheets_service()

#     results = notion.databases.query(database_id=NOTION_DB_ID)["results"]
    
#     for page in results:
#         props = page["properties"]
#         project_name = props["프로젝트명"]["title"][0]["plain_text"]
#         artist_name = project_name.split('_')[0]
#         tab_title = f"{artist_name}_결산"
        
#         # === 구글 시트 값 목록 가져오기
#         sheet_result = sheets.spreadsheets().values().batchGet(
#             spreadsheetId=GOOGLE_SHEET_ID,
#             ranges=[
#                 f'{tab_title}!D4', # 프로젝트명
#                 f'{tab_title}!D6', # 타이틀 번호
#                 f'{tab_title}!D7', # 프로젝트 타입
#                 f'{tab_title}!D8', # 담당자
#                 f'{tab_title}!F6', # 납품일
#                 f'{tab_title}!I5', # unit quantity
#                 f'{tab_title}!I6', # extra quantity
#                 f'{tab_title}!I7', # 세트수
#             ]
#         ).execute()
#         sheet_info = sheet_result['valueRanges']

#         notion.pages.update(
#             page_id=page["id"],
#             properties={
#                 "프로젝트명": {"title":[{"text": {"content": f"{sheet_info[0]['values'][0][0]}"}}]},
#                 "프로젝트 형태": {"multi_select": [{"name": f"{sheet_info[2]['values'][0][0]}"}]},
#                 "영업 담당자": {"multi_select": [{"name": f"{sheet_info[3]['values'][0][0]}"}]},
#                 "납품일": {"date": {"start": f"{sheet_info[4]['values'][0][0]}"}},
#                 "Cat No.": {"rich_text": [{"text": {"content": f"{sheet_info[1]['values'][0][0]}"}}]},
#                 "unit quantity": {"number": int(f"{sheet_info[5]['values'][0][0]}")},
#                 "extra quantity": {"number": int(f"{sheet_info[6]['values'][0][0]}")},
#                 "vinyl set": {"multi_select": [{"name": f"{sheet_info[7]['values'][0][0]}"}]},
#             }
#         )
        
#     print("✅ 업데이트 완료")

# # === DAG 정의 ===
# default_args = {
#     'owner': 'joonghyeonan',
#     'depends_on_past': False,
#     'start_date': datetime(2025, 5, 23),
#     'retries': 3,
#     'retry_delay': timedelta(minutes=3),
# }
# with DAG(
#     dag_id='updateNotion',
#     default_args=default_args,
#     schedule='*/30 9-17 * * 1-5',
#     catchup=False,
#     max_active_runs=1,
#     tags=['notion', 'gsheet', 'automation', 'update']
# ) as dag:
#     update_task = PythonOperator(
#         task_id='check_update_gsheet',
#         python_callable=update_notion
#     )
#     update_task
