from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

import json, os, pickle, time
from datetime import datetime, timedelta


# === 환경 변수 ===
TEMPLATE_SHEET_ID = Variable.get('TEMPLATE_SHEET_ID')

SHEETS_INFO = '/opt/airflow/temp/sheets_info.json'
TEMPLATE_TABS_INFO = '/opt/airflow/temp/template_tabs_info.json'

CREDENTIALS_FILE = '/opt/airflow/temp/credentials.json'
TOKEN_PICKLE = '/opt/airflow/temp/token.pickle'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


# === Google Sheets 연결 ===
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

    return build('sheets', 'v4', credentials=creds, cache_discovery=False)


def load_json_dict(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return dict(json.load(f))
    return {}


def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=4)


def extract_template_tabs(template_tabs_info: dict):
    """
    template_tabs_info.json 포맷:
    {
        "TEMPLATE_TAB_ID1": {"gid": 386920762, "name": "dashboard"},
        ...
    }
    -> [(gid:int, title:str), ...]
    """
    if not isinstance(template_tabs_info, dict):
        raise ValueError("template_tabs_info.json 포맷이 dict가 아님")

    tabs = []
    for _, v in template_tabs_info.items():
        if not isinstance(v, dict):
            continue
        if "gid" not in v:
            continue

        gid = int(v["gid"])
        title = v.get("name")
        if not title:
            raise ValueError(f"template tab name이 비어있음 (gid={gid})")

        tabs.append((gid, title))

    if not tabs:
        raise ValueError("template_tabs_info.json에서 탭 정보를 찾지 못함")
    return tabs


def execute_with_retry(request, *, retries=6, base_sleep=1.0, max_sleep=30.0, tag=""):
    """
    429/500/503 같은 일시 오류는 지수 백오프로 재시도.
    """
    for attempt in range(1, retries + 1):
        try:
            return request.execute()
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            if status in (429, 500, 503):
                sleep_s = min(max_sleep, base_sleep * (2 ** (attempt - 1))) + attempt * 0.2
                print(f"[RETRY] {tag} HttpError {status} attempt={attempt}/{retries} sleep={sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue
            raise
    raise RuntimeError(f"{tag} request failed after {retries} retries")


def check_release_tabs():
    sheets = get_sheets_service()

    sheet_dict = load_json_dict(SHEETS_INFO)
    template_tabs_info = load_json_dict(TEMPLATE_TABS_INFO)

    year = datetime.today().year
    next_year_title = f"{year + 1}_결산"

    if next_year_title in sheet_dict:
        print(f"[SKIP] already exists: {next_year_title}")
        return

    # 1) 새 스프레드시트 생성
    created = execute_with_retry(
        sheets.spreadsheets().create(body={"properties": {"title": next_year_title}}),
        tag="spreadsheets.create"
    )
    new_spreadsheet_id = created["spreadsheetId"]
    print(f"[OK] created spreadsheet: {new_spreadsheet_id} ({next_year_title})")

    # create 직후 안정화 텀
    time.sleep(2.0)

    # 2) 기본 탭 sheetId 확인
    meta = execute_with_retry(
        sheets.spreadsheets().get(spreadsheetId=new_spreadsheet_id),
        tag="spreadsheets.get(new)"
    )
    default_sheet_id = int(meta["sheets"][0]["properties"]["sheetId"])

    # 캐시 저장 구조
    sheet_dict[next_year_title] = {"sheet_id": new_spreadsheet_id, "project_id": {}}

    # 3) 템플릿 탭 목록
    template_tabs = extract_template_tabs(template_tabs_info)

    # 4) 탭 복사 + 즉시 rename(“의 사본” 제거)
    for template_gid, desired_title in template_tabs:
        copied = execute_with_retry(
            sheets.spreadsheets().sheets().copyTo(
                spreadsheetId=TEMPLATE_SHEET_ID,
                sheetId=int(template_gid),
                body={"destinationSpreadsheetId": new_spreadsheet_id}
            ),
            tag=f"copyTo gid={template_gid}"
        )
        new_sheet_id = int(copied["sheetId"])

        execute_with_retry(
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=new_spreadsheet_id,
                body={
                    "requests": [
                        {
                            "updateSheetProperties": {
                                "properties": {"sheetId": new_sheet_id, "title": desired_title},
                                "fields": "title"
                            }
                        }
                    ]
                }
            ),
            tag=f"rename sheetId={new_sheet_id}"
        )

        time.sleep(1.0)

    # 5) 기본 탭 삭제
    execute_with_retry(
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=new_spreadsheet_id,
            body={"requests": [{"deleteSheet": {"sheetId": default_sheet_id}}]}
        ),
        tag="delete default sheet"
    )

    # 6) 캐시 저장
    save_json(SHEETS_INFO, sheet_dict)
    print(f"[DONE] next year spreadsheet initialized: {next_year_title}")


# === DAG ===
default_args = {
    'owner': 'joonghyeonan',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='checkReleaseSheet',
    default_args=default_args,
    schedule='10 9,14 * * 1-5',
    catchup=False,
    max_active_runs=1,
    tags=['gsheet', 'automation']
) as dag:
    check_sheet_task = PythonOperator(
        task_id='check_sheet',
        python_callable=check_release_tabs,
    )

    check_sheet_task
    