from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable

from notion_client import Client as NotionClient
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

import json, os, pickle, time
from datetime import datetime, timedelta

# Env / Paths
NOTION_TOKEN = Variable.get('NOTION_TOKEN')
PROJECT_DB_ID = Variable.get('PROJECT_DB_ID')

TEMPLATE_SHEET_ID = Variable.get('TEMPLATE_SHEET_ID')
PROJECT_TEMPLATE_TAB_ID = int(Variable.get('RELEASE_TAB_ID'))  # 프로젝트 탭 템플릿 gid

TOKEN_PICKLE = '/opt/airflow/temp/token.pickle'
SHEETS_INFO = '/opt/airflow/temp/sheets_info.json'
TEMPLATE_TABS_INFO = '/opt/airflow/temp/template_tabs_info.json'
PROJECT_UPDATE_INFO = '/opt/airflow/temp/project_update_info.json'
CREDENTIALS_FILE = '/opt/airflow/temp/credentials.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Google Sheets Client
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

# JSON helpers
def load_json_dict(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return dict(json.load(f))
    return {}


def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=4)

# Retry helper (429/500/503)
def execute_with_retry(request, *, retries=7, base_sleep=1.0, max_sleep=40.0, tag=""):
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

def query_all_pages(notion, data_source_id, **kwargs):
    all_results = []
    cursor = None
    while True:
        payload = dict(kwargs)
        if cursor:
            payload["start_cursor"] = cursor
        payload["page_size"] = 100

        resp = notion.data_sources.query(data_source_id, **payload)
        all_results.extend(resp.get("results", []))

        cursor = resp.get("next_cursor")
        if not resp.get("has_more"):
            break
    return all_results

def safe_title_text(title_prop):
    arr = (title_prop or {}).get("title") or []
    return "".join(x.get("plain_text", "") for x in arr).strip()


def get_release_date_start(props):
    d = (props.get("납품일") or {}).get("date") or {}
    return d.get("start")


def get_release_year(props):
    start = get_release_date_start(props)
    if not start:
        return None
    return start.split("-")[0]


def parse_gid_from_sheet_url(url: str):
    if not url or "gid=" not in url:
        return None
    try:
        return int(url.split("gid=")[-1].split("#")[0])
    except Exception:
        return None


def parse_spreadsheet_id_from_url(url: str):
    if not url:
        return None
    marker = "/spreadsheets/d/"
    if marker not in url:
        return None
    try:
        return url.split(marker, 1)[1].split("/", 1)[0]
    except Exception:
        return None


def build_sheet_url(spreadsheet_id: str, gid: int):
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit?gid={gid}#gid={gid}"


def a1_sheet(title: str) -> str:
    safe = (title or "").replace("'", "''")
    return f"'{safe}'"

def extract_template_tabs(template_tabs_info: dict):
    tabs = []
    for _, v in (template_tabs_info or {}).items():
        if not isinstance(v, dict):
            continue
        if "gid" not in v:
            continue
        gid = int(v["gid"])
        name = v.get("name")
        if not name:
            raise ValueError(f"template tab name이 비어있음 (gid={gid})")
        tabs.append((gid, name))
    if not tabs:
        raise ValueError("template_tabs_info.json에서 탭 정보를 찾지 못함")
    return tabs


def ensure_year_sheet(sheets, sheets_info: dict, template_tabs_info: dict, year_key: str):
    year_obj = sheets_info.get(year_key)
    if year_obj and year_obj.get("sheet_id"):
        year_obj.setdefault("projects", {})
        year_obj.setdefault("gid_index", {})
        year_obj.setdefault("legacy_project_id", year_obj.get("legacy_project_id", {}))
        return year_obj["sheet_id"]

    created = execute_with_retry(
        sheets.spreadsheets().create(body={"properties": {"title": year_key}}),
        tag=f"spreadsheets.create {year_key}"
    )
    new_spreadsheet_id = created["spreadsheetId"]
    time.sleep(2.0)

    meta = execute_with_retry(
        sheets.spreadsheets().get(spreadsheetId=new_spreadsheet_id),
        tag=f"spreadsheets.get(meta) {year_key}"
    )
    default_sheet_id = int(meta["sheets"][0]["properties"]["sheetId"])

    template_tabs = extract_template_tabs(template_tabs_info)
    for template_gid, desired_title in template_tabs:
        copied = execute_with_retry(
            sheets.spreadsheets().sheets().copyTo(
                spreadsheetId=TEMPLATE_SHEET_ID,
                sheetId=int(template_gid),
                body={"destinationSpreadsheetId": new_spreadsheet_id}
            ),
            tag=f"copyTo template gid={template_gid} -> {year_key}"
        )
        new_sheet_id = int(copied["sheetId"])

        execute_with_retry(
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=new_spreadsheet_id,
                body={"requests": [{
                    "updateSheetProperties": {
                        "properties": {"sheetId": new_sheet_id, "title": desired_title},
                        "fields": "title"
                    }
                }]}
            ),
            tag=f"rename copied sheetId={new_sheet_id}"
        )
        time.sleep(1.0)

    execute_with_retry(
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=new_spreadsheet_id,
            body={"requests": [{"deleteSheet": {"sheetId": default_sheet_id}}]}
        ),
        tag=f"delete default sheet {year_key}"
    )

    sheets_info[year_key] = {
        "sheet_id": new_spreadsheet_id,
        "projects": {},
        "gid_index": {},
        "legacy_project_id": {}
    }
    return new_spreadsheet_id


def normalize_and_migrate_sheets_info_schema(sheets_info: dict, notion_pages: list):
    for year_key, year_obj in list((sheets_info or {}).items()):
        if not isinstance(year_obj, dict):
            continue
        year_obj.setdefault("projects", {})
        year_obj.setdefault("gid_index", {})
        if "legacy_project_id" not in year_obj:
            legacy = year_obj.get("project_id") or {}
            year_obj["legacy_project_id"] = legacy

    for page in notion_pages:
        props = page.get("properties", {})
        page_id = page["id"]

        year = get_release_year(props)
        if not year:
            continue
        year_key = f"{year}_결산"
        if year_key not in sheets_info:
            continue

        sheet_url = (props.get("sheet url") or {}).get("url")
        gid = parse_gid_from_sheet_url(sheet_url)
        if gid is None:
            continue

        project_name = safe_title_text(props.get("프로젝트명") or {})
        if not project_name:
            continue
        tab_title = f"{project_name}_결산"

        year_obj = sheets_info[year_key]
        year_obj["projects"][page_id] = {"gid": int(gid), "tab_title": tab_title}
        year_obj["gid_index"][str(gid)] = page_id

    return sheets_info

def create_project_tab(sheets, dest_spreadsheet_id: str, tab_title: str) -> int:
    copied = execute_with_retry(
        sheets.spreadsheets().sheets().copyTo(
            spreadsheetId=TEMPLATE_SHEET_ID,
            sheetId=int(PROJECT_TEMPLATE_TAB_ID),
            body={"destinationSpreadsheetId": dest_spreadsheet_id}
        ),
        tag=f"copyTo project_template -> {dest_spreadsheet_id}"
    )
    new_gid = int(copied["sheetId"])

    execute_with_retry(
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=dest_spreadsheet_id,
            body={"requests": [{
                "updateSheetProperties": {
                    "properties": {"sheetId": new_gid, "title": tab_title},
                    "fields": "title"
                }
            }]}
        ),
        tag=f"rename new project tab gid={new_gid}"
    )

    time.sleep(1.0)
    return new_gid


def rename_tab_if_needed(sheets, spreadsheet_id: str, gid: int, desired_title: str):
    execute_with_retry(
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{
                "updateSheetProperties": {
                    "properties": {"sheetId": int(gid), "title": desired_title},
                    "fields": "title"
                }
            }]}
        ),
        tag=f"rename existing gid={gid}"
    )
    time.sleep(0.5)


def write_project_values(sheets, spreadsheet_id: str, tab_title: str, props: dict):
    project_name = safe_title_text(props.get("프로젝트명") or {})

    project_info = {
        "project_name": project_name,
        "project_type": ((props.get("프로젝트 형태") or {}).get("select") or {}).get("name") or "-",
        "business_manager": (((props.get("영업 담당자") or {}).get("multi_select") or [{}])[:1][0].get("name")) if (props.get("영업 담당자") or {}).get("multi_select") else "-",
        "release_date": get_release_date_start(props) or "",
        "catalog_no": (((props.get("Cat No.") or {}).get("rich_text") or [{}])[:1][0].get("plain_text")) if (props.get("Cat No.") or {}).get("rich_text") else "",
        "unit_quantity": (props.get("unit quantity") or {}).get("number") or 0,
        "extra_quantity": (props.get("extra quantity") or {}).get("number") or 0,
        "vinyl_set": (((props.get("vinyl set") or {}).get("select") or {}).get("name")) or "-",
    }

    sheet = a1_sheet(tab_title)
    execute_with_retry(
        sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": [
                    {"range": f"{sheet}!D4", "values": [[project_info["project_name"]]]},
                    {"range": f"{sheet}!D6", "values": [[project_info["catalog_no"]]]},
                    {"range": f"{sheet}!D7", "values": [[project_info["project_type"]]]},
                    {"range": f"{sheet}!D8", "values": [[project_info["business_manager"]]]},
                    {"range": f"{sheet}!F6", "values": [[project_info["release_date"]]]},
                    {"range": f"{sheet}!I5", "values": [[project_info["unit_quantity"]]]},
                    {"range": f"{sheet}!I6", "values": [[project_info["extra_quantity"]]]},
                    {"range": f"{sheet}!I7", "values": [[project_info["vinyl_set"]]]},
                ]
            }
        ),
        tag=f"values.batchUpdate {tab_title}"
    )

def refresh_last_edited_time_from_notion_response(updated_page: dict):
    if not updated_page:
        return None
    try:
        t = (updated_page.get("properties", {})
                        .get("최종 편집 일시", {})
                        .get("last_edited_time"))
        if t:
            return t
    except Exception:
        pass
    return updated_page.get("last_edited_time")

# Ensure year sheets
def ensure_year_sheets_task():
    notion = NotionClient(auth=NOTION_TOKEN)
    sheets = get_sheets_service()

    sheets_info = load_json_dict(SHEETS_INFO)
    template_tabs_info = load_json_dict(TEMPLATE_TABS_INFO)

    pages = query_all_pages(notion=notion, data_source_id=PROJECT_DB_ID)

    # 일단 legacy schema normalize
    sheets_info = normalize_and_migrate_sheets_info_schema(sheets_info, pages)

    # 노션에서 등장하는 연도에 대해 시트 ensure
    years = set()
    for page in pages:
        y = get_release_year(page.get("properties", {}))
        if y:
            years.add(y)

    for y in sorted(years):
        year_key = f"{y}_결산"
        ensure_year_sheet(sheets, sheets_info, template_tabs_info, year_key)

    save_json(SHEETS_INFO, sheets_info)
    print(f"✅ ensure_year_sheets_task 완료: years={sorted(years)}")

# Sync projects (create/update/skip)

def sync_projects_task():
    notion = NotionClient(auth=NOTION_TOKEN)
    sheets = get_sheets_service()

    sheets_info = load_json_dict(SHEETS_INFO)
    template_tabs_info = load_json_dict(TEMPLATE_TABS_INFO)  # ensure_year_sheet에서 필요
    update_info = load_json_dict(PROJECT_UPDATE_INFO)

    pages = query_all_pages(notion=notion, data_source_id=PROJECT_DB_ID)
    sheets_info = normalize_and_migrate_sheets_info_schema(sheets_info, pages)

    for page in pages:
        props = page.get("properties", {})
        page_id = page["id"]

        release_year = get_release_year(props)
        if not release_year:
            continue
        year_key = f"{release_year}_결산"

        # 혹시 sheets_info가 비었거나 누락이면 task1 안 돌렸어도 여기서 ensure
        dest_spreadsheet_id = ensure_year_sheet(sheets, sheets_info, template_tabs_info, year_key)

        project_name = safe_title_text(props.get("프로젝트명") or {})
        if not project_name:
            continue
        tab_title = f"{project_name}_결산"

        last_edited_time = (props.get("최종 편집 일시") or {}).get("last_edited_time")

        sheet_url = (props.get("sheet url") or {}).get("url")
        url_gid = parse_gid_from_sheet_url(sheet_url)
        url_sheet_id = parse_spreadsheet_id_from_url(sheet_url)

        year_obj = sheets_info[year_key]
        projects = year_obj.setdefault("projects", {})
        gid_index = year_obj.setdefault("gid_index", {})
        cached = projects.get(page_id)
        cached_gid = cached.get("gid") if cached else None
        cached_title = cached.get("tab_title") if cached else None

        # ✅ 스킵 조건
        same_edit = (update_info.get(page_id) == last_edited_time)
        same_year_sheet = (url_sheet_id == dest_spreadsheet_id) if sheet_url else False
        same_gid = (url_gid is not None and cached_gid is not None and int(url_gid) == int(cached_gid))
        same_title = (cached_title == tab_title)

        if same_edit and same_year_sheet and same_gid and same_title:
            # print(f"[SKIP] year={year_key} page={page_id} gid={cached_gid} title={tab_title}")
            continue

        created = False

        # gid 결정
        if cached_gid:
            gid = int(cached_gid)
        elif url_gid is not None and url_sheet_id == dest_spreadsheet_id:
            gid = int(url_gid)
        else:
            gid = create_project_tab(sheets, dest_spreadsheet_id, tab_title)
            new_url = build_sheet_url(dest_spreadsheet_id, gid)

            updated_page = notion.pages.update(
                page_id=page_id,
                properties={"sheet url": {"url": new_url}}
            )
            time.sleep(0.5)

            # ✅ PATCH: URL 수정으로 바뀐 last_edited_time을 update_info에 반영
            new_last = refresh_last_edited_time_from_notion_response(updated_page)
            if new_last:
                last_edited_time = new_last

            created = True
            print(f"[CREATE] year={year_key} page={page_id} gid={gid} title={tab_title}")

        # 캐시 갱신
        projects[page_id] = {"gid": int(gid), "tab_title": tab_title}
        gid_index[str(gid)] = page_id

        # rename 필요시만
        if cached_title is not None and cached_title != tab_title:
            rename_tab_if_needed(sheets, dest_spreadsheet_id, gid, tab_title)

        # 값 쓰기
        write_project_values(sheets, dest_spreadsheet_id, tab_title, props)
        time.sleep(0.8)

        # ✅ 성공 후 update_info 저장
        update_info[page_id] = last_edited_time

        if created:
            print(f"[CREATED_AND_SYNCED] year={year_key} page={page_id} gid={gid} title={tab_title}")
        else:
            print(f"[UPDATE] year={year_key} page={page_id} gid={gid} title={tab_title}")

    save_json(SHEETS_INFO, sheets_info)
    save_json(PROJECT_UPDATE_INFO, update_info)
    print("✅ sync_projects_task 완료")

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
    schedule='10 9,14 * * 1-5',  # 매년 1월 13일
    catchup=False,
    max_active_runs=1,
    tags=['notion', 'gsheet', 'automation']
) as dag:

    ensure_year_sheets = PythonOperator(
        task_id='ensure_year_sheets',
        python_callable=ensure_year_sheets_task,
    )

    sync_projects = PythonOperator(
        task_id='sync_projects',
        python_callable=sync_projects_task,
    )

    ensure_year_sheets >> sync_projects
    