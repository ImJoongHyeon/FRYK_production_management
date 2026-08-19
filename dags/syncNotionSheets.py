"""
syncNotionSheets.py  (patched)

주요 변경점
-----------
1. [핵심] 상태 저장을 원자적 + 증분으로.
   - 프로젝트 1건 처리할 때마다 sheets_info / project_update_info 를 저장한다.
   - 저장은 tmp 파일 -> os.replace 로 원자적으로 수행한다.
   - task 가 중간에 죽어도 그때까지의 진행 상황이 반드시 남는다.
     (기존: 루프 끝까지 성공해야만 저장 -> 한 번 실패하면 영원히 복구 불가)

2. [핵심] 캐시를 믿지 않고 실제 스프레드시트 탭 목록을 기준으로 판단(self-healing).
   - TabIndex 가 spreadsheets().get 으로 실제 (title -> gid) 를 들고 있다.
   - 캐시에 없어도 같은 이름의 탭이 실제로 있으면 그 탭을 '입양(adopt)' 한다.
     -> "이름이 'XXX_결산'인 시트가 이미 있습니다" 400 에러가 원천적으로 사라진다.
   - 복사 후 rename 이 실패하면 방금 만든 사본을 즉시 삭제한다.
     -> "~의 사본" 고아 탭이 쌓이지 않는다.

3. Notion 에서 프로젝트명이 바뀌면 실제 시트 탭 이름도 rename 한다.
   (기존: 캐시의 tab_title 만 바꾸고 실제 탭은 그대로 -> 이후 write_values 가
    'Unable to parse range' 로 실패)

4. sheets_info 의 "project_id" / "projects" 키 불일치 정규화.
   (checkSupplyTabs.py 는 project_id 로, 이 DAG 는 projects 로 쓰고 있었음
    -> 2027_결산에서 KeyError 발생 예정이었음)

5. 프로젝트 단위 예외 격리.
   - 한 건이 실패해도 나머지는 계속 처리한다.
   - 실패가 하나라도 있으면 task 는 마지막에 AirflowException 으로 실패 처리하되,
     상태는 이미 저장된 뒤이므로 다음 run 이 이어서 진행할 수 있다.

6. 같은 연도 내 프로젝트명 중복 감지.
   (기존: 두 번째 페이지가 첫 번째 페이지의 탭을 조용히 덮어씀)

7. 연도 이동(납품일 연도 변경) 감지 및 경고 로그.
"""

from __future__ import annotations

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable

from notion_client import Client as NotionClient
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

import json
import logging
import os
import pickle
import tempfile
import time
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

# =========================
# ENV
# =========================
NOTION_TOKEN = Variable.get('NOTION_TOKEN')
PROJECT_DB_ID = Variable.get('PROJECT_DB_ID')

TEMPLATE_SHEET_ID = Variable.get('TEMPLATE_SHEET_ID')
PROJECT_TEMPLATE_TAB_ID = int(Variable.get('RELEASE_TAB_ID'))

TOKEN_PICKLE = '/opt/airflow/temp/token.pickle'
SHEETS_INFO = '/opt/airflow/temp/sheets_info.json'
TEMPLATE_TABS_INFO = '/opt/airflow/temp/template_tabs_info.json'
PROJECT_UPDATE_INFO = '/opt/airflow/temp/project_update_info.json'
CREDENTIALS_FILE = '/opt/airflow/temp/credentials.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

TAB_SUFFIX = '_결산'


# =========================
# Google Sheets
# =========================
def get_sheets_service():
    creds = None
    if os.path.exists(TOKEN_PICKLE):
        with open(TOKEN_PICKLE, 'rb') as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # 주의: Airflow worker 는 헤드리스이므로 여기 들어오면 사실상 무한 대기한다.
            # 토큰이 만료/폐기됐다면 로컬에서 token.pickle 을 새로 만들어 넣어야 한다.
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PICKLE, 'wb') as f:
            pickle.dump(creds, f)

    return build('sheets', 'v4', credentials=creds, cache_discovery=False)


# =========================
# JSON helpers (원자적 저장)
# =========================
def load_json(path):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_json(path, data):
    """tmp 파일에 쓰고 os.replace 로 교체 -> 중간에 죽어도 파일이 깨지지 않는다."""
    directory = os.path.dirname(path) or '.'
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix='.tmp_', suffix='.json')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


# =========================
# Retry wrapper
# =========================
def exec_retry(req, tag='', retries=6):
    last_exc = None
    for i in range(retries):
        try:
            return req.execute()
        except HttpError as e:
            status = getattr(e.resp, 'status', None)
            if status in (429, 500, 502, 503, 504):
                last_exc = e
                sleep_s = min(30.0, 2 ** i)
                log.warning("[RETRY] %s status=%s attempt=%d/%d sleep=%.1fs",
                            tag, status, i + 1, retries, sleep_s)
                time.sleep(sleep_s)
                continue
            raise
    raise RuntimeError(f"Google API failed after {retries} retries: {tag}") from last_exc


# =========================
# Notion
# =========================
def query_all_pages(notion, db_id):
    results, cursor = [], None
    while True:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        resp = notion.data_sources.query(db_id, **payload)
        results.extend(resp["results"])
        if not resp.get("has_more"):
            break
        cursor = resp["next_cursor"]
    return results


# =========================
# Utils
# =========================
def get_title(props):
    return ''.join(x["plain_text"] for x in props["프로젝트명"]["title"]).strip()


def get_release_date_start(props, year: bool = False):
    d = (props.get("납품일") or {}).get("date") or {}
    start = d.get("start")
    if not start:
        return None
    return start[:4] if year else start


def safe_title_text(title_prop):
    arr = (title_prop or {}).get("title") or []
    return "".join(x.get("plain_text", "") for x in arr).strip()


def a1(title):
    safe = title.replace("'", "''")
    return f"'{safe}'"


def sheet_url(sheet_id, gid):
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit?gid={gid}#gid={gid}"


def find_project_by_page_id(projects, page_id):
    for name, info in projects.items():
        if info.get("notion_page_id") == page_id:
            return name, info
    return None, None


def get_projects_bucket(year_entry):
    """
    checkSupplyTabs.py 는 'project_id', 이 DAG 는 'projects' 로 저장해왔다.
    'projects' 로 통일하고, 기존 'project_id' 데이터가 있으면 흡수한다.
    """
    if "projects" not in year_entry:
        year_entry["projects"] = year_entry.pop("project_id", None) or {}
    else:
        legacy = year_entry.pop("project_id", None)
        if legacy:
            for k, v in legacy.items():
                year_entry["projects"].setdefault(k, v)
    return year_entry["projects"]


# =========================
# 실제 스프레드시트 탭 인덱스 (핵심)
# =========================
class TabIndex:
    """스프레드시트의 실제 (title <-> gid) 매핑. 캐시가 아니라 진짜 상태."""

    def __init__(self, sheets, spreadsheet_id):
        self.sheets = sheets
        self.sid = spreadsheet_id
        self.by_title = {}
        self.by_gid = {}
        self.refresh()

    def refresh(self):
        meta = exec_retry(
            self.sheets.spreadsheets().get(
                spreadsheetId=self.sid,
                fields="sheets.properties(sheetId,title)",
            ),
            tag=f"get tabs {self.sid}",
        )
        self.by_title, self.by_gid = {}, {}
        for s in meta.get("sheets", []):
            p = s["properties"]
            gid, title = int(p["sheetId"]), p["title"]
            self.by_title[title] = gid
            self.by_gid[gid] = title

    def put(self, gid, title):
        gid = int(gid)
        old = self.by_gid.get(gid)
        if old is not None:
            self.by_title.pop(old, None)
        self.by_gid[gid] = title
        self.by_title[title] = gid

    def drop(self, gid):
        gid = int(gid)
        title = self.by_gid.pop(gid, None)
        if title is not None:
            self.by_title.pop(title, None)


def _rename_tab(sheets, sid, index: TabIndex, gid: int, new_title: str):
    exec_retry(
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=sid,
            body={"requests": [{
                "updateSheetProperties": {
                    "properties": {"sheetId": int(gid), "title": new_title},
                    "fields": "title",
                }
            }]},
        ),
        tag=f"rename gid={gid} -> {new_title}",
    )
    index.put(gid, new_title)


def _copy_template_tab(sheets, sid, index: TabIndex, template_gid: int, desired_title: str) -> int:
    """템플릿 탭 복사 후 rename. rename 실패 시 사본을 즉시 삭제한다."""
    copied = exec_retry(
        sheets.spreadsheets().sheets().copyTo(
            spreadsheetId=TEMPLATE_SHEET_ID,
            sheetId=int(template_gid),
            body={"destinationSpreadsheetId": sid},
        ),
        tag=f"copyTo gid={template_gid} -> {sid}",
    )
    new_gid = int(copied["sheetId"])
    index.put(new_gid, copied.get("title", f"__copied_{new_gid}"))

    try:
        _rename_tab(sheets, sid, index, new_gid, desired_title)
    except Exception:
        # 고아 "~의 사본" 탭이 남지 않도록 정리
        log.exception("[CLEANUP] rename 실패 -> 방금 만든 사본(gid=%s) 삭제 시도", new_gid)
        try:
            exec_retry(
                sheets.spreadsheets().batchUpdate(
                    spreadsheetId=sid,
                    body={"requests": [{"deleteSheet": {"sheetId": new_gid}}]},
                ),
                tag=f"delete orphan copy gid={new_gid}",
            )
            index.drop(new_gid)
        except Exception:
            log.exception("[CLEANUP] 사본 삭제까지 실패. gid=%s 를 수동 정리해야 함", new_gid)
        raise

    return new_gid


def ensure_tab(sheets, sid, index: TabIndex, desired_title: str, cached_gid, template_gid: int) -> int:
    """
    desired_title 을 가진 탭의 gid 를 보장해서 돌려준다.

    우선순위
      1) 캐시된 gid 가 실제로 존재 -> 필요하면 rename 후 재사용
      2) 같은 이름의 탭이 이미 실제로 존재 -> 그 탭을 입양 (self-healing)
      3) 없음 -> 템플릿 복사 + rename
    """
    # 1) 캐시된 gid 가 실제로 살아있는 경우
    if cached_gid is not None:
        cached_gid = int(cached_gid)
        actual_title = index.by_gid.get(cached_gid)
        if actual_title is not None:
            if actual_title != desired_title:
                holder = index.by_title.get(desired_title)
                if holder is not None and int(holder) != cached_gid:
                    raise RuntimeError(
                        f"탭 이름 충돌: '{desired_title}' 는 이미 gid={holder} 가 쓰고 있어서 "
                        f"gid={cached_gid} 를 rename 할 수 없음"
                    )
                log.info("[RENAME] gid=%s '%s' -> '%s'", cached_gid, actual_title, desired_title)
                _rename_tab(sheets, sid, index, cached_gid, desired_title)
            return cached_gid
        log.warning("[STALE] 캐시된 gid=%s 가 실제 시트에 없음. 재탐색.", cached_gid)

    # 2) 이름으로 입양 -- 400 duplicate 를 원천 차단하는 부분
    existing = index.by_title.get(desired_title)
    if existing is not None:
        log.warning("[ADOPT] 캐시엔 없지만 '%s' 탭이 이미 존재 -> gid=%s 를 재사용", desired_title, existing)
        return int(existing)

    # 3) 신규 생성
    log.info("[CREATE] '%s' 탭 생성", desired_title)
    return _copy_template_tab(sheets, sid, index, template_gid, desired_title)


# =========================
# Year sheet ensure
# =========================
def ensure_year_sheet(sheets, sheets_info, template_tabs, year_key):
    if year_key in sheets_info:
        entry = sheets_info[year_key]
        get_projects_bucket(entry)  # 키 정규화
        return entry["sheet_id"]

    created = exec_retry(
        sheets.spreadsheets().create(body={"properties": {"title": year_key}}),
        tag=f"create spreadsheet {year_key}",
    )
    sid = created["spreadsheetId"]
    log.info("[OK] created spreadsheet %s (%s)", sid, year_key)
    time.sleep(2.0)

    index = TabIndex(sheets, sid)
    # 방금 만든 시트라 탭은 기본 탭 하나뿐이다.
    default_gid = next(iter(index.by_gid))

    for tab in template_tabs.values():
        if not isinstance(tab, dict) or "gid" not in tab or not tab.get("name"):
            continue
        _copy_template_tab(sheets, sid, index, int(tab["gid"]), tab["name"])
        time.sleep(1.0)

    exec_retry(
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=sid,
            body={"requests": [{"deleteSheet": {"sheetId": default_gid}}]},
        ),
        tag="delete default sheet",
    )

    sheets_info[year_key] = {"sheet_id": sid, "projects": {}}
    return sid


# =========================
# 값 쓰기
# =========================
def write_values(sheets, sid, title, props):
    project_info = {
        "project_name": safe_title_text(props.get("프로젝트명") or {}),
        "project_type": ((props.get("프로젝트 형태") or {}).get("select") or {}).get("name") or "-",
        "business_manager": (((props.get("영업 담당자") or {}).get("multi_select") or [{}])[0].get("name") or "-")
                            if (props.get("영업 담당자") or {}).get("multi_select") else "-",
        "release_date": get_release_date_start(props) or "",
        "catalog_no": (((props.get("Cat No.") or {}).get("rich_text") or [{}])[0].get("plain_text") or "")
                            if (props.get("Cat No.") or {}).get("rich_text") else "",
        "unit_quantity": (props.get("unit quantity") or {}).get("number") or 0,
        "extra_quantity": (props.get("extra quantity") or {}).get("number") or 0,
        "vinyl_set": ((props.get("vinyl set") or {}).get("select") or {}).get("name") or "-",
    }

    sheet = a1(title)
    exec_retry(
        sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=sid,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": [
                    {"range": f"{sheet}!D4",  "values": [[project_info["project_name"]]]},
                    {"range": f"{sheet}!D6",  "values": [[project_info["catalog_no"]]]},
                    {"range": f"{sheet}!D7",  "values": [[project_info["project_type"]]]},
                    {"range": f"{sheet}!D8",  "values": [[project_info["business_manager"]]]},
                    {"range": f"{sheet}!F6",  "values": [[project_info["release_date"]]]},
                    {"range": f"{sheet}!D10", "values": [[project_info["unit_quantity"]]]},
                    {"range": f"{sheet}!D11", "values": [[project_info["extra_quantity"]]]},
                    {"range": f"{sheet}!D12", "values": [[project_info["vinyl_set"]]]},
                ],
            },
        ),
        tag=f"values update {title}",
    )


# =========================
# TASKS
# =========================
def ensure_year_sheets_task():
    notion = NotionClient(auth=NOTION_TOKEN)
    sheets = get_sheets_service()

    sheets_info = load_json(SHEETS_INFO)
    template_tabs = load_json(TEMPLATE_TABS_INFO)

    # 기존 데이터의 키 불일치를 먼저 정규화
    for entry in sheets_info.values():
        if isinstance(entry, dict):
            get_projects_bucket(entry)

    pages = query_all_pages(notion, PROJECT_DB_ID)
    years = {get_release_date_start(p["properties"], True) for p in pages}
    years.discard(None)

    try:
        for y in sorted(years):
            ensure_year_sheet(sheets, sheets_info, template_tabs, f"{y}_결산")
    finally:
        save_json(SHEETS_INFO, sheets_info)


def sync_projects_task():
    notion = NotionClient(auth=NOTION_TOKEN)
    sheets = get_sheets_service()

    sheets_info = load_json(SHEETS_INFO)
    update_info = load_json(PROJECT_UPDATE_INFO)
    template_tabs = load_json(TEMPLATE_TABS_INFO)

    for entry in sheets_info.values():
        if isinstance(entry, dict):
            get_projects_bucket(entry)

    pages = query_all_pages(notion, PROJECT_DB_ID)

    tab_indexes: dict[str, TabIndex] = {}   # spreadsheet_id -> TabIndex
    failures: list[str] = []
    stats = {"skipped": 0, "created": 0, "updated": 0, "adopted": 0}

    def flush():
        save_json(SHEETS_INFO, sheets_info)
        save_json(PROJECT_UPDATE_INFO, update_info)

    try:
        for page in pages:
            props = page["properties"]
            page_id = page["id"]
            name = get_title(props)

            try:
                year = get_release_date_start(props, True)
                if not year:
                    continue
                if not name:
                    raise RuntimeError("프로젝트명이 비어있음")

                year_key = f"{year}_결산"
                sid = ensure_year_sheet(sheets, sheets_info, template_tabs, year_key)
                projects = get_projects_bucket(sheets_info[year_key])
                title = f"{name}{TAB_SUFFIX}"

                # --- 연도 이동 감지 (다른 연도 캐시에 같은 page_id 가 있는지) ---
                for other_key, other_entry in sheets_info.items():
                    if other_key == year_key or not isinstance(other_entry, dict):
                        continue
                    other_projects = other_entry.get("projects") or {}
                    other_name, _ = find_project_by_page_id(other_projects, page_id)
                    if other_name:
                        log.warning(
                            "[YEAR-MOVE] '%s' 가 %s -> %s 로 이동. 이전 시트의 탭은 수동 정리 필요.",
                            other_name, other_key, year_key)
                        del other_projects[other_name]

                old_name, old_entry = find_project_by_page_id(projects, page_id)
                renamed = bool(old_name) and old_name != name

                # --- 같은 연도 내 프로젝트명 중복 감지 ---
                name_entry = projects.get(name)
                if name_entry and name_entry.get("notion_page_id") not in (None, page_id):
                    raise RuntimeError(
                        f"프로젝트명 중복: '{name}' 이(가) 다른 Notion 페이지"
                        f"({name_entry['notion_page_id']})에 이미 매핑돼 있음. "
                        f"Notion 에서 이름을 구분해 주세요."
                    )

                cached = name_entry or old_entry
                last_edit = props["최종 편집 일시"]["last_edited_time"]

                if cached and update_info.get(page_id) == last_edit and not renamed:
                    stats["skipped"] += 1
                    continue

                if sid not in tab_indexes:
                    tab_indexes[sid] = TabIndex(sheets, sid)
                index = tab_indexes[sid]

                cached_gid = cached.get("gid") if cached else None
                was_known = cached_gid is not None and int(cached_gid) in index.by_gid
                title_existed = title in index.by_title

                gid = ensure_tab(sheets, sid, index, title, cached_gid, PROJECT_TEMPLATE_TAB_ID)

                if not was_known and not title_existed:
                    stats["created"] += 1
                elif not was_known:
                    stats["adopted"] += 1
                else:
                    stats["updated"] += 1

                # Notion 의 sheet url 은 신규 생성/입양 시에만 갱신
                if not was_known:
                    try:
                        notion.pages.update(
                            page_id=page_id,
                            properties={"sheet url": {"url": sheet_url(sid, gid)}},
                        )
                    except Exception:
                        log.exception("[WARN] Notion sheet url 갱신 실패: %s", name)

                if renamed and old_name in projects:
                    del projects[old_name]

                projects[name] = {
                    "notion_page_id": page_id,
                    "gid": int(gid),
                    "tab_title": title,
                }

                write_values(sheets, sid, title, props)
                update_info[page_id] = last_edit

                # ★ 핵심: 건별 즉시 저장. 다음 줄에서 죽어도 여기까지는 남는다.
                flush()
                time.sleep(0.3)

            except Exception as e:
                log.exception("[FAIL] project='%s' page_id=%s", name, page_id)
                failures.append(f"{name or '(무명)'} ({page_id}): {e}")
                # 실패한 건은 update_info 를 쓰지 않으므로 다음 run 에서 재시도된다.
                flush()
                continue
    finally:
        flush()

    log.info("[SUMMARY] created=%(created)d adopted=%(adopted)d updated=%(updated)d skipped=%(skipped)d",
             stats)

    if failures:
        raise AirflowException(
            f"{len(failures)}건 실패 (상태는 저장됨, 다음 run 에서 재시도):\n  - "
            + "\n  - ".join(failures)
        )


with DAG(
    dag_id='syncNotionSheets',
    start_date=datetime(2025, 6, 23),
    schedule='10 9,14 * * 1-5',
    catchup=False,
    max_active_runs=1,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
    tags=['notion', 'gsheet', 'automation', 'sync'],
) as dag:

    ensure_year = PythonOperator(
        task_id='ensure_year_sheets',
        python_callable=ensure_year_sheets_task,
    )

    sync_projects = PythonOperator(
        task_id='sync_projects',
        python_callable=sync_projects_task,
    )

    ensure_year >> sync_projects