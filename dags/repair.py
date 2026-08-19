#!/usr/bin/env python3
"""
repair_sheets_info.py — 일회성 복구 스크립트 (v2: 로컬/컨테이너 양쪽 지원)

하는 일
-------
1. sheets_info.json 에 등록된 각 연도 스프레드시트의 **실제 탭 목록**을 읽는다.
2. Notion 프로젝트 DB 와 대조해서 sheets_info.json 을 처음부터 다시 만든다.
   (진실의 원천 = 실제 스프레드시트 + Notion. 기존 JSON 은 참고용으로만 사용)
3. 고아 탭("~의 사본", "Copy of ~")을 찾아내고, 원하면 삭제한다.
4. 'project_id' -> 'projects' 키 불일치를 정규화한다.
5. Notion 에서 사라진 페이지의 project_update_info.json 항목을 정리한다(선택).

기본은 **dry-run**. 아무것도 바꾸지 않고 리포트만 출력한다. 반영은 --apply.


실행 방법
=========

[A] Mac 에서 로컬로 (권장 — OAuth 창이 떠서 인증이 편함)
----------------------------------------------------
    pip install google-api-python-client google-auth-oauthlib
    # (notion-client 는 필요 없음 — Notion 은 REST 로 직접 호출하며
    #  구/신 API 버전을 자동 판별합니다)

    cd ~/Desktop/FRYK
    export NOTION_TOKEN='ntn_...'          # Airflow Variable 과 같은 값
    export PROJECT_DB_ID='...'
    python dags/repair.py                   # temp 폴더 자동 탐색

    # 경로가 자동으로 안 잡히면 명시:
    python dags/repair.py --temp-dir ~/Desktop/FRYK/temp

    Airflow Variable 값 확인:
        docker compose exec airflow-scheduler airflow variables get NOTION_TOKEN
        docker compose exec airflow-scheduler airflow variables get PROJECT_DB_ID

[B] Airflow 컨테이너 안에서
--------------------------
    docker compose exec airflow-scheduler \
        python /opt/airflow/temp/repair_sheets_info.py

    (컨테이너 안에서는 Airflow Variable 을 자동으로 읽으므로 export 불필요.
     단 token.pickle 이 만료됐다면 헤드리스라 재인증이 안 되니 [A] 로 하세요.)

주요 옵션
--------
    --apply                 실제로 파일을 덮어쓴다 (백업 자동 생성)
    --delete-copies         '~의 사본' 고아 탭 삭제 (--apply 필요)
    --temp-dir PATH         json/token 이 있는 폴더 직접 지정
    --year 2026_결산        특정 연도만 처리 (여러 번 지정 가능)
    --no-notion             Notion 조회 없이 탭 현황만 리포트 (부분 복구)
    --prune-update-info     Notion 에 없는 page_id 를 update_info 에서 제거
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import re
import shutil
import sys
import tempfile
import time
from datetime import datetime

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

TAB_SUFFIX = "_결산"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# 런타임에 resolve_paths() 가 채운다
PATHS: dict = {}

# Google Sheets 가 사본을 만들 때 붙이는 접미사
COPY_PATTERNS = [
    re.compile(r"의 사본(\s*\d+)?$"),   # 한국어 UI
    re.compile(r"^Copy of "),           # 영어 UI
    re.compile(r"\(사본(\s*\d+)?\)$"),
]


def is_copy_tab(title: str) -> bool:
    return any(p.search(title) for p in COPY_PATTERNS)


# =========================
# 경로 자동 탐색
# =========================
def candidate_dirs(explicit: str | None):
    """sheets_info.json 이 있을 만한 후보 폴더를 우선순위대로."""
    if explicit:
        yield os.path.abspath(os.path.expanduser(explicit))
        return

    env = os.environ.get("AIRFLOW_TEMP_DIR")
    if env:
        yield os.path.abspath(os.path.expanduser(env))

    here = os.path.dirname(os.path.abspath(__file__))          # 예: .../FRYK/dags
    parent = os.path.dirname(here)                             # 예: .../FRYK
    cwd = os.getcwd()

    for base in (cwd, parent, here):
        yield os.path.join(base, "temp")
    yield "/opt/airflow/temp"
    for base in (cwd, parent, here):
        yield base


def resolve_paths(explicit: str | None):
    tried = []
    for d in candidate_dirs(explicit):
        if d in tried:
            continue
        tried.append(d)
        if os.path.isfile(os.path.join(d, "sheets_info.json")):
            PATHS.update({
                "dir": d,
                "sheets_info": os.path.join(d, "sheets_info.json"),
                "template_tabs_info": os.path.join(d, "template_tabs_info.json"),
                "project_update_info": os.path.join(d, "project_update_info.json"),
                "token": os.path.join(d, "token.pickle"),
                "credentials": os.path.join(d, "credentials.json"),
            })
            return d

    raise SystemExit(
        "sheets_info.json 을 찾지 못했습니다.\n"
        "  찾아본 곳:\n    " + "\n    ".join(tried) + "\n\n"
        "  --temp-dir 로 직접 지정하세요. 예:\n"
        "    python dags/repair.py --temp-dir ~/Desktop/FRYK/temp"
    )


# =========================
# 자격 증명 / 클라이언트
# =========================
def get_setting(key: str, cli_value: str | None = None, required: bool = True):
    """CLI 인자 -> 환경변수 -> Airflow Variable 순으로 조회."""
    if cli_value:
        return cli_value
    if os.environ.get(key):
        return os.environ[key]
    try:
        from airflow.models.variable import Variable  # type: ignore
        return Variable.get(key)
    except Exception:
        pass
    if not required:
        return None
    raise SystemExit(
        f"'{key}' 를 찾을 수 없습니다.\n"
        f"  로컬 실행이라면 환경변수로 넘겨주세요:\n"
        f"    export {key}='...'\n"
        f"  값 확인:\n"
        f"    docker compose exec airflow-scheduler airflow variables get {key}\n"
        f"  또는 Notion 없이 탭 현황만 보려면 --no-notion 을 쓰세요."
    )


def get_sheets_service():
    token_path = PATHS["token"]
    creds_path = PATHS["credentials"]
    creds = None

    if os.path.exists(token_path):
        with open(token_path, "rb") as f:
            creds = pickle.load(f)

    if creds and creds.valid:
        return build("sheets", "v4", credentials=creds, cache_discovery=False)

    if creds and creds.expired and creds.refresh_token:
        print("token 만료 -> refresh 시도")
        creds.refresh(Request())
        with open(token_path, "wb") as f:
            pickle.dump(creds, f)
        return build("sheets", "v4", credentials=creds, cache_discovery=False)

    # 여기까지 왔으면 새로 인증해야 한다
    if not os.path.exists(creds_path):
        raise SystemExit(
            f"인증 정보가 없습니다.\n"
            f"  token.pickle:      {token_path}  ({'있음' if os.path.exists(token_path) else '없음'})\n"
            f"  credentials.json:  {creds_path}  (없음)\n\n"
            f"  둘 중 하나는 있어야 합니다. Airflow 컨테이너에서 복사해 오세요:\n"
            f"    docker compose cp airflow-scheduler:/opt/airflow/temp/token.pickle {token_path}"
        )

    if not sys.stdin.isatty():
        raise SystemExit(
            "재인증이 필요한데 대화형 터미널이 아닙니다.\n"
            "  Mac 터미널에서 직접 실행하세요 (브라우저 인증 창이 뜹니다)."
        )

    from google_auth_oauthlib.flow import InstalledAppFlow
    print("브라우저에서 Google 인증을 진행합니다...")
    flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
    creds = flow.run_local_server(port=0)
    with open(token_path, "wb") as f:
        pickle.dump(creds, f)
    print(f"인증 완료 -> {token_path} 저장\n")
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


# =========================
# Notion REST (라이브러리 비의존 — notion-client 버전 차이 회피)
# =========================
NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION_NEW = "2025-09-03"   # data_sources 지원
NOTION_VERSION_OLD = "2022-06-28"   # databases.query 만 지원


class NotionAPIError(Exception):
    def __init__(self, code, payload):
        self.code = code
        self.payload = payload
        super().__init__(f"Notion API {code}: {payload[:300]}")


class NotionHTTP:
    """
    notion-client 라이브러리를 쓰지 않고 REST 를 직접 호출한다.
    로컬/컨테이너의 라이브러리 버전 차이(data_sources 유무)에 영향받지 않는다.
    """

    def __init__(self, token):
        self.token = token
        self.query_path = None
        self.version = None

    def call(self, method, path, body=None, version=None):
        import urllib.error
        import urllib.request

        url = f"{NOTION_API}/{path.lstrip('/')}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {self.token}")
        req.add_header("Notion-Version", version or self.version or NOTION_VERSION_NEW)
        req.add_header("Content-Type", "application/json")

        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    return json.loads(r.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                payload = e.read().decode("utf-8", errors="replace")
                if e.code in (429, 500, 502, 503, 504) and attempt < 4:
                    time.sleep(min(30.0, 2 ** attempt))
                    continue
                raise NotionAPIError(e.code, payload) from None
            except urllib.error.URLError:
                if attempt < 4:
                    time.sleep(2 ** attempt)
                    continue
                raise
        raise RuntimeError("Notion API 재시도 초과")

    def resolve_endpoint(self, db_id):
        """
        PROJECT_DB_ID 가 data_source_id 인지 database_id 인지,
        워크스페이스가 신/구 API 중 어느 쪽인지 자동 판별한다.
        """
        # 1) 신 API + data_source_id 로 가정
        try:
            self.call("POST", f"data_sources/{db_id}/query", {"page_size": 1},
                      version=NOTION_VERSION_NEW)
            self.query_path, self.version = f"data_sources/{db_id}/query", NOTION_VERSION_NEW
            print(f"  Notion API: {NOTION_VERSION_NEW} / data_sources")
            return
        except NotionAPIError as e:
            if e.code in (401, 403):
                raise SystemExit(
                    f"Notion 인증 실패 ({e.code}). NOTION_TOKEN 을 확인하세요.\n  {e.payload[:200]}"
                )
            if e.code not in (400, 404):
                raise

        # 2) 신 API + database_id 로 가정 -> 첫 data_source 를 찾는다
        try:
            db = self.call("GET", f"databases/{db_id}", version=NOTION_VERSION_NEW)
            sources = db.get("data_sources") or []
            if sources:
                ds_id = sources[0]["id"]
                self.query_path, self.version = f"data_sources/{ds_id}/query", NOTION_VERSION_NEW
                print(f"  Notion API: {NOTION_VERSION_NEW} / database -> data_source {ds_id}")
                return
        except NotionAPIError:
            pass

        # 3) 구 API
        try:
            self.call("POST", f"databases/{db_id}/query", {"page_size": 1},
                      version=NOTION_VERSION_OLD)
            self.query_path, self.version = f"databases/{db_id}/query", NOTION_VERSION_OLD
            print(f"  Notion API: {NOTION_VERSION_OLD} / databases")
            return
        except NotionAPIError as e:
            raise SystemExit(
                f"Notion DB 조회에 실패했습니다 (id={db_id}).\n"
                f"  {e.code}: {e.payload[:300]}\n\n"
                f"  확인할 것:\n"
                f"   - PROJECT_DB_ID 값이 맞는지 "
                f"(docker compose exec airflow-scheduler airflow variables get PROJECT_DB_ID)\n"
                f"   - 해당 DB 가 integration 에 공유돼 있는지 (Notion 페이지 우상단 ... > 연결)"
            )

    def query_all(self, db_id):
        if not self.query_path:
            self.resolve_endpoint(db_id)
        results, cursor = [], None
        while True:
            body = {"page_size": 100}
            if cursor:
                body["start_cursor"] = cursor
            resp = self.call("POST", self.query_path, body)
            results.extend(resp.get("results", []))
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
        return results


def get_notion_client(token):
    return NotionHTTP(token)


# =========================
# JSON
# =========================
def load_json(path):
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_json(path, data):
    directory = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp_", suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def backup(path):
    if not os.path.exists(path):
        return None
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = f"{path}.bak_{stamp}"
    shutil.copy2(path, dst)
    return dst


# =========================
# API helpers
# =========================
def exec_retry(req, tag="", retries=6):
    last = None
    for i in range(retries):
        try:
            return req.execute()
        except HttpError as e:
            if getattr(e.resp, "status", None) in (429, 500, 502, 503, 504):
                last = e
                time.sleep(min(30.0, 2 ** i))
                continue
            raise
    raise RuntimeError(f"Google API 실패: {tag}") from last


def fetch_tabs(sheets, sid):
    meta = exec_retry(
        sheets.spreadsheets().get(
            spreadsheetId=sid, fields="sheets.properties(sheetId,title,index)"
        ),
        tag=f"get {sid}",
    )
    return [
        (int(s["properties"]["sheetId"]), s["properties"]["title"])
        for s in meta.get("sheets", [])
    ]


def query_all_pages(notion, db_id):
    return notion.query_all(db_id)


def get_title(props):
    arr = (props.get("프로젝트명") or {}).get("title") or []
    return "".join(x.get("plain_text", "") for x in arr).strip()


def get_year(props):
    d = (props.get("납품일") or {}).get("date") or {}
    s = d.get("start")
    return s[:4] if s else None


# =========================
# 메인
# =========================
def main():
    ap = argparse.ArgumentParser(
        description="sheets_info.json 을 실제 시트 상태로 재동기화",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--apply", action="store_true", help="실제로 파일을 덮어쓴다 (기본: dry-run)")
    ap.add_argument("--delete-copies", action="store_true",
                    help="'~의 사본' 고아 탭을 실제로 삭제한다 (--apply 필요)")
    ap.add_argument("--temp-dir", default=None,
                    help="json/token 이 있는 폴더 (미지정 시 자동 탐색)")
    ap.add_argument("--year", action="append", default=None,
                    help="특정 연도 키만 처리 (예: --year 2026_결산)")
    ap.add_argument("--no-notion", action="store_true",
                    help="Notion 조회를 건너뛰고 탭 현황만 리포트")
    ap.add_argument("--notion-token", default=None)
    ap.add_argument("--project-db-id", default=None)
    ap.add_argument("--prune-update-info", action="store_true",
                    help="Notion 에 없는 page_id 를 project_update_info.json 에서 제거")
    args = ap.parse_args()

    d = resolve_paths(args.temp_dir)
    print(f"작업 폴더: {d}\n")

    if args.delete_copies and not args.apply:
        print("!! --delete-copies 는 --apply 와 함께 써야 합니다. 이번엔 삭제하지 않습니다.\n")

    sheets = get_sheets_service()

    old_info = load_json(PATHS["sheets_info"])
    update_info = load_json(PATHS["project_update_info"])
    template_tabs = load_json(PATHS["template_tabs_info"])

    if not template_tabs:
        print(f"!! template_tabs_info.json 을 찾지 못했습니다 ({PATHS['template_tabs_info']}).\n"
              f"   시스템 탭(_raw 등)이 프로젝트 탭으로 오인될 수 있습니다.\n")

    system_tab_names = {
        v["name"] for v in template_tabs.values()
        if isinstance(v, dict) and v.get("name")
    }

    # --- Notion 쪽 진실 ---
    notion_by_year: dict[str, dict[str, str]] = {}
    notion_page_ids: set = set()
    dup_names: list[str] = []

    if args.no_notion:
        print("[--no-notion] Notion 조회를 건너뜁니다. 기존 캐시의 page_id 만 보존됩니다.\n")
    else:
        token = get_setting("NOTION_TOKEN", args.notion_token)
        project_db_id = get_setting("PROJECT_DB_ID", args.project_db_id)
        notion = get_notion_client(token)
        print("Notion 프로젝트 DB 조회 중...")
        pages = query_all_pages(notion, project_db_id)
        for p in pages:
            props = p["properties"]
            y, name = get_year(props), get_title(props)
            notion_page_ids.add(p["id"])
            if not y or not name:
                continue
            bucket = notion_by_year.setdefault(f"{y}_결산", {})
            if name in bucket:
                dup_names.append(f"{y}_결산 / {name}")
            bucket[name] = p["id"]
        print(f"  -> 페이지 {len(pages)}건, 연도 {sorted(notion_by_year)}\n")

    # --- 처리 대상 연도 ---
    year_keys = list(old_info.keys())
    for yk in notion_by_year:
        if yk not in year_keys:
            print(f"!! {yk} 가 sheets_info.json 에 없습니다. "
                  f"(스프레드시트를 먼저 만들어야 함 — ensure_year_sheets 가 처리)")
    if args.year:
        year_keys = [y for y in year_keys if y in set(args.year)]
        if not year_keys:
            print(f"!! --year 로 지정한 연도가 sheets_info.json 에 없습니다: {args.year}")
            return 1

    new_info: dict = {}
    report: list[str] = []
    copies_to_delete: list[tuple[str, str, int, str]] = []

    for year_key in year_keys:
        entry = old_info[year_key]
        if not isinstance(entry, dict) or not entry.get("sheet_id"):
            print(f"[SKIP] {year_key}: sheet_id 없음")
            new_info[year_key] = entry
            continue
        sid = entry["sheet_id"]

        old_projects = dict(entry.get("projects") or {})
        for k, v in (entry.get("project_id") or {}).items():
            old_projects.setdefault(k, v)
        if entry.get("project_id") is not None and entry.get("projects") is None:
            report.append(f"[KEY-FIX] {year_key}: 'project_id' -> 'projects' 로 정규화")

        old_by_gid = {
            int(info["gid"]): info
            for info in old_projects.values()
            if isinstance(info, dict) and info.get("gid") is not None
        }

        print(f"=== {year_key}  ({sid}) ===")
        tabs = fetch_tabs(sheets, sid)
        live_gids = {g for g, _ in tabs}
        print(f"  실제 탭 {len(tabs)}개 / 캐시 {len(old_projects)}개")

        notion_names = notion_by_year.get(year_key, {})
        projects: dict[str, dict] = {}
        seen_titles: dict[str, int] = {}
        unknown_tabs, orphan_copies, no_notion = [], [], []

        for gid, title in tabs:
            if title in system_tab_names:
                continue
            if is_copy_tab(title):
                orphan_copies.append((gid, title))
                copies_to_delete.append((year_key, sid, gid, title))
                continue
            if not title.endswith(TAB_SUFFIX):
                unknown_tabs.append((gid, title))
                continue

            name = title[: -len(TAB_SUFFIX)]
            if title in seen_titles:
                report.append(f"[DUP-TAB] {year_key}: '{title}' 중복 gid={seen_titles[title]},{gid}")
                continue
            seen_titles[title] = gid

            page_id = notion_names.get(name)
            if not page_id:
                cached = old_by_gid.get(gid) or old_projects.get(name)
                page_id = (cached or {}).get("notion_page_id")
                if not args.no_notion:
                    no_notion.append((gid, title, bool(page_id)))
                if not page_id:
                    continue

            projects[name] = {
                "notion_page_id": page_id,
                "gid": int(gid),
                "tab_title": title,
            }

        vanished = [
            nm for nm, info in old_projects.items()
            if isinstance(info, dict) and info.get("gid") is not None
            and int(info["gid"]) not in live_gids
        ]
        missing_tabs = [nm for nm in notion_names if nm not in projects]
        recovered = [nm for nm in projects if nm not in old_projects]

        print(f"  복구된(캐시에 없던) 프로젝트: {len(recovered)}")
        for nm in recovered:
            print(f"     + {nm}  (gid={projects[nm]['gid']})")
        if orphan_copies:
            print(f"  고아 사본 탭: {len(orphan_copies)}")
            for gid, t in orphan_copies:
                print(f"     x {t}  (gid={gid})")
        if unknown_tabs:
            print(f"  '{TAB_SUFFIX}' 로 끝나지 않는 미분류 탭: {len(unknown_tabs)}  (건드리지 않음)")
            for gid, t in unknown_tabs:
                print(f"     ? {t}  (gid={gid})")
        if no_notion:
            print(f"  Notion 에 대응 프로젝트가 없는 탭: {len(no_notion)}")
            for gid, t, kept in no_notion:
                print(f"     ! {t}  (gid={gid}) {'-> 기존 page_id 유지' if kept else '-> 매핑 불가, 캐시에서 제외'}")
        if vanished:
            print(f"  캐시에만 있고 실제로 없는 탭: {len(vanished)}")
            for nm in vanished:
                print(f"     - {nm}")
        if missing_tabs:
            print(f"  Notion 에는 있으나 탭 없음(다음 run 에 생성됨): {len(missing_tabs)}")
            for nm in missing_tabs[:20]:
                print(f"     > {nm}")
            if len(missing_tabs) > 20:
                print(f"     ... 외 {len(missing_tabs) - 20}건")

        new_info[year_key] = {"sheet_id": sid, "projects": projects}
        print()

    for k, v in old_info.items():
        if k not in new_info:
            new_info[k] = v

    if dup_names:
        print("!! Notion 내 프로젝트명 중복 (같은 연도에 같은 이름):")
        for x in dup_names:
            print(f"   - {x}")
        print("   -> 이름을 구분하지 않으면 한 탭을 두 페이지가 공유하게 됩니다.\n")

    for line in report:
        print(line)
    if report:
        print()

    stale_updates = [] if args.no_notion else [p for p in update_info if p not in notion_page_ids]
    if stale_updates:
        print(f"project_update_info.json 에 Notion 에 없는 page_id {len(stale_updates)}건"
              f"{' -> 제거함' if args.prune_update_info and args.apply else ' (제거하려면 --prune-update-info)'}")

    total = sum(len(v.get("projects", {})) for v in new_info.values() if isinstance(v, dict))

    if not args.apply:
        print(f"\n[DRY-RUN] 아무것도 변경하지 않았습니다. 반영하려면 --apply 를 붙이세요.")
        print(f"          반영 시 sheets_info.json 프로젝트 총 {total}건이 됩니다.")
        if args.no_notion:
            print(f"          (--no-notion 모드라 Notion 에만 있는 프로젝트는 반영되지 않습니다.\n"
                  f"           최종 복구는 NOTION_TOKEN/PROJECT_DB_ID 를 넣고 다시 돌리세요.)")
        return 0

    if args.no_notion:
        print("\n!! --no-notion 모드에서는 sheets_info.json 을 덮어쓰지 않습니다 "
              "(불완전한 결과로 캐시를 망가뜨릴 수 있음).")
        print("   고아 사본 탭 삭제만 진행합니다.")
    else:
        b = backup(PATHS["sheets_info"])
        if b:
            print(f"\n백업 생성: {b}")
        save_json(PATHS["sheets_info"], new_info)
        print(f"저장 완료: {PATHS['sheets_info']}  (프로젝트 {total}건)")

        if args.prune_update_info and stale_updates:
            b2 = backup(PATHS["project_update_info"])
            if b2:
                print(f"백업 생성: {b2}")
            for pid in stale_updates:
                update_info.pop(pid, None)
            save_json(PATHS["project_update_info"], update_info)
            print(f"저장 완료: {PATHS['project_update_info']}")

    if args.delete_copies and copies_to_delete:
        print(f"\n고아 사본 탭 {len(copies_to_delete)}개 삭제 중...")
        by_sid: dict[str, list[int]] = {}
        for _, sid, gid, _t in copies_to_delete:
            by_sid.setdefault(sid, []).append(gid)
        for sid, gids in by_sid.items():
            exec_retry(
                sheets.spreadsheets().batchUpdate(
                    spreadsheetId=sid,
                    body={"requests": [{"deleteSheet": {"sheetId": g}} for g in gids]},
                ),
                tag=f"delete copies {sid}",
            )
            print(f"  {sid}: {len(gids)}개 삭제")

    print("\n완료.")
    if not args.no_notion:
        print("이제 sheets_info.json 을 컨테이너로 되돌리고 syncNotionSheets DAG 을 수동 실행하세요:")
        print(f"  docker compose cp {PATHS['sheets_info']} airflow-scheduler:/opt/airflow/temp/sheets_info.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
    