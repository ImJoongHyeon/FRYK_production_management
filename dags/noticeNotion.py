from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable

from notion_client import Client as NotionClient
from datetime import datetime, timedelta

import time

# === 환경 변수 ===
NOTION_TOKEN = Variable.get('NOTION_TOKEN')
NOTION_NOTICE_DB_ID = Variable.get('NOTION_NOTICE_DB_ID') # 테이블 블록의 ID

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

def notice_add():
    # === 연결 ===
    notion = NotionClient(auth=NOTION_TOKEN)

    results = query_all_pages(notion=notion, data_source_id=NOTION_NOTICE_DB_ID)
    
    for notice in results:
        props = notice["properties"]
        add_check = props["각 프로젝트 추가 여부"]["checkbox"]
        project_id = props["프로젝트명"]["title"][0]["href"].replace('https://www.notion.so/', '') if props["프로젝트명"]["title"] and props["프로젝트명"]["title"][0]["href"] else ''
        notice_text = props["공지사항"]["rich_text"][0]["plain_text"].strip()
        
        if add_check or len(notice_text) == 0 or project_id == '':
            continue
        
        
        notion.pages.update(
            page_id=notice["id"],
            properties={
                "각 프로젝트 추가 여부": {"checkbox": True}
            }
        )
        notion.blocks.children.append(
            block_id=project_id,
            children=[
                {
                    "object": "block",
                    "type": "to_do",
                    "to_do": {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {
                                    "content": f"{notice_text}"
                                }
                            }
                        ]
                    }
                }
            ]
        )
        time.sleep(1)

# === DAG 정의 ===
default_args = {
    'owner': 'joonghyeonan',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 23),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}
with DAG(
    dag_id='addNoticeEachProject',
    default_args=default_args,
    schedule='*/30 9-17 * * 1-5',
    catchup=False,
    max_active_runs=1,
    tags=['notion', 'gsheet', 'automation', 'update']
) as dag:
    notice_task = PythonOperator(
        task_id='project_notice_add',
        python_callable=notice_add
    )
    notice_task
