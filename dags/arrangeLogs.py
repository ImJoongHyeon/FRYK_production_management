import os
import shutil
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator

def arrange_old_logs():
    log_dir = '/opt/airflow/logs'
    # 14일치 로그 파일 계산
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=14)).date()
    delete_count = 0

    # dag_id : logs 아래 각 폴더명
    # dag_path : logs 아래 각 폴더의 경로
    # run_id : dag_id=~~ 폴더 아래의 각 폴더명
    # run_path : run_id로 되어있는 로그를 담고있는 파일 경로
    # run_time_str : 런타임 시간 문자열
    
    for dag_id in os.listdir(log_dir):
        dag_path = os.path.join(log_dir, dag_id)
        if os.path.isdir(dag_path):
            for run_id in os.listdir(dag_path):
                run_path = os.path.join(dag_path, run_id)
                if os.path.isdir(run_path):
                    run_time_str = run_id.split('__')[-1]
                    try:
                        run_time = datetime.fromisoformat(run_time_str).date()
                        # 지금으로부터 14일 전 보다 더 이전 파일들 제거
                        if run_time < cutoff_date:
                            shutil.rmtree(run_path)
                            delete_count += 1
                            print(f"Deleted logs: {run_path}")
                    except ValueError:
                        print(f"Skipping: {run_path}, unable to parse date from {run_time_str}")
    
    print(f"Total logs deleted: {delete_count}")

default_args = {
    'owner': 'joonghyeon',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    dag_id='arrangeLogs',
    default_args=default_args,
    schedule='0 17 * * 1-5',  # 매일 새벽 2시에 실행
    catchup=False,
    max_active_runs=1,
    tags=['log', 'arrange', 'cleanup']
) as dag:
    arrange_task = PythonOperator(
        task_id='arrange_old_logs',
        python_callable=arrange_old_logs,
    )
    arrange_task
