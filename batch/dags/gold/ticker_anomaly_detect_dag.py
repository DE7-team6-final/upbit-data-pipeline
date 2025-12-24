from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'ticker_anomaly_detect_dag',
    default_args=default_args,
    description='Run dbt gold models every 10 mins',
    schedule_interval=None,  # 10분마다 실행
    start_date=datetime(2025, 12, 24),
    catchup=False,
    max_active_runs=1 # 이전 작업 안 끝나면 대기
) as dag:

    # dbt run 실행
    run_dbt = BashOperator(
        task_id='dbt_run_gold',
         bash_command=(
            "cd /opt/dbt && "
            "/opt/dbt_venv/bin/dbt run "
            "--select gold_ticker_line_chart_anomaly_detect "
            
        ),
    )