from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from SlackAlert import send_slack_failure_callback

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': send_slack_failure_callback,
}

with DAG(
    'stock_gold_and_big_ticker_dag',
    default_args=default_args,
    description='Run dbt gold models every 10 mins',
    schedule_interval=None,  # 10분마다 실행
    start_date=datetime(2025, 12, 29),
    catchup=False,
    max_active_runs=1 # 이전 작업 안 끝나면 대기
) as dag:

    # dbt run 실행
    run_dbt = BashOperator(
        task_id='dbt_run_gold',
         bash_command=(
            "cd /opt/dbt && "
            "source /opt/dbt_venv/bin/activate && "
            "dbt run --select gold_stock_ticker_trend gold_correlation_matrix gold_ticker_big_trade"
        ),
    )