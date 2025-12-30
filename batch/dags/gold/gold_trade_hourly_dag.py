from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime
from SlackAlert import send_slack_failure_callback

with DAG(
    dag_id = 'dbt_gold_trade_hourly',
    start_date = datetime(2025, 12, 26),
    schedule = '30 1 * * *',
    catchup = False,
    tags = ['upbit', 'candle', 'gold'],
    default_args = {
        'on_failure_callback': send_slack_failure_callback
    },
) as dag:
    
    run_dbt_gold_trade_hourly = BashOperator(
        task_id = 'dbt_gold_trade_hourly',
        bash_command = (
            'cd /opt/dbt && '
            'source /opt/dbt_venv/bin/activate && '
            'dbt run --select gold_trade_hourly'
        )
    )
