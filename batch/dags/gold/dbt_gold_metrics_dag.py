"""
Airflow DAG: dbt Gold automation

- Runs dbt Gold models (Silver → Gold)
- Executed on Airflow Worker (EC2)
- Schedule: 01:15 UTC (10:15 KST)
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from SlackAlert import send_slack_failure_callback

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 19),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'on_failure_callback': send_slack_failure_callback
}

with DAG(
    dag_id="dbt_gold_metrics",
    default_args=default_args,
    description="Run dbt Gold models (Silver → Gold)",
    schedule_interval="15 1 * * *",  # 01:15 UTC = 10:15 KST
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["dbt", "gold", "snowflake"],
) as dag:

    run_dbt_gold_models = BashOperator(
        task_id="run_dbt_gold_models",
        bash_command=(
            "cd /opt/dbt && "
            "/opt/dbt_venv/bin/dbt run "
            "--select gold "
        ),
    )
