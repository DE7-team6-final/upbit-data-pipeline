from airflow import DAG
from airflow.decorators import task

from datetime import datetime
import logging
import time

with DAG(
    dag_id = 'test_dag',
    start_data = datetime(2025, 12, 18),
    catchup = False,
    tags = ['upbit', 'test']
) as dag:

    @task
    def test_task():
        logging.info('Start Test Task')
        print('Start Test Task')
        
        time.sleep(5)

        logging.info('Dag Test Complete')
        print('Dag Test Complete')

    test_task()
