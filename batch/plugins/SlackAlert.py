from airflow.models import Variable

from datetime import timedelta
import requests
import logging

URL = Variable.get('slack_webhook_url')
HEADERS = {
    'Content-type': 'applycation/json'
}

def send_message(json):
    response = requests.post(URL, headers = HEADERS, json = json)
    if response.status_code == 200:
        logging.info('슬랙 전송 성공')
    else:
        logging.info('슬랙 전송 실패')    

def send_slack_failure_callback(context):
    ti = context.get('task_instance')
    execution_date = context.get('execution_date').strftime('%Y-%m-%d %H:%M')
    run_id = ti.run_id
    dag_id = context.get('dag').dag_id
    task_id = ti.task_id
    exception = context.get('exception')

    data = f'''
        :x: DAG Failed
        • Execution Date : {execution_date}
        • Run id: {run_id}
        • DAG id: {dag_id}
        • task id: {task_id}
        ```{exception}```
    '''    
    json = {'text': data}
    send_message(json)

def send_slack_success_callback(context):
    ti = context.get('task_instance')
    execution_date = context.get('execution_date').strftime('%Y-%m-%d %H:%M')
    run_id = ti.run_id
    dag_id = context.get('dag').dag_id
    duration = ti.duration

    data = f'''
        :white_check_mark: DAG Succeeded
        • Execution Date: {execution_date}
        • Run id: {run_id}
        • DAG id: {dag_id}
        • Duration: {duration:.1f}s
    '''
    
    json = {'text': data}
    send_message(json)
