from airflow import DAG
from airflow.decorators import task

from BatchPlugin import transform_and_load_to_s3
from BatchPlugin import BASE_URL, MARKETS, DEFAULT_PARAMS

from datetime import datetime
import requests
import time
import logging

with DAG(
    dag_id = 'days_candle',
    start_date = datetime(2025, 12, 17),
    schedule = '33 0 * * *', # 한국 시간은 +9시
    catchup = False,
    tags = ["upbit", "candle", "raw"]
) as dag:
    '''
        일간 캔들의 데이터를 수집합니다.
        
        오늘의 날짜를 가져옵니다. 오늘의 날짜와 DAG 시작일이 같다면 최초 실행, 아니라면 반복 실행으로 간주합니다.
        최초 실행이라면 200일 전 ~ 현재의 데이터를, 반복 실행이라면 최근 7일의 데이터를 가져옵니다.
        추출된 데이터는 parquet 형태로 파싱하여 S3에 적재됩니다.
    '''

    @task
    def check_first_time(**context):
        start_date = context['dag'].start_date.strftime('%Y%m%d')
        today = datetime.today().strftime('%Y%m%d')

        if start_date == today:
            count = 200
            logging.info('First Run')
        else:
            count = 7
            logging.info('Incremental Run')

        return count

    @task
    def extract(count):
        today = datetime.today().strftime('%Y-%m-%dT00:00:00')
        url = BASE_URL + 'days'
        market = MARKETS.copy()
        params = DEFAULT_PARAMS.copy()
        params['to'] = today
        params['count'] = count

        all_market_data = []
        for m in market:
            try:
                params['market'] = m
                response = requests.get(url = url, params = params)
                data = response.json()
                all_market_data = all_market_data + data
            except Exception as e:
                logging.info(f'Extract Error. Coin name: {m}')
                print(f'Extract Error. Coin name: {m}')
                raise e
            time.sleep(1)

        logging.info('Extract Complete')
        return all_market_data
        
    count = check_first_time()
    all_market_data = extract(count)
    transform_and_load_to_s3(all_market_data, key = 'candle_days')
