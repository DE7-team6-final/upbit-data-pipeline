from airflow import DAG
from airflow.decorators import task

from CandlePlugin import transform_and_load_to_s3
from CandlePlugin import BASE_URL, MARKETS, DEFAULT_PARAMS

from datetime import datetime, timedelta
import requests
import time
import logging

with DAG(
    dag_id = '1min_candle',
    start_date = datetime(2025, 12, 17),
    schedule = '30 0 * * *', # 한국 시간은 +9시
    catchup = False,
    tags = ["upbit", "candle", "raw"]
) as dag:
    '''
        1분 캔들의 데이터를 수집합니다.
        
        수집 날짜(0시, 한국은 9시)와 전날의 날짜를 가져옵니다. 이후 3시간 간격으로 기준이 되는 시간 목록을 만들어냅니다.
        기준 시간에 대해 반복하여 코인별 1440개의 데이터를 추출합니다.
        추출된 데이터는 parquet 형태로 파싱하여 S3에 적재됩니다.
    '''

    @task
    def extract():
        url = BASE_URL + 'minutes/1'
        market = MARKETS.copy()
        params = DEFAULT_PARAMS.copy()
        params['count'] = 180

        hours = [21, 18, 15, 12, 9, 6, 3]
        today = datetime.now().strftime('%Y-%m-%dT00:00:00')
        yesterday = datetime.strptime(today, '%Y-%m-%dT%H:%M:%S') - timedelta(days = 1)
        time_list = [today] + [yesterday.replace(hour = i).strftime('%Y-%m-%dT%H:00:00') for i in hours]

        all_market_data = []
        for m in market:
            params['market'] = m
            for t in time_list:
                try:
                    params['to'] = t
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

    all_market_data = extract()
    transform_and_load_to_s3(all_market_data, key = 'candle_1min')
