from airflow import DAG
from airflow.decorators import task

from BatchPlugin import transform_and_load_to_s3
from BatchPlugin import BASE_URL, MARKETS, DEFAULT_PARAMS

from datetime import datetime
import requests
import time
import logging

with DAG(
    dag_id = '1hour_candle',
    start_date = datetime(2025, 12, 17),
    schedule = '32 0 * * *', # 한국 시간은 +9시,
    catchup = False,
    tags = ["upbit", "candle", "raw"]
) as dag:
    '''
        1시간(60분) 캔들의 데이터를 수집합니다.
        
        수집 날짜(0시, 한국은 9시)를 가져옵니다.
        날짜를 기준으로 24시간 이전까지의 데이터를 가져와 코인별 24개의 데이터를 추출합니다.
        추출된 데이터는 parquet 형태로 파싱하여 S3에 적재됩니다.
    '''

    @task
    def extract():
        today = datetime.today().strftime('%Y-%m-%dT00:00:00')
        url = BASE_URL + 'candles/minutes/60'
        market = MARKETS.copy()
        params = DEFAULT_PARAMS.copy()
        params['to'] = today
        params['count'] = 24
        
        all_market_data = []
        for m in market:
            try:
                params['market'] = m
                response = requests.get(url = url, params = params)
                data = response.json()
                all_market_data = all_market_data + data
            except Exception as e:
                logging.info(f'Extract Error. Coin name: {m}, Error: {data}')
                raise e
            time.sleep(0.5)

        logging.info('Extract Complete')
        return all_market_data
    
    all_market_data = extract()
    transform_and_load_to_s3(all_market_data, key = 'candle_1hour')
