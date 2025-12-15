from airflow import DAG
from airflow.decorators import task

from CandlePlugin import transform_and_load_to_s3
from CandlePlugin import BASE_URL, MARKETS, DEFAULT_PARAMS

from datetime import datetime, date, timedelta
import requests
import time
import logging

with DAG(
    dag_id = '5min_candle',
    start_date = datetime(2025, 12, 15),
    schedule = '1 0 * * *', # 한국 시간은 +9시
    catchup = False,
    tags = ["upbit", "candle", "raw"]
) as dag:
    '''
        5분 캔들의 데이터를 수집합니다.
        
        수집 날짜(0시, 한국은 9시)와 12시간 전 날짜를 가져옵니다.
        두 시간대에 대해 반복하여 코인별 288개의 데이터를 추출합니다.
        추출된 데이터는 json 형태로 파싱하여 S3에 적재됩니다.
    '''

    @task
    def extract():
        url = BASE_URL + 'minutes/5'
        market = MARKETS.copy()
        params = DEFAULT_PARAMS.copy()
        params['count'] = 144

        today = datetime.combine(date.today(), datetime.min.time())
        yesterday = today - timedelta(hours = 12)
        time_list = [i.strftime('%Y-%m-%dT%H:00:00') for i in [today, yesterday]]

        all_market_data = []
        for m in market:
            params['market'] = m
            market_data = []
            for t in time_list:
                try:
                    params['to'] = t
                    response = requests.get(url = url, params = params)
                    data = response.json()
                    market_data = market_data + data
                except Exception as e:
                    logging.info(f'Extract Error. Coin name: {m}')
                    print(f'Extract Error. Coin name: {m}')
                    raise e
            all_market_data.append({'market': m, 'raw': market_data})
            time.sleep(1)
        
        logging.info('Extract Complete')
        return all_market_data

    all_market_data = extract()
    transform_and_load_to_s3(all_market_data, prefix = 'candle_5min')
