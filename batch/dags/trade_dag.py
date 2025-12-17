from airflow import DAG
from airflow.decorators import task

from BatchPlugin import transform_and_load_to_s3
from BatchPlugin import BASE_URL, MARKETS, DEFAULT_PARAMS

import logging
from datetime import datetime, timedelta
import requests
import time

with DAG(
    dag_id="trades_ingest",
    start_date=datetime(2025, 12, 17),
    schedule='34 * * * *',
    catchup=False,
    tags=["upbit", "trades", "raw"],
) as dag:
    """
        Airflow DAG: Upbit Trades Raw Ingest (fetch_trades -> S3)
        체결 데이터는 하루에 대략 200,000건 내외를 가지고 있습니다.
        많은 용량으로 인한 메모리 사용량 부족/비용 문제로 10,000개를 가져옵니다.
        
        어제의 날짜를 가져와 표준 시간 0시(한국 9시)를 기준으로
        최근 20,000개의 체결 데이터를 수집합니다.
        추출된 데이터는 parquet 형태로 파싱하여 S3에 적재됩니다.
    """

    @task
    def fetch_trades():
        """
            UTC(00:00) 이전 체결 데이터를 최대 10,000개 가져옵니다.
            한 번의 시행에서 최대 500개를 가져옵니다.
            10,000개를 채우지 못했을 경우, sequential_id를 기준으로 이어서 데이터를 수집합니다.
        """
        yesterday = (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')
        url = BASE_URL + 'trades/ticks'
        market = MARKETS.copy()
        params = DEFAULT_PARAMS.copy()
        params['count'] = 500
        params['cursor'] = None
        params['days_ago'] = 1

        all_market_data = []
        for m in market:
            market_data = []
            try:
                while len(market_data) < 10000:
                    params['market'] = m
                    response = requests.get(url = url, params = params)
                    data = response.json()

                    if data[-1]['trade_date_utc'] == yesterday:
                        market_data = market_data + data
                        params['cursor'] = data[-1]['sequential_id']
                    else:
                        temp = []
                        for i in data:
                            if i['trade_date_utc'] != yesterday:
                                break
                            else:
                                temp.append(i)
                        market_data = market_data + temp
                    time.sleep(1)
            except Exception as e:
                logging.info(f'Fetch Error. Coin name: {m}')
                print(f'Fetch Error. Coin name: {m}')
                raise e
            all_market_data = all_market_data + market_data
            logging.info(f'Fetch Complete. Coin name: {m}')
        
        logging.info(f'Fetch Task Complete.')
        return all_market_data

    trades_data = fetch_trades()
    transform_and_load_to_s3(trades_data, key = 'trade')
