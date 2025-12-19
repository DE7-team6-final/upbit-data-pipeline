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
    schedule='34 0 * * *',
    catchup=False,
    tags=["upbit", "trades", "raw"],
) as dag:
    """
        Airflow DAG: Upbit Trades Raw Ingest (fetch_trades -> S3)
        BTC의 체결 데이터는 하루에 대략 200,000건 내외를 가지고 있습니다.
        많은 용량으로 인한 메모리 사용량 부족/비용 문제가 있으므로
        가장 거래량이 많은 시간대인 22:00 ~ 02:00(UTC-13:00 ~ 17:00)의 체결 데이터를 가져옵니다.
        
        추출된 데이터는 parquet 형태로 파싱하여 S3에 적재됩니다.
    """

    @task
    def fetch_trades():
        """
            UTC-13:00 ~ 17:00사이의 비트코인의 체결 데이터를 가져옵니다.
            한 번의 호출 시 500개의 가져오며 13시가 되지 않았다면
            sequential_id 를 이용하여 해당 지점부터 데이터를 가져옵니다.
        """
        yesterday = (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')
        url = BASE_URL + 'trades/ticks'
        params = DEFAULT_PARAMS.copy()
        params['market'] = 'KRW-BTC'
        params['count'] = 500
        params['cursor'] = None
        params['days_ago'] = 1

        ingest = True
        market_data = []
        try:
            while ingest:
                response = requests.get(url = url, params = params)
                data = response.json()

                if data[-1]['trade_time_utc'] >= '13:00:00':
                    market_data = market_data + data
                    params['cursor'] = data[-1]['sequential_id']
                else:
                    temp = []
                    for i in data:
                        if i['trade_time_utc'] < '13:00:00':
                            ingest = False
                            break
                        else:
                            temp.append(i)
                    market_data = market_data + temp
                time.sleep(0.5)
        except Exception as e:
            logging.info(f'Fetch Error')
            print(f'Fetch Error')
            raise e
        
        logging.info(f'Fetch Complete.')
        return market_data

    trades_data = fetch_trades()
    transform_and_load_to_s3(trades_data, key = 'trade')
