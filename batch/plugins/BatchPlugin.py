from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task

import logging
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import io

'''
    캔들 데이터 수집 DAG에 공통적으로 사용되는 요소들
'''

BASE_URL = 'https://api.upbit.com/v1/'
MARKETS = ['KRW-BTC', 'KRW-ETH', 'KRW-XRP', 'KRW-SOL']
DEFAULT_PARAMS = {
    'market': None,
    'to': None,
    'count': None
}

@task
def transform_and_load_to_s3(all_market_data, key):
    hook = S3Hook(aws_conn_id = 'aws_conn_id')

    today = datetime.today().strftime('%Y-%m-%d')
    try:
        print(all_market_data)
        print('길이', len(all_market_data))
        data = pa.Table.from_pylist(all_market_data)

        buffer = io.BytesIO()
        pq.write_table(data, buffer)
        data_bytes = buffer.getvalue()

        key = f'yymmdd={today}/{key}.parquet'
        hook.load_bytes(
            bytes_data = data_bytes,
            key = key,
            bucket_name = 'team6-batch',
            replace = True
        )
        logging.info('Transform, Load Complete')
    except Exception as e:
        logging.info('Transform, Load Error')
        print(e)
        raise e
