from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task

import logging
from datetime import date
import pyarrow as pa
import pyarrow.parquet as pq
import io

BASE_URL = 'https://api.upbit.com/v1/candles/'
MARKETS = ['KRW-BTC', 'KRW-ETH', 'KRW-XRP', 'KRW-SOL']
DEFAULT_PARAMS = {
    'market': None,
    'to': None,
    'count': None
}

@task
def transform_and_load_to_s3(all_market_data, prefix):
    hook = S3Hook(aws_conn_id = 'aws_s3')

    today = date.today().strftime('%Y-%m-%d')
    for m in all_market_data:
        try:
            market = m['market']
            raw = m['raw']
            data = pa.Table.from_pylist(raw)

            buffer = io.BytesIO()
            pq.write_table(data, buffer)
            data_bytes = buffer.getvalue()

            key = f'{prefix}/market={market}/{today}.parquet'
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
