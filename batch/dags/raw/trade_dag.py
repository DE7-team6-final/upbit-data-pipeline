"""
Airflow DAG: Upbit Trades Raw Ingest (fetch_trades -> S3)
- devcourse EC2 가용시간(09~21 KST) 동안 latest trades 원본 수집
- 중복 허용, 가공 없음 (raw layer)
- EC2 비용 절감 batch ingest
"""

import logging
from datetime import datetime, timedelta, timezone
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


BASE = "https://api.upbit.com/v1"
MARKETS = ["KRW-BTC", "KRW-ETH", "KRW-SOL", "KRW-XRP"]

S3_BUCKET = "team6-batch"
S3_PREFIX = "trades"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="upbit_trades_raw_ingest",
    start_date=datetime(2025, 1, 1),
    # schedule="0/5 * * * *",
    # schedule="0 * * * *",
    # schedule="0 0 * * *",
    catchup=False,
    default_args=default_args,
    tags=["upbit", "trades", "raw"],
) as dag:

    @task
    def fetch_trades(**context):
        """
        Airflow logical_date 기준으로 운영시간에만 trades 원본 수집.
        - 09~21(KST) 외 시간은 의도적으로 skip
        - 최신 500개 trades 반환
        """
        logical_date = context["logical_date"]  # Airflow 기준 시간 (UTC)
        kst_hour = (logical_date + timedelta(hours=9)).hour

        # 운영시간 가드 (EC2 가동시간만 책임)
        if kst_hour < 9 or kst_hour >= 21:
            logging.info(f"Skipping fetch: {kst_hour}시, 운영시간 아님")
            return []

        all_data = []
        for market in MARKETS:
            url = f"{BASE}/trades/ticks"
            params = {"market": market, "count": 500}
            try:            
                res = requests.get(url, params=params)
                res.raise_for_status()
                data = res.json()
                # fetch_timestamp = 데이터 수집 기준 시간 (logical_date)
                for row in data:
                    row["fetch_timestamp"] = logical_date.strftime("%Y%m%d-%H%M%S")
                all_data.append({"market": market, "rows": data})
                logging.info(f"Fetched {len(data)} rows for {market} at {logical_date}")
            except requests.RequestException as e:
                logging.error(f"Fetch failed for {market} at {logical_date}: {e}")
                continue  # 실패한 마켓만 skip

        return all_data

    @task
    def save_to_s3(trades_data, **context):
        """
        trades raw data (Parquet) -> S3 raw layer save.
        - logical_date 기준 파티션
        - 덮어쓰기 없음
        """
        if not trades_data:
            logging.info("No data to save (out of operating hours or fetch failed)")
            return "no data (out of operating hours)"

        from io import BytesIO
        hook = S3Hook(aws_conn_id="aws_s3")
        logical_date = context["logical_date"]
        dt = logical_date.strftime("%Y-%m-%d")
        timestamp = logical_date.strftime("%Y%m%d_%H%M%S")

        for t in trades_data:
            market = t["market"]
            rows = t["rows"]
            try:
                table = pa.Table.from_pylist(rows)
                parquet_buffer = BytesIO()
                pq.write_table(table, parquet_buffer)
                parquet_buffer.seek(0)

                key = (
                    f"{S3_PREFIX}/"
                    f"market={market}/"
                    f"dt={dt}/"
                    f"{timestamp}.parquet"
                )

                hook.load_bytes(
                    bytes_data=parquet_buffer.getvalue(),
                    key=key,
                    bucket_name=S3_BUCKET,
                    replace=False   # raw 보존
                )

                logging.info(f"S3 save complete: {key} ({len(rows)} rows)")
            except Exception as e:
                logging.error(f"S3 save failed for {market} at {logical_date}: {e}")
                continue  # 실패한 마켓만 skip

        return f"{len(trades_data)} markets raw data saved"

    trades_data = fetch_trades()
    save_to_s3(trades_data)