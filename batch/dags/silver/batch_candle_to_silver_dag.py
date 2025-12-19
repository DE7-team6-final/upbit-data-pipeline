"""
Airflow DAG: Batch Candle → Snowflake Silver

Responsibilities:
- Read daily candle parquet files from S3 (partitioned by execution date)
- Filter only candle-related files
- Derive candle interval from filename (1m, 5m, 1h, 1d)
- Normalize raw schema into Silver schema
- Load unified candle data into Snowflake SILVER_CANDLES table

Notes:
- Execution date (ds) represents the partition date in S3
- TRADE_DATE is derived from candle timestamp (KST)
- Append-only strategy (idempotency can be added later)
"""

from snowflake.connector.pandas_tools import write_pandas
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
import io
import pyarrow.parquet as pq
import logging

# -------------------------------------------------------------------
# DAG default arguments
# -------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 17),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
S3_BUCKET = "team6-batch"
SNOWFLAKE_CONN_ID = "snowflake_conn_id"
TARGET_TABLE = "SILVER_CANDLES"

# Filename-based candle interval mapping
INTERVAL_MAP = {
    "candle_1min": "1m",
    "candle_5min": "5m",
    "candle_1hour": "1h",
    "candle_days": "1d",
}

# -------------------------------------------------------------------
# Helper functions
# -------------------------------------------------------------------
def get_candle_interval(filename: str) -> str:
    """
    Derive candle interval from filename.

    Example:
    - candle_1min.parquet  -> 1m
    - candle_5min.parquet  -> 5m
    """
    for key, interval in INTERVAL_MAP.items():
        if key in filename:
            return interval
    raise ValueError(f"Unknown candle interval for filename: {filename}")


# -------------------------------------------------------------------
# Main task logic
# -------------------------------------------------------------------
def load_candles_to_snowflake(ds, **context):
    """
    Load daily candle parquet files from S3 into Snowflake Silver table.

    Steps:
    1. List S3 objects for execution date partition
    2. Read candle parquet files
    3. Normalize schema (Raw → Silver)
    4. Derive TRADE_DATE from candle timestamp (KST)
    5. Append data into Snowflake
    """

    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    prefix = f"yymmdd={ds}/"
    logging.info(f"Listing S3 objects with prefix: {prefix}")

    keys = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=prefix) or []
    candle_files = [key for key in keys if "candle" in key]

    if not candle_files:
        logging.info("No candle files found for this execution date.")
        return

    all_dataframes = []

    for key in candle_files:
        logging.info(f"Processing file: {key}")

        obj = s3_hook.get_key(key, bucket_name=S3_BUCKET)
        parquet_bytes = obj.get()["Body"].read()

        table = pq.read_table(io.BytesIO(parquet_bytes))
        df = table.to_pandas()

        df["CANDLE_INTERVAL"] = get_candle_interval(key)
        df["INGESTION_TIME"] = datetime.utcnow()

        all_dataframes.append(df)

    # -------------------------------------------------------------------
    # Schema normalization & validation
    # -------------------------------------------------------------------
    unified_df = pd.concat(all_dataframes, ignore_index=True)
    logging.info(f"Total records to insert: {len(unified_df)}")

    # Raw → Silver column mapping
    unified_df = unified_df.rename(
        columns={
            "market": "CODE",
            "timestamp": "CANDLE_TS",
            "opening_price": "OPEN_PRICE",
            "high_price": "HIGH_PRICE",
            "low_price": "LOW_PRICE",
            "trade_price": "CLOSE_PRICE",
            "candle_acc_trade_volume": "VOLUME",
        }
    )

    # Explicit timestamp parsing (Upbit candle timestamp = epoch ms)
    unified_df["CANDLE_TS"] = pd.to_datetime(
        unified_df["CANDLE_TS"],
        unit="ms",
        utc=True,
        errors="coerce",
    )

    if unified_df["CANDLE_TS"].isna().any():
        raise ValueError("CANDLE_TS contains invalid values after parsing")

    # Derived Silver columns
    unified_df["TRADE_PRICE"] = unified_df["CLOSE_PRICE"]
    unified_df["TRADE_DATE"] = (
        unified_df["CANDLE_TS"]
        .dt.tz_convert("Asia/Seoul")
        .dt.date
    )

    # ============================
    # Snowflake write stabilization
    # ============================
    unified_df["CANDLE_TS"] = (
        unified_df["CANDLE_TS"]
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )

    unified_df["SOURCE"] = "UPBIT_BATCH"

    expected_columns = [
        "CODE",
        "CANDLE_INTERVAL",
        "CANDLE_TS",
        "OPEN_PRICE",
        "HIGH_PRICE",
        "LOW_PRICE",
        "CLOSE_PRICE",
        "VOLUME",
        "TRADE_PRICE",
        "TRADE_DATE",
        "INGESTION_TIME",
        "SOURCE",
    ]

    missing = set(expected_columns) - set(unified_df.columns)
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    unified_df = unified_df[expected_columns]

    # -------------------------------------------------------------------
    # Debug logging (safe to keep for early production phase)
    # -------------------------------------------------------------------
    logging.info("==== TRADE_DATE SAMPLE ====")
    logging.info(unified_df[["CANDLE_TS", "TRADE_DATE"]].head().to_string())
    logging.info("==== TRADE_DATE COUNTS ====")
    logging.info(unified_df["TRADE_DATE"].value_counts().head().to_string())

    # -------------------------------------------------------------------
    # Snowflake write (append-only)
    # -------------------------------------------------------------------
    conn = snowflake_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    cur.execute("USE DATABASE UPBIT_DB")
    cur.execute("USE SCHEMA SILVER")

    success, nchunks, nrows, _ = write_pandas(
        conn,
        unified_df,
        table_name=TARGET_TABLE,
        database="UPBIT_DB",
        schema="SILVER",
        use_logical_type=True,
    )

    logging.info(
        f"Snowflake write complete: success={success}, rows={nrows}"
    )


# -------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------
with DAG(
    dag_id="batch_candle_to_silver_dag",
    default_args=default_args,
    description="Load batch candle data from S3 into Snowflake Silver layer",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    concurrency=1,
) as dag:

    load_candles_task = PythonOperator(
        task_id="load_candles_to_snowflake",
        python_callable=load_candles_to_snowflake,
    )
