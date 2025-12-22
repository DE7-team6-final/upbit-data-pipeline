"""
Airflow DAG: Batch Trade → Snowflake Silver

Responsibilities:
- Read daily trade parquet files from S3 (partitioned by execution date)
- Normalize raw schema into Silver schema
- Load unified trade data into Snowflake SILVER_TRADES table

Notes:
- Execution date (ds) represents the partition date in S3
- TRADE_DATE is derived from trade timestamp (KST)
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
from SlackAlert import send_slack_failure_callback


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 17),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'on_failure_callback': send_slack_failure_callback
}


S3_BUCKET = "team6-batch"
SNOWFLAKE_CONN_ID = "snowflake_conn_id"
TARGET_TABLE = "SILVER_TRADES"


def load_trades_to_snowflake(ds, **context):
    """
    Load daily trade parquet files from S3 into Snowflake Silver table.

    Steps:
    1. List S3 objects for execution date partition
    2. Read trade parquet files
    3. Normalize schema (Raw → Silver)
    4. Derive TRADE_DATE from trade timestamp (KST)
    5. Append data into Snowflake
    """

    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    prefix = f"yymmdd={ds}/"
    logging.info(f"Listing S3 objects with prefix: {prefix}")

    keys = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=prefix) or []

    # trade parquet filename may vary slightly depending on upstream
    trade_files = [
        key for key in keys
        if key.endswith("trade.parquet") or "trade" in key
    ]
    if not trade_files:
        logging.info("No trade files found for this execution date.")
        return

    all_dataframes = []

    for key in trade_files:
        logging.info(f"Processing file: {key}")

        obj = s3_hook.get_key(key, bucket_name=S3_BUCKET)
        parquet_bytes = obj.get()["Body"].read()

        table = pq.read_table(io.BytesIO(parquet_bytes))
        df = table.to_pandas()

        # Debug: log raw columns
        logging.debug(f"Raw trade columns: {df.columns.tolist()}")

        all_dataframes.append(df)

    unified_df = pd.concat(all_dataframes, ignore_index=True)
    logging.info(f"Total records loaded from S3: {len(unified_df)}")

    # detect timestamp column (upstream schema may vary)
    if "trade_timestamp" in unified_df.columns:
        ts_col = "trade_timestamp"
    elif "timestamp" in unified_df.columns:
        ts_col = "timestamp"
    else:
        raise ValueError("No timestamp column found in raw trade data")

    raw_required = ["market", ts_col, "trade_price", "trade_volume"]
    missing_raw = [c for c in raw_required if c not in unified_df.columns]
    if missing_raw:
        raise ValueError(f"Missing required raw columns: {missing_raw}")

    # Raw → Silver mapping (Notion schema)
    unified_df = unified_df.rename(
        columns={
            "market": "CODE",
            ts_col: "TRADE_TS",
            "trade_price": "TRADE_PRICE",
            "trade_volume": "TRADE_VOLUME",
            "prev_closing_price": "PREV_CLOSING_PRICE",
            "change_price": "CHANGE_PRICE",
            "ask_bid": "ASK_BID",
            "sequential_id": "SEQUENTIAL_ID",
        }
    )

    # Explicit timestamp parsing (epoch ms)
    unified_df["TRADE_TS"] = pd.to_datetime(
        unified_df["TRADE_TS"],
        unit="ms",
        utc=True,
    )

    if unified_df["TRADE_TS"].isna().any():
        raise ValueError("TRADE_TS contains invalid values after parsing")

    unified_df["TRADE_DATE"] = (
        unified_df["TRADE_TS"]
        .dt.tz_convert("Asia/Seoul")
        .dt.date
    )

    # ============================
    # Snowflake write stabilization
    # ============================
    unified_df["TRADE_TS"] = (
        unified_df["TRADE_TS"]
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )

    # Ingestion and lineage
    unified_df["INGESTION_TIME"] = pd.Timestamp.utcnow().tz_localize(None)
    unified_df["SOURCE"] = "batch_trade"

    # Optional columns may not exist depending on upstream
    # If missing, create them as null to keep Snowflake insert consistent
    optional_cols = ["PREV_CLOSING_PRICE", "CHANGE_PRICE", "ASK_BID", "SEQUENTIAL_ID"]
    for col in optional_cols:
        if col not in unified_df.columns:
            unified_df[col] = None

    expected_columns = [
        "CODE",
        "TRADE_DATE",
        "TRADE_TS",
        "TRADE_PRICE",
        "TRADE_VOLUME",
        "PREV_CLOSING_PRICE",
        "CHANGE_PRICE",
        "ASK_BID",
        "SEQUENTIAL_ID",
        "INGESTION_TIME",
        "SOURCE",
    ]

    missing = set(expected_columns) - set(unified_df.columns)
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    unified_df = unified_df[expected_columns]

    logging.info("==== TRADE_DATE SAMPLE ====")
    logging.info(unified_df[["TRADE_TS", "TRADE_DATE"]].head().to_string())
    logging.info("==== TRADE_DATE COUNTS ====")
    logging.info(unified_df["TRADE_DATE"].value_counts().head().to_string())

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

    logging.info(f"Snowflake write complete: success={success}, rows={nrows}")


with DAG(
    dag_id="batch_trade_to_silver_dag",
    default_args=default_args,
    description="Load batch trade data from S3 into Snowflake Silver layer",
    schedule_interval="0 1  * * *", # UTC 01:00 = KST 10:00
    catchup=False,
    max_active_runs=1,
    concurrency=1,
) as dag:

    load_trades_task = PythonOperator(
        task_id="load_trades_to_snowflake",
        python_callable=load_trades_to_snowflake,
    )
