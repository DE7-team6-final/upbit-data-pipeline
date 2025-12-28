from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from batch.plugins.SlackAlert import send_slack_failure_callback

# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': send_slack_failure_callback,
}

with DAG(
    dag_id='ticker_to_silver_gold_dag',
    default_args=default_args,
    description='Update Silver Ticker table incrementally',
    schedule_interval='*/10 * * * *', 
    start_date=pendulum.datetime(2025, 12, 24),
    catchup=False,
    tags=['dbt', 'silver', 'crypto'],
    max_active_runs=1
) as dag:
    
    load_upbit_data = SnowflakeOperator(
        task_id='load_upbit_data_to_snowflake',

        snowflake_conn_id='snowflake_conn_id',

        sql="""
            BEGIN;

            -- 1. 임시 테이블 생성 
            CREATE OR REPLACE TEMPORARY TABLE TEMP_TICKER LIKE SILVER_TICKER;

            -- 2. 임시 테이블에 일단 적재
            COPY INTO TEMP_TICKER (
                CODE, TRADE_PRICE, OPENING_PRICE, HIGH_PRICE, LOW_PRICE, PREV_CLOSING_PRICE,
                CHANGE, CHANGE_PRICE, CHANGE_RATE, 
                TRADE_VOLUME, ACC_TRADE_PRICE, ACC_TRADE_VOLUME, ASK_BID,
                TRADE_TIMESTAMP, STREAM_TIME, TRADE_DATE
            )
            FROM (
                SELECT 
                    $1:code::VARCHAR,
                    $1:trade_price::FLOAT,
                    $1:opening_price::FLOAT,
                    $1:high_price::FLOAT,
                    $1:low_price::FLOAT,
                    $1:prev_closing_price::FLOAT,
                    $1:change::VARCHAR,
                    $1:change_price::FLOAT,
                    $1:change_rate::FLOAT,
                    $1:trade_volume::FLOAT,
                    $1:acc_trade_price::FLOAT,
                    $1:acc_trade_volume::FLOAT,
                    $1:ask_bid::VARCHAR,
                    TO_TIMESTAMP_LTZ($1:trade_timestamp::NUMBER, 3), 
                    TO_TIMESTAMP_LTZ($1:stream_time::NUMBER, 3),
                    TO_DATE($1:trade_date::VARCHAR, 'YYYYMMDD')
                FROM @gcs_stage
            )
            --  오늘 날짜 파일만 스캔 
            PATTERN = '.*{{ ds_nodash }}.*.jsonl'
            ON_ERROR = CONTINUE;

            
            MERGE INTO SILVER_TICKER AS T
            USING (
                
                SELECT * FROM TEMP_TICKER
                QUALIFY ROW_NUMBER() OVER (PARTITION BY CODE, TRADE_TIMESTAMP ORDER BY STREAM_TIME DESC) = 1
            ) AS S
            ON T.CODE = S.CODE 
           AND T.TRADE_TIMESTAMP = S.TRADE_TIMESTAMP -- PK 기준 비교

            
            WHEN NOT MATCHED THEN
                INSERT (
                    CODE, TRADE_PRICE, OPENING_PRICE, HIGH_PRICE, LOW_PRICE, PREV_CLOSING_PRICE,
                    CHANGE, CHANGE_PRICE, CHANGE_RATE, 
                    TRADE_VOLUME, ACC_TRADE_PRICE, ACC_TRADE_VOLUME, ASK_BID,
                    TRADE_TIMESTAMP, STREAM_TIME, TRADE_DATE
                ) VALUES (
                    S.CODE, S.TRADE_PRICE, S.OPENING_PRICE, S.HIGH_PRICE, S.LOW_PRICE, S.PREV_CLOSING_PRICE,
                    S.CHANGE, S.CHANGE_PRICE, S.CHANGE_RATE, 
                    S.TRADE_VOLUME, S.ACC_TRADE_PRICE, S.ACC_TRADE_VOLUME, S.ASK_BID,
                    S.TRADE_TIMESTAMP, S.STREAM_TIME, S.TRADE_DATE
                );

            COMMIT;
        """
    )
    
    run_silver_ticker = BashOperator(
        task_id='run_silver_ticker',
        bash_command=(            
            "cd /opt/dbt && "
            "source /opt/dbt_venv/bin/activate &&"
            "dbt run --select silver_ticker "
            
        )
    )

    trigger_anomaly_detect = TriggerDagRunOperator(
        task_id='trigger_anomaly_detect_dag',
        trigger_dag_id='ticker_anomaly_detect_dag', 
        wait_for_completion=False 
    )
 
    load_upbit_data >> run_silver_ticker >> trigger_anomaly_detect

