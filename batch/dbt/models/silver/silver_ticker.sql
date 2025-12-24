{{ config(
    MATERIALIZED='incremental',
    unique_key=['code', 'trade_timestamp']
)}}

WITH source_data AS (
    SELECT * FROM {{ source('silver', 'BRONZE_TICKER') }}

    {% if is_incremental() %}
        WHERE ingestion_time > (SELECT MAX(ingestion_time) FROM {{ this }})
    {% endif %}
),

filtered_data AS (
    SELECT *
    FROM source_data
    WHERE trade_price > 0            
      AND trade_date IS NOT NULL      
),

deduplicated_data AS (
    SELECT
        code,
        trade_price,
        trade_volume,
        
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', trade_timestamp) as trade_timestamp,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', stream_time) as stream_time,
        
        trade_date,
        opening_price,
        high_price,
        low_price,
        prev_closing_price,
        change,
        change_price,       
        change_rate,       
        acc_trade_price,
        acc_trade_volume,   
        ask_bid,
        CONVERT_TIMEZONE('UTC', 'Asia/Seoul', ingestion_time) as ingestion_time, 

        ROW_NUMBER() OVER (
            PARTITION BY code, trade_timestamp
            ORDER BY stream_time DESC
        ) as rn
    FROM filtered_data
)

SELECT * EXCLUDE(rn)
FROM deduplicated_data
WHERE rn = 1