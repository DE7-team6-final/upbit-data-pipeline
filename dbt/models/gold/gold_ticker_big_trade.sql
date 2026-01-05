{{ config(
    materialized='incremental',
    unique_key=['code', 'candle_time']
)}}

WITH silver_data AS (
    SELECT * FROM {{ source('silver', 'SILVER_TICKER') }}
    
    {% if is_incremental() %}
    
    WHERE trade_timestamp >= DATEADD('hour', -1, (SELECT MAX(candle_time) FROM {{ this }}))
    {% endif %}
),

calculated_1min AS (
    SELECT
        code,
        -- 1분 단위로 자르기
        DATE_TRUNC('minute', trade_timestamp) AS candle_time,
        ARRAY_AGG(trade_price) WITHIN GROUP (ORDER BY stream_time ASC)[0]::FLOAT AS open_price,
        MAX(trade_price) AS high_price,
        MIN(trade_price) AS low_price,
        ARRAY_AGG(trade_price) WITHIN GROUP (ORDER BY stream_time DESC)[0]::FLOAT AS close_price,
        
        SUM(trade_volume) AS total_volume,
        SUM(trade_price * trade_volume) AS total_amount,
        
        
        MAX(CASE WHEN (trade_price * trade_volume) >= 300000000 THEN 1 ELSE 0 END)::BOOLEAN AS has_whale,
        MAX(CASE WHEN (trade_price * trade_volume) >= 300000000 THEN trade_price ELSE NULL END) AS whale_price,
        
        MAX(CASE 
            WHEN (trade_price * trade_volume) >= 300000000 AND ask_bid = 'BID' THEN '매수'
            WHEN (trade_price * trade_volume) >= 300000000 AND ask_bid = 'ASK' THEN '매도'
            ELSE NULL 
        END) AS whale_side,
        
        MAX(CASE WHEN (trade_price * trade_volume) >= 300000000 THEN (trade_price * trade_volume) ELSE NULL END) AS max_whale_amount

    FROM silver_data
    GROUP BY 1, 2
)

SELECT * FROM calculated_1min
ORDER BY candle_time DESC