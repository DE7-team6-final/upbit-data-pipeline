{{ config(
    materialized='incremental',
    unique_key=['code', 'trade_timestamp']
) }}

WITH source AS (
    -- 파이썬이 데이터를 쏟아부은 Raw 테이블 (테이블명은 환경에 맞게 수정)
    SELECT * FROM {{ source('raw', 'raw_stock_history') }}
    
    {% if is_incremental() %}
    -- 증분 실행 시, 최근 2일치 데이터만 스캔해서 비용 절약
    WHERE collected_at >= DATEADD('day', -2, CURRENT_TIMESTAMP())
    {% endif %}
),

cleaned AS (
    SELECT
        code,
        name,
        market,
        
        -- 1. 날짜와 시간을 합쳐서 제대로 된 타임스탬프 생성
        -- (원본 포맷이 YYYYMMDD, HHMMSS 문자열이라고 가정)
        TO_TIMESTAMP(trade_date || trade_time, 'YYYYMMDDHH24MISS') AS trade_timestamp,
        
        -- 분석용으로 일자만 따로 필요할 때를 위해 DATE 타입으로 변환
        TO_DATE(trade_date, 'YYYYMMDD') AS trade_date,
        
        -- 2. 숫자형 데이터 안전하게 캐스팅 (API 오류 방지)
        TRY_CAST(open AS FLOAT) AS open_price,
        TRY_CAST(high AS FLOAT) AS high_price,
        TRY_CAST(low AS FLOAT) AS low_price,
        TRY_CAST(close AS FLOAT) AS close_price,
        TRY_CAST(volume AS FLOAT) AS volume,
        
        -- 수집 시점 (중복 제거 기준용)
        collected_at

    FROM source
)

SELECT
    trade_timestamp, -- 이제 이게 메인 시간축
    trade_date,
    code,
    name,
    market,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    collected_at
FROM cleaned

-- 3. 중복 제거 (Dedup)
-- 같은 종목(code), 같은 시간(trade_timestamp) 데이터가 중복으로 들어오면
-- 가장 늦게 수집된(collected_at이 큰) 1건만 남긴다.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY code, trade_timestamp 
    ORDER BY collected_at DESC
) = 1