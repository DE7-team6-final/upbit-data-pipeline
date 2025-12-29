{{ config(
    MATERIALIZED='incremental',
    unique_key=['code', 'candle_time'],
    incremental_strategy='merge'
)}}

with silver_candles as(
    select code,
        date_trunc('MINUTE', trade_timestamp) as candle_time,
        --1분 내 가장 빠른 시간의 가격
        array_agg(trade_price) within group (order by trade_timestamp asc)[0]::float as open_price, 
        max(trade_price) as high_price,
        min(trade_price) as low_price,
        array_agg(trade_price) within group (order by trade_timestamp desc)[0]::float as close_price,
        sum(trade_volume) as volume
    from {{ ref('silver_ticker')}}

    {% if is_incremental() %} --최근 2시간 데이터만 증분처리 
        where trade_timestamp >= dateadd(hour, -2, (select max(candle_time) from {{ this }}))
    {% endif %}
    group by 1,2
),

metrics as (
    SELECT code,
        candle_interval,
        candle_time,
        zscore

    from {{ ref('gold_candle_window_metrics')}}

    WHERE candle_interval='1m'
    )

select
    s.code,
    s.candle_time,
    s.open_price,
    s.high_price,
    s.low_price,
    s.close_price,
    s.volume,

    coalesce(m.zscore, 0) as z_score, --zscore가 없으면 0으로 처리 
    CASE 
        WHEN abs(coalesce(m.zscore, 0)) > 1 then True --이상치 1.0 이상이면 점 찍음   
        ELSE False 
    END as is_anomaly,

    current_timestamp() as updated_at

from silver_candles s
left join metrics m 
    on s.code=m.code

    and s.candle_time= m.candle_ts

