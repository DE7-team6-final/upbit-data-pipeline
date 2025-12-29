with stock_1min as (
    select 
        CODE as ticker,
        NAME as name,
        'STOCK' as asset_type,
        to_timestamp(trade_date || trade_time, 'YYYYMMDDHH24MISS') AS candle_time,
        CLOSE_PRICE as price,
        VOLUME as volume
    from {{ source('silver', 'silver_stock') }}
),

ticker_1min as (
    select 
        code as ticker,
        code as name,
        "TICKER" as asset_type,
        DATE_TRUNC('MINUTE', trade_timestamp) as candle_time,
        avg(trade_price) as price,
        sum(trade_volume) as volume
    from {{ ref('silver_ticker')}}
    group by 1,2,3,4
)

select * from stock_1min
union all 
select * from ticker_1min