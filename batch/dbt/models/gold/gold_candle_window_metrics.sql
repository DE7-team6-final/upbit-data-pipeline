with base as (

    select
        code,
        candle_interval,
        trade_date,
        candle_ts,
        close_price
    from {{ source('silver', 'SILVER_CANDLES') }}

),

window_metrics as (

    select
        code,
        candle_interval,
        trade_date,
        close_price,

        avg(close_price) over (
            partition by code, candle_interval
            order by candle_ts
            rows between 4 preceding and current row
        ) as ma_5,

        stddev(close_price) over (
            partition by code, candle_interval
            order by candle_ts
            rows between 4 preceding and current row
        ) as std_5

    from base

),

zscore_calc as (

    select
        code,
        candle_interval,
        trade_date,
        close_price,
        ma_5,
        std_5,

        close_price - ma_5 as deviation,

        case
            when std_5 = 0 then null
            else (close_price - ma_5) / std_5
        end as zscore

    from window_metrics

)

select *
from zscore_calc
