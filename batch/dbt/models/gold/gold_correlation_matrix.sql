with stock_returns as (
    select
        CODE,
        NAME,
        to_timestamp(trade_date:VARCHAR || LPAD(trade_time::VARCHAR,6,'0'), 'YYYYMMDDHH24MISS') AS candle_time,
        CLOSE_PRICE,
        (CLOSE_PRICE - LAG(CLOSE_PRICE) OVER (PARTITION BY CODE ORDER BY TRADE_DATE, TRADE_TIME)) 
        / NULLIF(LAG(CLOSE_PRICE) OVER (PARTITION BY CODE ORDER BY TRADE_DATE, TRADE_TIME),0) AS UPDOWN_RATE
    FROM {{ ref('silver_stock')}}
),

ticker_returns as (
    select 
        code, 
        DATE_TRUNC('MINUTE', trade_timestamp) AS candle_time,
        AVG(trade_price) as avg_price,
        (avg_price - LAG(avg_price) OVER (PARTITION BY code ORDER BY candle_time)) 
        / NULLIF(LAG(avg_price) OVER (PARTITION BY code ORDER BY candle_time), 0) AS updown_rate
    FROM {{ ref('silver_ticker') }}
    GROUP BY 1, 2
),

joined_data as (
    select 
        s.name as stock_name,
        c.code as ticker_code,
        s.updown_rate as stock_return,
        c.updown_rate as ticker_return
    from stock_returns s 
    inner join ticker_returns c 
        on s.candle_time = c.candle_time
    where s.updown_rate is not NULL
        and c.updown_rate is not NULL
)

select 
    stock_name,
    ticker_code,
    CORR(stock_return, ticker_return) as corrlation_coefficient,
    count(*) as data_points
from joined_data
group by 1, 2
having count(*) > 10
order by 3 desc ;