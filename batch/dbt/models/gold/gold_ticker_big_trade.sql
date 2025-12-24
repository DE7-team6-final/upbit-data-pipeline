--코인별로 나눠서 매수, 매도량 보여주기 
with source as (
    select * from {{ ref('silver_ticker') }}
),

calculated as ( 
    select 
        trade_timestamp, 
        trade_date,
        code,
        case 
            when ask_bid = "BID" then '매수'
            when ask_bid = "ASK" then '매도'
            else ask_bid
        end as side,
        trade_price,
        trade_volume,
        (trade_price*trade_volume) AS trade_amount
    from source 
)

select 
    trade_timestamp, 
    trade_date,
    code,
    side,
    trade_price,
    trade_volume,
    trade_amount
from calculated
where trade_amount >=300000000
order by trade_timestamp desc