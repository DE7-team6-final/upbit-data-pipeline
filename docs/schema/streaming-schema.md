# Streaming Message Schema

## Producer → Kafka(Redpanda) 메시지 구조 예시

```json
{
  "type": "ticker",
  "code": "KRW-BTC",
  "trade_price": 50342000.0,
  "trade_volume": 0.0021,
  "timestamp": 1700000000000,
  "stream_time": "2024-12-10T12:00:00.123456Z"
}

| 필드           | 설명                           |
| ------------ | ---------------------------- |
| type         | 메시지 타입 (ticker/trade 등)      |
| code         | 종목 코드                        |
| trade_price  | 현재 거래 가격                     |
| trade_volume | 거래량                          |
| timestamp    | Upbit 서버 타임                  |
| stream_time  | producer 수신 시간 (latency 측정용) |
