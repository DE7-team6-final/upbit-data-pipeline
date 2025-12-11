# Producer Module

Upbit WebSocket → Kafka(Redpanda) 스트리밍 Producer

## Features
- WebSocket 연결
- 자동 reconnect + heartbeat
- stream_time 추가
- partition key = 종목 코드

## Run
python producer.py