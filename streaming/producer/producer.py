"""
Upbit WebSocket → Redpanda Producer (Stable Version)

Features:
- Automatic reconnect with exponential backoff
- Heartbeat (ping interval/timeout)
- Kafka key-partitioning by market code
- stream_time 추가로 latency 측정 가능

Used in: streaming/producer
"""

import asyncio
import json
import time
from typing import Any, Dict, List

import websockets
from confluent_kafka import Producer


BROKER = "localhost:19092"
TOPIC = "upbit-ticks"
CODES: List[str] = ["KRW-BTC", "KRW-ETH", "KRW-XRP"]

KAFKA_CONF = {
    "bootstrap.servers": BROKER,
    "linger.ms": 5,
    "batch.size": 32_768,
    "acks": "1",
}

producer = Producer(KAFKA_CONF)


def delivery_report(err, msg) -> None:
    """Kafka delivery callback function."""
    if err:
        print(f"❌ Kafka delivery failed: {err}")
    else:
        print(f"🟢 Kafka OK | offset={msg.offset()}")


async def connect_and_stream() -> None:
    """Connect to Upbit WebSocket and stream data into Kafka."""
    uri = "wss://api.upbit.com/websocket/v1"

    subscribe_msg: List[Dict[str, Any]] = [
        {"ticket": "upbit-producer"},
        {"type": "ticker", "codes": CODES},
    ]

    backoff: int = 1  # reconnect delay

    while True:
        try:
            print("🔗 Connecting to Upbit WebSocket...")

            async with websockets.connect(
                uri,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
            ) as ws:

                print("✅ Connected to Upbit WebSocket.")
                backoff = 1  # reset backoff after successful connect

                await ws.send(json.dumps(subscribe_msg))

                while True:
                    raw = await ws.recv()
                    parsed = json.loads(raw)

                    parsed["stream_time"] = int(time.time() * 1000)

                    producer.produce(
                        TOPIC,
                        key=parsed["code"].encode(),
                        value=json.dumps(parsed).encode(),
                        callback=delivery_report,
                    )
                    producer.poll(0)

        except websockets.exceptions.ConnectionClosed:
            print("⚠️ Connection closed by server.")
        except websockets.exceptions.WebSocketException as e:
            print(f"⚠️ WebSocket exception: {e}")
        except Exception as e:
            print(f"⚠️ Unexpected error: {e}")

        print(f"⏳ Reconnecting in {backoff} sec...\n")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)


async def main() -> None:
    await connect_and_stream()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Interrupted by user. Exiting...")
