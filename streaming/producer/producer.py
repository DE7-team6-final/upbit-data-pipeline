"""
Upbit WebSocket → Redpanda Streaming Producer (Optimized & Documented Version)

This module connects to Upbit’s WebSocket API and streams real-time ticker data 
into a Redpanda (Kafka-compatible) topic with batching, compression, idempotence, 
and automatic reconnection.

Key Features:
    • Stable WebSocket connection with exponential backoff
    • Kafka producer optimized for real-time streaming on small GCP VM
    • LZ4 compression → lower network cost and disk usage
    • Idempotent producer → prevents duplicate messages during retries
    • Reduced logging noise (prints only errors + every 1000 successful sends)
"""

import asyncio
import json
import time
from typing import Any, Dict, List

import websockets
from confluent_kafka import Producer


# ---------------------------------------------------------------------
# Kafka / Upbit configuration
# ---------------------------------------------------------------------

BROKER = "10.128.0.5:19092"  # Redpanda internal IP in GCP
TOPIC = "upbit-ticks"

# Markets to subscribe from Upbit
CODES: List[str] = ["KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-SOL"]

# Optimized Kafka Producer Configuration
KAFKA_CONF = {
    "bootstrap.servers": BROKER,

    # Reliability vs Speed (ideal for real-time ticker data)
    "enable.idempotence": True,      # Prevents duplicates during retry
    "acks": "1",                      # Faster than "all", acceptable for market ticks
    "retries": 5,
    "retry.backoff.ms": 200,

    # Performance & Cloud Cost Optimization
    "linger.ms": 5,                   # Batch messages for up to 5ms
    "batch.size": 64 * 1024,          # 64KB batching
    "compression.type": "lz4",        # Highly efficient for JSON data

    # Connection Stability
    "socket.keepalive.enable": True,
}

producer = Producer(KAFKA_CONF)

# Counter used to limit logging frequency
msg_count = 0


# ---------------------------------------------------------------------
# Kafka delivery callback
# ---------------------------------------------------------------------

def delivery_report(err, msg) -> None:
    """
    Kafka delivery callback function.

    Logs:
        • Errors ALWAYS
        • Successful sends every 1000th message only (to reduce CPU/log noise)
    """
    global msg_count

    if err:
        print(f"❌ Kafka delivery failed: {err}")
        return

    msg_count += 1
    if msg_count % 1000 == 0:
        print(f"🟢 Kafka OK | Sent {msg_count} messages | latest offset={msg.offset()}")


# ---------------------------------------------------------------------
# WebSocket → Kafka streaming logic
# ---------------------------------------------------------------------

async def connect_and_stream() -> None:
    """
    Maintain a persistent WebSocket connection to Upbit,
    receive real-time ticker data, enrich it with a timestamp,
    and push it into Kafka using the optimized producer.
    """
    uri = "wss://api.upbit.com/websocket/v1"

    # Subscription message format expected by Upbit WebSocket API
    subscribe_msg: List[Dict[str, Any]] = [
        {"ticket": "upbit-producer"},
        {"type": "ticker", "codes": CODES},
    ]

    backoff = 1  # exponential reconnection delay

    while True:
        try:
            print(f"🔗 Connecting to Upbit WebSocket... (codes={CODES})")

            async with websockets.connect(
                uri,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
            ) as ws:

                print("✅ Connected to Upbit WebSocket.")
                backoff = 1  # reset reconnect delay

                await ws.send(json.dumps(subscribe_msg))

                # MAIN LOOP: Receive → Process → Produce to Kafka
                while True:
                    raw = await ws.recv()
                    parsed = json.loads(raw)

                    # Add producer-side processing timestamp
                    parsed["stream_time"] = int(time.time() * 1000)

                    # Send to Kafka
                    producer.produce(
                        TOPIC,
                        key=parsed["code"].encode(),
                        value=json.dumps(parsed).encode(),
                        callback=delivery_report,
                    )

                    # Non-blocking poll to trigger delivery reports
                    producer.poll(0)

        except websockets.exceptions.ConnectionClosed:
            print("⚠️ WebSocket connection closed by server.")

        except Exception as e:
            print(f"⚠️ Unexpected error: {e}")

        # Exponential backoff reconnection
        print(f"⏳ Reconnecting in {backoff} sec...")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)  # cap at 30 sec


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------

async def main() -> None:
    """Main async entry point."""
    await connect_and_stream()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Interrupted by user. Flushing remaining messages...")
        producer.flush(5)
