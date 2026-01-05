"""
Streaming Price & Volume Alert

- Source: Upbit ticker JSONL stored in GCS
- Processing: Aggregate ticker events into 1-minute bars (price, volume)
- Logic: Compare current 1-minute bar against recent SMA baseline
- Mode: Real-time / near-real-time (polling GCS)
- Purpose: Detect short-term price spikes and volume spikes separately
            and notify via Slack


Design goals:
- Minimize false positives
- Simple, explainable rules
- Independent from batch / Gold pipeline
"""


import os
import json
import time
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional

import requests
from google.cloud import storage


# =========================
# Config (Streaming alert)
# =========================
WINDOW_MINUTES = 5

# Asset-aware price thresholds (1-min bar vs 5-min SMA)
PRICE_THRESHOLDS = {
    "KRW-BTC": 0.015,  # ±1.5%
    "KRW-ETH": 0.013,  # ±1.3%
    "KRW-SOL": 0.011,  # ±1.1%
    "KRW-XRP": 0.009,  # ±0.9%
}

DEFAULT_PRICE_THRESHOLD = 0.012  # ±1.2%
VOLUME_MULTIPLIER = 4.5     # 4.5x
COOLDOWN_SECONDS = 10 * 60
POLL_SECONDS = 10

KST = timezone(timedelta(hours=9))


# =========================
# Time helpers
# =========================
def minute_key(ts_ms: int) -> int:
    """Convert timestamp(ms) to minute bucket"""
    return ts_ms // 60000


def minute_to_str(min_key: int) -> str:
    """Minute bucket to human-readable KST time"""
    return datetime.fromtimestamp(min_key * 60, tz=KST).strftime("%Y-%m-%d %H:%M")


# =========================
# Minute Aggregator
# =========================
class MinuteAggregator:
    """
    Collect ticker events and build 1-minute bars per market.
    """

    def __init__(self):
        # market -> current minute key
        self.current_minute: Dict[str, int] = {}

        # market -> last trade_price in the minute (close)
        self.current_close: Dict[str, float] = {}

        # market -> accumulated trade_volume in the minute
        self.current_volume: Dict[str, float] = {}

        # market -> deque of finalized minute bars
        self.history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=WINDOW_MINUTES)
        )

    def ingest(
        self,
        market: str,
        ts_ms: int,
        price: float,
        volume: float,
    ) -> Optional[dict]:
        """
        Ingest one ticker event.

        Returns:
            finalized bar dict when minute changes, otherwise None
        """
                
        # build 1-minute bar per market
        # - minute_key = ts_ms // 60000
        # - first time seeing market -> init state
        # - same minute -> update close price and add volume
        # - minute changed -> finalize previous bar and return it

        min_key = minute_key(ts_ms)
        if market not in self.current_minute:
            self.current_minute[market] = min_key
            self.current_close[market] = price
            self.current_volume[market] = volume
            return None
        
        if min_key == self.current_minute[market]:
            self.current_close[market] = price
            self.current_volume[market] += volume
            return None
        
        finalized_bar = {
            "market": market,
            "minute": self.current_minute[market],
            "close_price": self.current_close[market],
            "volume": self.current_volume[market],
        }

        # store finalized bar
        self.history[market].append(finalized_bar)

        # reset for new minute
        self.current_minute[market] = min_key
        self.current_close[market] = price
        self.current_volume[market] = volume

        return finalized_bar
        

    def get_sma(self, market: str) -> Optional[tuple]:
        """
        Return (avg_price, avg_volume) over last WINDOW_MINUTES bars.
        """
        # return None if not enough bars
        # calculate average close price and average volume
        # return (avg_price, avg_volume)
        if len(self.history[market]) < WINDOW_MINUTES:
            return None
        
        bars = self.history[market]
        avg_price = sum(bar["close_price"] for bar in bars) / len(bars)
        avg_volume = sum(bar["volume"] for bar in bars) / len(bars)
        
        return (avg_price, avg_volume)

# =========================
# Slack notifier
# =========================
def send_slack(webhook: str, message: str):
    """
    Send Slack message.
    If webhook is empty, print to stdout (dry-run).
    """
    if not webhook:
        print(f"DRY-RUN Slack message: {message}")
        return

    payload = {"text": message}
    response = requests.post(webhook, json=payload, timeout=5)
    if response.status_code != 200:
        raise RuntimeError(f"Slack notification failed: {response.text}")
    

def send_price_spike_alert(
    webhook: str,
    market: str,
    price_change: float,
    minute: int,
):
    message = (
        f"🚨 *Price Spike Alert (Streaming)*\n"
        f"Market: `{market}`\n"
        f"Price change: {price_change:.2%}\n"
        f"Time (KST): {minute_to_str(minute)}"
    )
    send_slack(webhook, message)


def send_volume_spike_alert(
    webhook: str,
    market: str,
    volume_ratio: float,
    minute: int,
):
    message = (
        f"📊 *Volume Spike Alert (Streaming)*\n"
        f"Market: `{market}`\n"
        f"Volume ratio: {volume_ratio:.2f}x\n"
        f"Time (KST): {minute_to_str(minute)}"
    )
    send_slack(webhook, message)


# =========================
# Alert Worker
# =========================
class AlertWorker:
    def __init__(self, bucket: str, prefix: str, slack_webhook: str):
        self.bucket = bucket
        self.prefix = prefix
        self.slack_webhook = slack_webhook

        self.aggregator = MinuteAggregator()

        # market -> last alert timestamp
        self.cooldowns: Dict[str, float] = {}

        # GCS client is optional (skip in dry-run)
        self.client = None
        if bucket != "dry-run":
            self.client = storage.Client()
    
    def list_latest_jsonl_blobs(self, limit: int = 1) -> list:
        """
        List latest JSONL blobs from GCS prefix.
        """
        if not self.client:
            return []

        blobs = self.client.list_blobs(self.bucket, prefix=self.prefix)
        jsonl_blobs = [blob for blob in blobs if blob.name.endswith(".jsonl")]
        # sort by updated time descending
        sorted_blobs = sorted(jsonl_blobs, key=lambda b: b.updated, reverse=True)
        return sorted_blobs[:limit]
    
    def read_jsonl_blob(self, blob_name: str, max_lines: int = 2000) -> list[dict]:
        """
        Read JSONL blob from GCS and return list of dicts.
        """
        if not self.client:
            return []

        blob = self.client.bucket(self.bucket).blob(blob_name)
        content = blob.download_as_text()
        lines = content.strip().splitlines()[:max_lines]
        records = [json.loads(line) for line in lines]
        return records
    
        # checkpoint helpers
    CHECKPOINT_FILE = ".alert_worker_checkpoint"

    def save_checkpoint(self, blob_name: str):
        """
        Save current state as a checkpoint.
        """
        with open(self.CHECKPOINT_FILE, "w") as f:
            f.write(blob_name)

    def load_checkpoint(self) -> Optional[str]:
        """
        Load state from the latest checkpoint.
        """
        try:
            with open(self.CHECKPOINT_FILE, "r") as f:
                return f.read().strip()
        except FileNotFoundError:
            return None

    def list_unprocessed_blobs(self, limit: int = 3):
        """
        List unprocessed blobs since last checkpoint.
        """
        if not self.client:
            return []

        blobs = self.client.list_blobs(self.bucket, prefix=self.prefix)
        jsonl_blobs = [blob for blob in blobs if blob.name.endswith(".jsonl")]

        # sort by blob name (chronological order encoded in filename)
        sorted_blobs = sorted(jsonl_blobs, key=lambda b: b.name)

        checkpoint = self.load_checkpoint()
        unprocessed = []

        for blob in sorted_blobs:
            if checkpoint is None or blob.name > checkpoint:
                unprocessed.append(blob)
            if len(unprocessed) >= limit:
                break
            
        return unprocessed
    
    def run_gcs_loop(self):
        """
        Main loop using GCS blobs.
        """
        print("AlertWorker GCS loop started")

        while True:
            unprocessed_blobs = self.list_unprocessed_blobs(limit=3)
            if not unprocessed_blobs:
                print("No new JSONL blobs found, sleeping...")
                time.sleep(POLL_SECONDS)
                continue

            for blob in unprocessed_blobs:
                print(f"Processing blob: {blob.name}")
                try:
                    records = self.read_jsonl_blob(blob.name, max_lines=2000)
                    for record in records:
                        bar = self.aggregator.ingest(
                            market=record["code"],
                            ts_ms=record["trade_timestamp"],
                            price=record["trade_price"],
                            volume=record["trade_volume"],
                        )
                        if bar:
                            self.detect_and_alert(record["code"], bar)

                    # save checkpoint after processing blob
                    self.save_checkpoint(blob.name)
                
                except Exception as e:
                    print(f"Failed processing blob {blob.name}: {e}")

            time.sleep(POLL_SECONDS)

    def in_cooldown(self, market: str) -> bool:
        # TODO: cooldown check
        last_alert = self.cooldowns.get(market)
        if last_alert is None:
            return False
        return last_alert > time.time() - COOLDOWN_SECONDS

    def set_cooldown(self, market: str):
        # TODO: set cooldown
        self.cooldowns[market] = time.time()

    def detect_and_alert(self, market: str, bar: dict):
        """
        Compare current bar vs SMA and send Slack alert if needed.
        """
        # get SMA for market
        # if SMA not ready, return
        # calculate price change rate and volume ratio
        # if price OR volume condition met and not in cooldown:
        #   print alert (dry-run)
        #   set cooldown
        sma = self.aggregator.get_sma(market)
        if sma is None:
            return
        avg_price, avg_volume = sma

        # calculate price change rate and volume ratio
        price_change = (bar["close_price"] - avg_price) / avg_price
        volume_ratio = bar["volume"] / avg_volume

        threshold = PRICE_THRESHOLDS.get(market, DEFAULT_PRICE_THRESHOLD)
        price_hit = abs(price_change) >= threshold
        volume_hit = volume_ratio >= VOLUME_MULTIPLIER

        if price_hit and not self.in_cooldown(market):
            print(
                f"[Price Spike] {market} "
                f"price_change={price_change:.2%} "
                f"time={minute_to_str(bar['minute'])}"
            )
            send_price_spike_alert(
                self.slack_webhook,
                market,
                price_change,
                bar["minute"],
            )
            self.set_cooldown(market)

        if volume_hit and not self.in_cooldown(market):
            print(
                f"[Volume Spike] {market} "
                f"volume_ratio={volume_ratio:.2f} "
                f"time={minute_to_str(bar['minute'])}"
            )
            send_volume_spike_alert(
                self.slack_webhook,
                market,
                volume_ratio,
                bar["minute"],
            )
            self.set_cooldown(market)


    def run(self):
        """
        Dry-run main loop using fake ticker events.
        """
        print("AlertWorker dry-run started")

        fake_events = [
            {"market": "KRW-BTC", "timestamp": 1700000000000, "trade_price": 50000000, "trade_volume": 1.2},
            {"market": "KRW-BTC", "timestamp": 1700000060000, "trade_price": 50100000, "trade_volume": 0.8},
            {"market": "KRW-BTC", "timestamp": 1700000120000, "trade_price": 52000000, "trade_volume": 5.0},
            {"market": "KRW-BTC", "timestamp": 1700000180000, "trade_price": 52100000, "trade_volume": 6.0},
            {"market": "KRW-BTC", "timestamp": 1700000240000, "trade_price": 60000000, "trade_volume": 7.0},
            {"market": "KRW-BTC", "timestamp": 1700000300000, "trade_price": 70000000, "trade_volume": 20.0},
        ]

        for event in fake_events:
            bar = self.aggregator.ingest(
                market=event["market"],
                ts_ms=event["timestamp"],
                price=event["trade_price"],
                volume=event["trade_volume"],
            )
            if bar:
                self.detect_and_alert(event["market"], bar)

    def run_gcs_once(self):
        """
        One iteration of GCS-based processing.
        """
        blobs = self.list_latest_jsonl_blobs(limit=1)
        if not blobs:
            print("No JSONL blobs found")
            return

        latest_blob = blobs[0]
        print(f"Processing blob: {latest_blob.name}")

        records = self.read_jsonl_blob(latest_blob.name, max_lines=2000)
        for record in records:
            bar = self.aggregator.ingest(
                market=record["code"],
                ts_ms=record["trade_timestamp"],
                price=record["trade_price"],
                volume=record["trade_volume"],
            )
            if bar:
                self.detect_and_alert(record["code"], bar)




# =========================
# Entrypoint
# =========================
if __name__ == "__main__":
    bucket = os.getenv("GCS_BUCKET")
    prefix = os.getenv("GCS_PREFIX", "upbit-streaming/ticker/")
    slack_webhook = os.getenv("SLACK_WEBHOOK", "")  # empty = dry-run

    if not bucket:
        raise RuntimeError("GCS_BUCKET env var is required")

    worker = AlertWorker(
        bucket=bucket,
        prefix=prefix,
        slack_webhook=slack_webhook,
    )
    worker.run_gcs_loop()