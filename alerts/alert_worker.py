"""
Alert Worker v1

- Reads Upbit ticker JSONL from GCS
- Aggregates ticks into 1-minute bars (close price, volume sum)
- Compares current 1-minute bar vs SMA of last 5 minutes
- Sends observation-only alerts to Slack

v1 goals:
- Minimize false positives
- Simple, explainable logic
- No changes to streaming consumer
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
# Config (v1 fixed)
# =========================
WINDOW_MINUTES = 5
PRICE_THRESHOLD = 0.03      # ±3%
VOLUME_MULTIPLIER = 3.0     # 3x
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
    # TODO: implement slack sender (Copilot OK)
    pass


# =========================
# Alert Worker
# =========================
class AlertWorker:
    def __init__(self, bucket: str, prefix: str, slack_webhook: str):
        # TODO: initialize GCS client and state
        pass

    def in_cooldown(self, market: str) -> bool:
        # TODO: cooldown check
        pass

    def set_cooldown(self, market: str):
        # TODO: set cooldown
        pass

    def detect_and_alert(self, market: str, bar: dict):
        """
        Compare current bar vs SMA and send Slack alert if needed.
        """
        # TODO: detection logic (OR condition)
        pass

    def run(self):
        """
        Main loop:
        - poll GCS
        - read new JSONL objects
        - ingest ticker events
        - detect anomalies
        """
        # TODO: main loop
        pass


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
    worker.run()