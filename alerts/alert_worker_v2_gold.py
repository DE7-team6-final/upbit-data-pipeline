"""
Alert Worker v2 (Gold-based anomaly detection)

- Source: Snowflake GOLD.GOLD_CANDLE_WINDOW_METRICS
- Logic: Z-score threshold + ranking
- Mode: Batch, read-only
- Purpose: Observation-oriented Slack alert
"""

import os
import snowflake.connector
from typing import List, Dict
import requests
from dotenv import load_dotenv
from pathlib import Path

# Explicitly load .env from the project root
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

# =========================
# Config
# =========================
Z_THRESHOLD = 1.75
INTERVAL = "1m"
LOOKBACK_DAYS = 1
MAX_ALERTS = 10

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


QUERY = f"""
SELECT
    CODE,
    CANDLE_INTERVAL,
    CLOSE_PRICE,
    ZSCORE
FROM UPBIT_DB.GOLD.GOLD_CANDLE_WINDOW_METRICS
WHERE ABS(ZSCORE) >= {Z_THRESHOLD}
  AND CANDLE_INTERVAL = '{INTERVAL}'
  AND TRADE_DATE >= CURRENT_DATE() - {LOOKBACK_DAYS}
ORDER BY ABS(ZSCORE) DESC
LIMIT {MAX_ALERTS};
"""


# =========================
# Snowflake
# =========================
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="UPBIT_DB",
        schema="GOLD",
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def fetch_anomalies() -> List[Dict]:
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor(snowflake.connector.DictCursor)
        cur.execute(QUERY)
        return cur.fetchall()
    finally:
        conn.close()


# =========================
# Slack
# =========================
def send_slack_message(message: str):
    if not SLACK_WEBHOOK_URL:
        print("[Alert v2] SLACK_WEBHOOK_URL not set. Printing instead.")
        print(message)
        return

    payload = {"text": message}
    response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)

    if response.status_code != 200:
        raise RuntimeError(
            f"Slack webhook failed: {response.status_code}, {response.text}"
        )


def format_message(rows: List[Dict]) -> str:
    total_count = len(rows)

    lines = [
        "📊 *Upbit Gold Anomaly Report (v2)*",
        "",
        f"오늘 Z-score 기준 초과 코인: *{total_count}개*",
        f"상위 {min(MAX_ALERTS, total_count)}개만 표시 (관측용 리포트)",
        "",
    ]

    for row in rows:
        lines.append(
            f"• `{row['CODE']}` | Price: {row['CLOSE_PRICE']} | "
            f"Z: *{row['ZSCORE']:.2f}*"
        )

    return "\n".join(lines)



# =========================
# Main
# =========================
def main():
    rows = fetch_anomalies()

    if not rows:
        print("[Alert v2] No anomalies detected. Exiting.")
        return

    message = format_message(rows)
    send_slack_message(message)

    print(f"[Alert v2] Sent {len(rows)} alerts to Slack.")


if __name__ == "__main__":
    main()
