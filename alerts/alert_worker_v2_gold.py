"""
Daily Volatility Report (Gold-based)

- Source: Snowflake GOLD.GOLD_CANDLE_WINDOW_METRICS
- Metric: Percentile-ranked relative volatility (internal Z-score)
- Mode: Daily batch (read-only)
- Purpose: Human-readable anomaly summary via Slack
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
INTERVAL = "1m"
MAX_ALERTS = 4    # Only 4 markets are tracked; cap alerts at one per coin

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


QUERY = f"""
WITH base AS (
    SELECT
        CODE,
        CANDLE_INTERVAL,
        CLOSE_PRICE,
        ZSCORE,
        TRADE_DATE,
        ABS(ZSCORE) AS abs_z
    FROM UPBIT_DB.GOLD.GOLD_CANDLE_WINDOW_METRICS
    WHERE CANDLE_INTERVAL = '{INTERVAL}'
      AND TRADE_DATE = CURRENT_DATE() - 1
),

ranked AS (
    SELECT
        CODE,
        CLOSE_PRICE,
        ZSCORE,
        TRADE_DATE,
        PERCENT_RANK() OVER (
            PARTITION BY CODE, TRADE_DATE
            ORDER BY abs_z
        ) AS percentile,
        ROW_NUMBER() OVER (
            PARTITION BY CODE, TRADE_DATE
            ORDER BY abs_z DESC
        ) AS rn
    FROM base
)

SELECT
    CODE,
    CLOSE_PRICE,
    percentile
FROM ranked
WHERE rn = 1
ORDER BY percentile DESC
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
        print("[Daily Volatility Report] SLACK_WEBHOOK_URL not set. Printing instead.")
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
        "📊 *Daily Volatility Report*",
        "",
        f"오늘 가장 이례적인 1분 가격 변동 (코인별 1건): *{total_count}개*",
        "",
    ]

    for row in rows:
        percentile = round(row["PERCENTILE"] * 100, 1)
        lines.append(
            f"• `{row['CODE']}` | Price: {row['CLOSE_PRICE']} | "
            f"Unusual movement (*{percentile} percentile today*)"
        )

    lines.append("")
    lines.append("기준: 당일 1분 캔들 기준 상대적 변동성")

    return "\n".join(lines)



# =========================
# Main
# =========================
def main():
    rows = fetch_anomalies()

    if not rows:
        print("[Daily Volatility Report] No anomalies detected. Exiting.")
        return

    message = format_message(rows)
    send_slack_message(message)

    print(f"[Daily Volatility Report] Sent {len(rows)} alerts to Slack.")


if __name__ == "__main__":
    main()
