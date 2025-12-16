# Alert Worker v1

GCS에 적재된 Upbit 실시간 ticker JSONL 데이터를 읽어
1분 단위로 집계하고,
SMA 기반 규칙으로 이상변동을 감지하는 Alert Worker입니다.

Streaming Consumer와 분리된 독립 컴포넌트로,
운영 안정성을 우선해 설계되었습니다.

---

## 역할

- GCS(JSONL) 기반 실시간 데이터 처리
- 1분 bar aggregation (close price, volume)
- 최근 N분(SMA) 대비 가격/거래량 이상 감지
- Slack 알림 전송
- checkpoint 기반 중복 처리 방지

---

## 실행 방식

### 환경 변수

```bash
export GCS_BUCKET=upbit-streaming
export GCS_PREFIX=ticker/dt=YYYY-MM-DD/
export SLACK_WEBHOOK=...
```

### 수동 실행

```bash
python alert_worker.py
```

### systemd 실행

Alert Worker는 systemd 서비스로 등록되어 있으며,
VM 재부팅 및 서비스 재시작 시 자동 실행됩니다.

```
systemctl status alert-worker.service
journalctl -u alert-worker.service -f
```

## Timestamp 기준

실시간 이상변동 판단은 처리 시점이 아닌
실제 시장 체결 시점을 기준으로 해야 한다고 판단해,
aggregation 기준 timestamp로 ```trade_timestamp```를 사용합니다.