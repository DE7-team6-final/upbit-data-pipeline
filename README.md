# 📌 Upbit Real-time & Batch Data Pipeline

Upbit WebSocket 스트리밍과 REST 기반 배치 수집을 통해
실시간 이상변동 알림 + 분석용 데이터 파이프라인을 구축하는 팀 프로젝트입니다.

실시간 처리와 배치 처리를 의도적으로 분리한 구조로,
운영 안정성과 장애 격리, 확장성을 고려한 아키텍처를 목표로 합니다.


> This is a team-built data engineering project that implements a real-time and batch data pipeline using Upbit WebSocket streaming and REST-based batch ingestion.
>  
> The architecture intentionally separates streaming and batch processing to improve operational stability, fault isolation, and scalability, while supporting real-time anomaly alerts and analytics-ready data.

---

## 🎬 Demo

[![Demo Video](https://img.youtube.com/vi/Nv62ItVWKBU/0.jpg)](https://youtu.be/Nv62ItVWKBU)

▶️ Click to watch the short demo (16s):  
Real-time streaming alerts and analytics pipeline overview

---

## 🧠 Key Architectural Decisions (ADR)

본 프로젝트는 단순 기능 구현이 아니라,  
**운영 안정성과 설명 가능성**을 중심으로 설계 결정을 내렸습니다.

주요 설계 결정 요약:

- 실시간 스트리밍 처리와 배치 분석 파이프라인을 의도적으로 분리
- 실시간 알림(v1)은 자동 대응이 아닌 **관측용(Alerting)** 으로 한정
- 배치 기반 일일 요약 알림(v2)을 별도 파이프라인으로 분리
- 애플리케이션 로직이 아닌 **systemd 서비스 상태 기반 운영 알림** 구성
- 제한된 리소스를 고려하여 Kafka 대신 **Redpanda(Kafka-compatible)** 선택
- Streaming Producer와 Consumer를 **물리적으로 분리된 VM**에서 운영

👉 상세한 설계 배경과 트레이드오프는 팀 Notion의  
[**주요 아키텍처 설계 결정 (ADR)**](https://www.notion.so/ADR-2da6e9180a9680e1b9b0f40a60f161bb?source=copy_link) 문서에 정리되어 있습니다.

---

## 🛠 Operational Considerations
- Streaming과 Batch 파이프라인은 독립적으로 동작
- 실시간 Alert Worker(v1)는 systemd 기반으로 운영
- 서비스 중단 시 Slack 장애 알림 전송
- 배치 요약 알림(v2)은 단일 실패 시에도 fallback 허용

## 🚨 Failure Scenarios (Summary)
- Producer 중단 → Consumer 및 Batch 영향 없음
- Consumer 중단 → Slack 알림 발생
- Gold DAG 실패 → 전날 데이터 기반 요약 알림 유지

---
## 📁 Directory Structure
```markdown
upbit-data-pipeline/
├── streaming/              # Real-time data ingestion
│   ├── producer/           # Upbit WebSocket producer
│   └── consumer/           # Streaming consumer → GCS (Linux-based service management)
│
├── alerts/                 # Monitoring & alert workers
│                           # 실시간 이상 변동 알림 / 일일 변동성 리포트
│
├── airflow/                # Batch orchestration (Airflow)
│   ├── dags/               # Medallion architecture DAGs
│   │   ├── raw/            # Raw data ingestion DAGs
│   │   ├── silver/         # Data cleaning & transformation DAGs
│   │   └── gold/           # Analytics & metrics DAGs
│   └── plugins/            # Custom Airflow plugins (Slack, batch utils)
│
├── dbt/                    # Transform layer (ELT)
│   ├── models/
│   │   ├── silver/         # Cleaned intermediate models
│   │   └── gold/           # Analytics-ready models
│   └── macros/             # Shared dbt macros
│
├── docs/                   # Design docs & decisions
│   ├── adr/                # Architectural Decision Records
│   └── architecture/       # Pipeline & system architecture
│
├── infra/                  # Cloud context
│   ├── aws/                # AWS usage notes
│   └── gcp/                # GCP usage notes
│
├── Dockerfile
├── Dockerfile_Worker
├── pyproject.toml
├── requirements.txt
└── README.md

```

