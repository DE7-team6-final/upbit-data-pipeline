# 📌 Upbit Real-time & Batch Data Pipeline

Upbit WebSocket 스트리밍과 REST 기반 배치 수집을 통해
실시간 이상변동 알림 + 분석용 데이터 파이프라인을 구축하는 팀 프로젝트입니다.

실시간 처리와 배치 처리를 의도적으로 분리한 구조로,
운영 안정성과 장애 격리, 확장성을 고려한 아키텍처를 목표로 합니다.

English summary: Real-time + batch data pipeline with operational alerts and analytics-ready outputs.

---

## 🏗️ Architecture

![Upbit Real-time & Batch Data Pipeline Architecture](docs/architecture/overview.png)

본 프로젝트는 Streaming과 Batch 파이프라인을 명확히 분리하여
장애 격리, 운영 안정성, 확장성을 중심으로 설계되었습니다.

Streaming 데이터는 Kafka-compatible 플랫폼(Redpanda)을 통해 버퍼링된 후
Object Storage에 Raw 데이터로 적재됩니다.

Batch 파이프라인은 Airflow 기반으로 RAW → SILVER → GOLD 단계를 순차적으로 처리하며,
분석 및 알림에 필요한 데이터셋을 생성합니다.

---

## 🎬 Demo

[![Demo Video](https://img.youtube.com/vi/Nv62ItVWKBU/0.jpg)](https://youtu.be/Nv62ItVWKBU)

▶️ Click to watch the short demo (16s):  
Real-time streaming alerts and analytics pipeline overview

---

## 🧠 Key Design Decisions

본 프로젝트는 단순 기능 구현이 아니라,  
**운영 안정성과 설명 가능성**을 중심으로 설계 결정을 내렸습니다.

주요 설계 결정 요약:

- 교육용 인프라 제약(야간 인스턴스 중단)을 고려한 아키텍처 설계
- 실시간 스트리밍 처리와 배치 분석 파이프라인을 의도적으로 분리
- 실시간 알림(v1)은 자동 대응이 아닌 **관측용(Alerting)** 으로 한정
- 배치 기반 일일 요약 알림(v2)을 별도 파이프라인으로 분리
- 애플리케이션 로직이 아닌 **systemd 서비스 상태 기반 운영 알림** 구성
- 제한된 리소스를 고려하여 Kafka 대신 **Redpanda(Kafka-compatible)** 선택
- Streaming Producer와 Consumer를 **물리적으로 분리된 VM**에서 운영

👉 설계 배경과 주요 트레이드오프에 대한 상세 내용은  
팀 Notion 문서에 참고 자료로 정리되어 있습니다.

---

## ⚠️ 운영 제약 및 트레이드오프

본 프로젝트는 실제 서비스 환경이 아닌 교육용 인프라 제약 하에서 진행되었습니다.
해당 제약은 아키텍처 설계에 직접적인 영향을 주었습니다.

- 프로젝트 정책에 따라 AWS EC2 인스턴스가 야간 시간대(약 21시–09시)에 3~4회 자동 중단
- 실시간 스트리밍 파이프라인은 이상 변동 감지를 위해 가능한 한 지속적인 실행이 필요
- 사용 가능한 컴퓨트 리소스는 EC2 medium 급으로 제한

위와 같은 제약으로 인해, 스트리밍 파이프라인의 연속성을 유지하기 위해
상대적으로 리소스 사용량이 적고 상시 실행이 필요한 구성 요소 일부를
GCP Free Trial 환경으로 분리하여 운영하였습니다.

이러한 멀티 클라우드 구성은 장기적인 운영 아키텍처를 지향한 것이 아니라,
제한된 리소스 환경에서 안정적인 파이프라인 운영을 위한 현실적인 선택이었습니다.

실제 운영 환경에서는 단일 클라우드 환경으로 통합하여
운영 복잡도와 비용 관리 측면을 단순화하는 방향이 더 적합하다고 판단합니다.

---

## 🛠 Operational & Reliability
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

