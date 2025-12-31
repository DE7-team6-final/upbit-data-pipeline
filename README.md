# 📌 Upbit Real-time & Batch Data Pipeline

Upbit WebSocket 스트리밍과 REST 기반 배치 수집을 통해
실시간 이상변동 알림 + 분석용 데이터 파이프라인을 구축하는 팀 프로젝트입니다.

실시간 처리와 배치 처리를 의도적으로 분리한 구조로,
운영 안정성과 장애 격리, 확장성을 고려한 아키텍처를 목표로 합니다.

---

## 🎬 Demo

[![Demo Video](https://img.youtube.com/vi/Nv62ItVWKBU/0.jpg)](https://youtu.be/Nv62ItVWKBU)

▶️ Click to watch the short demo (16s):  
Real-time streaming alerts and analytics pipeline overview

---

## 🧠 Key Architectural Decisions (ADR)

본 프로젝트는 단순 기능 구현이 아니라,  
**운영 안정성과 설명 가능성**을 중심으로 설계 결정을 내렸습니다.

주요 아키텍처 설계 결정은 다음과 같습니다.

- 실시간 스트리밍 처리와 배치 분석 파이프라인을 의도적으로 분리
- 실시간 알림(v1)은 자동 대응이 아닌 **관측용(Alerting)** 으로 한정
- 배치 기반 일일 요약 알림(v2)을 별도 파이프라인으로 분리
- 애플리케이션 로직이 아닌 **systemd 서비스 상태 기반 운영 알림** 구성
- 제한된 리소스를 고려하여 Kafka 대신 **Redpanda(Kafka-compatible)** 선택
- Streaming Producer와 Consumer를 **물리적으로 분리된 VM**에서 운영

👉 상세한 설계 배경과 트레이드오프는 팀 Notion의  
[**주요 아키텍처 설계 결정 (ADR)**](https://www.notion.so/ADR-2da6e9180a9680e1b9b0f40a60f161bb?source=copy_link) 문서에 정리되어 있습니다.


---
## 📁 Directory Structure
```markdown
upbit-data-pipeline/
├── streaming/          # Upbit WebSocket → Redpanda → GCS
│   ├── producer/       # 실시간 데이터 수집
│   └── consumer/       # GCS 적재용 Consumer (Linux-based managed execution)
│
├── alerts/             # GCS 기반 이상변동 감지 (Alert Worker v1)
│
├── batch/              # Airflow → Snowflake 배치 파이프라인
│   ├── dags/
│   └── scripts/
│
├── docs/               # 설계 문서, 스키마 정의, 운영 기록
├── .github/            # Issue/PR 템플릿, CI 설정
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## 🚀 Current Progress
- 실시간 Producer / Consumer 구현 및 Linux-based(systemd) 운영 안정화
- GCS 기반 실시간 데이터 적재 구조 확립
- Alert Worker v1 구현 및 실데이터 기반 이상변동 감지 검증
- GitHub 협업 규칙 및 프로젝트 문서 정리

## 📌 Next Steps
- Snowflake Silver 스키마 적재 및 검증
- dbt 기반 Gold 테이블 설계
- 배치 파이프라인 범위 정리 및 확장 여부 결정
