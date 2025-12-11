📌 Upbit Real-time & Batch Data Pipeline

Upbit WebSocket 스트리밍 + REST 배치 수집을 기반으로
실시간 이상변동 알림 시스템을 구축하는 팀 프로젝트입니다.

📁 Directory Structure
```markdown
upbit-data-pipeline/
├── streaming/         # 실시간 WebSocket → Redpanda → (추후 GCS)
│   ├── producer/
│   └── consumer/
│
├── batch/             # Airflow → S3 → DBT
│   ├── dags/
│   └── scripts/
│
├── docs/              # 프로젝트 문서, 규칙, 스키마 정의
├── infra/             # AWS/GCP 인프라 (추가 예정)
├── samples/           # 샘플 JSONL 데이터 (추가 예정)
└── .github/           # Issue/PR 템플릿 & CI
```

🚀 Current Progress

Producer 안정 버전 구현 완료

프로젝트 컨벤션 및 문서 구조 세팅

GitHub 협업 환경 구축 (Issue/PR 템플릿, CI)

📌 Next Steps

Consumer 구현

GCS 저장 구조 확립

Batch DAG 개발