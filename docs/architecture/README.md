# Architecture Overview

본 문서는 Upbit Real-time & Batch Data Pipeline의 전체 아키텍처를 설명합니다.

본 시스템은 실시간 데이터 처리와 배치 분석 파이프라인을 명확히 분리하여,
운영 안정성과 장애 격리, 확장성을 확보하는 것을 목표로 설계되었습니다.

## Design Principles
- Streaming과 Batch 파이프라인의 책임 분리
- 장애 발생 시 영향 범위 최소화
- 운영 관점에서의 가시성과 복구 용이성
- 분석용 데이터셋의 재현성 확보

## Diagram
아래 다이어그램은 전체 데이터 흐름과 주요 컴포넌트 간의 관계를 나타냅니다.
