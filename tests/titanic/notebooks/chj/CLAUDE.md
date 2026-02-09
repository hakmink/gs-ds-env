# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Git Commit 규칙

- **Co-Authored-By 사용 금지**: 커밋 메시지에 `Co-Authored-By: Claude` 추가하지 않음

## Project Overview

AWS SageMaker Training + Custom ECR Image를 사용한 Titanic 생존 예측 End-to-End 파이프라인 구축 프로젝트.

**핵심 목표**: Titanic 생존 예측 모델을 직접 만든 Docker 이미지로 ECR에 배포하고, SageMaker Training(`Estimator.fit`)으로 학습한 뒤, 모델 산출물을 S3에 저장.

## Excellence Rubric (합격 기준)

- **End-to-End 재현 가능**: 제3자가 그대로 실행해도 동일하게 ECR 푸시 → SageMaker Training → S3 산출물 저장까지 완주 가능
- **요구사항 100% 충족**: (1) 직접 빌드한 Docker 이미지 (2) ECR 배포 (3) `Estimator.fit()`로 트레이닝 (4) 가능한 최소 인스턴스 사용 근거 (5) S3에 결과 저장
- **SageMaker 컨테이너 규약 준수**: `/opt/ml/input`, `/opt/ml/model`, `/opt/ml/output` 등 표준 경로 기반으로 동작
- **운영 관점 품질**: IAM 최소권한, 로그/에러 트러블슈팅 내역, 실행 파라미터/버전/아티팩트 경로 명확
- **문서/코드 품질**: README + 노트북 + Dockerfile + 학습 스크립트 구성 명료, 커맨드 한 줄로 재현

## AWS Account Info

- **리전**: us-east-1 (N. Virginia)
- **계정 ID**: 155954279556
- **IAM 사용자**: cheon.hj
- **ARN**: `arn:aws:iam::155954279556:user/cheon.hj`
- **S3 버킷**: `cheonhj-mlops-edu-202602`
- **ECR 리포지토리**: `cheonhj-titanic-training`
- **Image URI**: `155954279556.dkr.ecr.us-east-1.amazonaws.com/cheonhj-titanic-training:latest`
- **SageMaker Notebook**: `cheon-hj-t3-50g` (ml.t3.medium)
- **SageMaker Role**: `arn:aws:iam::155954279556:role/service-role/AmazonSageMaker-ExecutionRole-20260123T111313`
- **Training Job**: `cheonhj-titanic-2026-02-02-08-58-02-217`
- **Model Artifacts**: `s3://cheonhj-mlops-edu-202602/output/cheonhj-titanic-2026-02-02-08-58-02-217/output/model.tar.gz`

## Development Environment

- Python 3.12 (`.python-version`)
- 의존성: `pyproject.toml`

## Project Structure

```
.
├── train.py              # 학습 엔트리 스크립트 (LightGBM) — SageMaker 컨테이너에서 실행
├── run_training.py       # SageMaker Estimator.fit() 실행 — 로컬에서 실행
├── split_data.py         # 데이터 80:20 Split — 로컬에서 실행
├── Dockerfile            # 학습 컨테이너 이미지
├── requirements.txt      # 컨테이너 Python 의존성
├── pyproject.toml        # 로컬 Python 의존성
├── data/                 # 원본 Titanic CSV
│   └── output/           # Split 결과 (train.csv, val.csv)
├── README.md             # 프로젝트 문서 + 재현 절차
├── progress.md           # 진행 기록 + 트러블슈팅
├── notion_submission.md  # Notion 제출용 페이지 내용
├── staff_exercise.md     # 과제 요구사항
├── CLAUDE.md             # Claude Code 가이드
└── AGENTS.md             # Agent 가이드
```

## 필수 요구사항 (Must)

### A. 데이터 준비
- Titanic 데이터로 이진분류(생존/사망) 수행
- 학습 데이터는 S3에서 입력받아야 함 (예: `s3://<bucket>/titanic/train.csv`)
- 데이터 소스는 자유 (직접 업로드/공개 데이터 활용 등)
- 제출 문서에 "데이터를 S3에 올리는 절차" 포함 필수

### B. Docker 이미지 제작
- 직접 작성한 Dockerfile로 빌드
- SageMaker Training 규약 준수:
  - 입력: `/opt/ml/input/data/<channel_name>/`
  - 출력: `/opt/ml/model/`
- 학습 스크립트 필수 포함 사항:
  - 데이터 로딩/전처리
  - 모델 학습
  - 평가 (Accuracy/AUC/F1 중 1개 이상)
  - 모델 저장

### C. ECR 배포
- ECR Repository 생성 후 Docker 이미지 push
- 필수 기록 사항:
  - ECR Repo 이름
  - Image URI (`<account>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>`)
  - 빌드/푸시 커맨드

### D. SageMaker Training 실행
- SageMaker Python SDK 사용
- `sagemaker.estimator.Estimator` + `estimator.fit()` 필수
- 채널 기반 입력 권장: `fit({"train": <s3_input>, "validation": <s3_input>})`
- 결과 아티팩트 S3 저장 (SageMaker가 `output_path` 또는 기본 버킷에 `model.tar.gz` 저장)
- 제출 문서에 **S3 결과 경로** 명시 필수

### E. 인스턴스 타입
- 가능한 최소 스펙 사용
- 단순히 "작다"가 아니라 근거 설명 필수:
  - *왜 그 인스턴스가 최소인지* (계정/리전 가용성, 할당량, 정책, 비용 등)
  - *실제 학습이 정상 완료되는지* 검증

## 권장 구현 가이드

### 컨테이너 구조
```
├── train.py              # 학습 엔트리 스크립트
├── requirements.txt      # 필요 라이브러리
└── Dockerfile            # 베이스 이미지 + 의존성 + 엔트리포인트
```

- 엔트리포인트: `python train.py`
- 하이퍼파라미터: argparse로 수신

### 모델 선택
- **LightGBM / XGBoost / Logistic Regression** 중 택1
- 최소 인스턴스에서 빠르게 완료되는 구성 권장

### 학습 산출물
`/opt/ml/model/`에 저장:
- `model.joblib` 또는 `model.pkl`
- `metrics.json` (지표 기록)

## 흔한 실패 포인트

1. **ECR push 권한 부족**: `ecr:*`, `sts:GetCallerIdentity` 권한 필요
2. **경로 오류**: 컨테이너가 `/opt/ml/input/data/...` 경로를 못 읽음
3. **아티팩트 누락**: 로컬에 저장하고 `/opt/ml/model`에 저장하지 않음
4. **컨테이너 즉시 종료**: image_uri는 맞지만 엔트리포인트/커맨드 없음
5. **인스턴스 근거 부족**: "최소"라고 주장했지만 증빙 없음

## 제출물 구조 (Notion 페이지)

1. **Executive Summary** (5줄 내외)
2. **아키텍처 & 플로우**: 데이터(S3) → Training(SageMaker) → 아티팩트(S3)
3. **리소스 정보**: Region, S3 Bucket, ECR Repo, Image URI, Job Name, Output Path
4. **실행 방법**: 복사-붙여넣기 가능한 커맨드
5. **핵심 코드 스니펫**: Dockerfile, Estimator 생성/fit, 모델 저장 로직
6. **트러블슈팅 로그**: 막힌 이슈 1개 이상 + 해결 과정
7. **결과**: 지표 1개 이상

## 평가 체크리스트

### Pass/Fail (필수)
- [ ] Dockerfile로 이미지 직접 빌드
- [ ] ECR로 push 성공 (Image URI 증빙)
- [ ] `Estimator.fit()`로 Training Job 성공 종료
- [ ] 최소 인스턴스 사용 및 근거 제시
- [ ] 모델 아티팩트 S3 저장 경로 명시 + 실제 존재
- [ ] 제3자가 따라할 수 있는 재현 절차 제공

### Plus (가산점)
- [ ] 하이퍼파라미터를 Estimator hyperparameters로 주입
- [ ] `metrics.json` 저장 및 로그 출력 정리
- [ ] IAM 최소권한 정책 초안 제시
- [ ] 비용/시간 최적화 (스팟, 데이터 샘플링 등) 근거 있는 적용
