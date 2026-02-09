# Titanic Survival Prediction — SageMaker Training + Custom ECR Image

## Executive Summary

- Titanic 승객 데이터(891행, 7 피처)로 생존/사망 이진분류 모델을 구축
- LightGBM 기반 학습 스크립트를 `python:3.12-slim` Docker 이미지로 빌드하여 ECR에 배포
- SageMaker `Estimator.fit()`으로 `ml.m5.large` Spot Instance에서 학습 실행 (54초 완료)
- 검증 정확도 78.8%, AUC 0.798 달성, 모델 아티팩트를 S3에 자동 저장
- Spot Instance 적용으로 On-Demand 대비 63% 비용 절감 (Billable 20초, 추정 ~$0.00019)

## 아키텍처

> **용어 안내**: S3 = AWS 파일 저장소, ECR = Docker 이미지 저장소, SageMaker = AWS 머신러닝 학습 서비스

```
data/train.csv ─► split_data.py ─► S3 (train.csv, val.csv)
  [원본 데이터]    [80:20 분할]       [AWS 클라우드에 업로드]
                                        │
Dockerfile ─► docker build ─► ECR push  │
[컨테이너 레시피] [이미지 생성]   [이미지 업로드] │
                                  │     │
                                  ▼     ▼
                           SageMaker Training Job
                           (Estimator.fit)
                           [클라우드에서 모델 학습]
                                  │
                                  ▼
                           S3 (model.tar.gz)
                           [학습 결과 자동 저장]
                           ├── model.joblib         ← 학습된 모델
                           ├── label_encoders.joblib ← 문자→숫자 변환기
                           └── metrics.json          ← 성능 지표
```

## 리소스 정보

| 항목 | 값 | 설명 |
|------|-----|------|
| AWS Region | us-east-1 | N. Virginia 리전 |
| S3 Bucket | cheonhj-mlops-edu-202602 | 데이터/모델 저장 버킷 |
| S3 데이터 경로 | `s3://cheonhj-mlops-edu-202602/titanic/` | 학습 데이터 위치 |
| ECR Repository | cheonhj-titanic-training | Docker 이미지 저장소 |
| Image URI | `155954279556.dkr.ecr.us-east-1.amazonaws.com/cheonhj-titanic-training:latest` | SageMaker가 사용할 이미지 주소 |
| SageMaker Role | `arn:aws:iam::155954279556:role/service-role/AmazonSageMaker-ExecutionRole-20260123T111313` | Training Job이 AWS 자원에 접근할 때 사용하는 역할 |
| Training Instance | ml.m5.large (Spot) | 학습에 사용한 서버 사양 — [선택 근거](#인스턴스-타입-선택-근거) |
| Output S3 Path | `s3://cheonhj-mlops-edu-202602/output/cheonhj-titanic-2026-02-02-08-58-02-217/output/model.tar.gz` | 학습 완료 후 모델 저장 위치 |

## 사전 요구사항

- AWS 계정 (SageMaker, S3, ECR 권한)
- SageMaker Notebook Instance (Docker, AWS CLI, Python SDK 내장)

## 재현 절차

### 0) SageMaker Notebook Instance 생성

SageMaker 콘솔에서 Notebook Instance를 생성합니다 (Docker, AWS CLI, Python SDK 내장).

1. SageMaker 콘솔 → Notebooks → Notebook instances → **Create notebook instance**
2. Instance type: `ml.t3.medium`, IAM role: 기존 SageMaker Execution Role 선택
3. 상태가 **InService**가 되면 **Open JupyterLab** → **Terminal** 열기

### 1) 코드 다운로드 & 데이터 S3 업로드

> **소스 코드 위치**: 프로젝트 전체 코드가 S3에 zip으로 업로드되어 있습니다.
> SageMaker Notebook은 동일 계정의 S3에 바로 접근 가능하므로 별도 인증 없이 다운로드할 수 있습니다.

```bash
# 1-1. S3에서 코드 zip 다운로드 (SageMaker 기본 작업 디렉토리: ~/SageMaker)
cd ~/SageMaker
aws s3 cp s3://cheonhj-mlops-edu-202602/code/staff_titanic.zip .

# 1-2. 압축 해제 후 프로젝트 디렉토리로 이동
unzip staff_titanic.zip -d staff_titanic
cd staff_titanic

# 1-3. 분할된 학습/검증 데이터를 S3에 업로드 (SageMaker Training 입력용)
aws s3 cp data/output/train.csv s3://cheonhj-mlops-edu-202602/titanic/train.csv
aws s3 cp data/output/val.csv s3://cheonhj-mlops-edu-202602/titanic/val.csv

# 1-4. 업로드 확인
aws s3 ls s3://cheonhj-mlops-edu-202602/titanic/
```

> **zip 내부 구조**: `train.py`(학습 스크립트), `run_training.py`(Estimator 실행), `Dockerfile`, `split_data.py`(데이터 분할), `data/`(원본+분할 CSV) 등 재현에 필요한 모든 파일 포함

### 2) Docker 빌드 & ECR Push

```bash
# ECR Repository 생성
aws ecr create-repository --repository-name cheonhj-titanic-training --region us-east-1

# ECR 로그인
aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin 155954279556.dkr.ecr.us-east-1.amazonaws.com

# Docker 이미지 빌드
docker build -t cheonhj-titanic-training .

# 태그 지정 & ECR 푸시
docker tag cheonhj-titanic-training:latest 155954279556.dkr.ecr.us-east-1.amazonaws.com/cheonhj-titanic-training:latest
docker push 155954279556.dkr.ecr.us-east-1.amazonaws.com/cheonhj-titanic-training:latest
```

### 3) SageMaker Training 실행

```bash
pip install sagemaker
python run_training.py
```

### 4) 결과 확인

```bash
# S3에 저장된 모델 파일 확인
aws s3 ls s3://cheonhj-mlops-edu-202602/output/ --recursive

# Training Job 상태 확인
aws sagemaker describe-training-job \
  --training-job-name cheonhj-titanic-2026-02-02-08-58-02-217 \
  --region us-east-1 \
  --query '{Status:TrainingJobStatus,Instance:ResourceConfig.InstanceType,Spot:EnableManagedSpotTraining,BillableSeconds:BillableTimeInSeconds,OutputPath:ModelArtifacts.S3ModelArtifacts}' \
  --output table

# CloudWatch 로그 확인
aws logs get-log-events \
  --log-group-name /aws/sagemaker/TrainingJobs \
  --log-stream-name cheonhj-titanic-2026-02-02-08-58-02-217/algo-1-1738483147 \
  --region us-east-1 \
  --query 'events[*].message' \
  --output text
```

## 핵심 코드 스니펫

### Dockerfile

```dockerfile
FROM python:3.12-slim
WORKDIR /opt/program
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY train.py .
ENTRYPOINT ["python", "train.py"]
```

### Estimator 생성 & fit (`run_training.py`)

```python
from sagemaker.estimator import Estimator

estimator = Estimator(
    image_uri=IMAGE_URI,
    role=ROLE_ARN,
    instance_count=1,
    instance_type="ml.m5.large",
    output_path=f"s3://{BUCKET}/output",
    base_job_name="cheonhj-titanic",
    hyperparameters={
        "n_estimators": "100",
        "max_depth": "5",
        "random_state": "42",
    },
    max_run=3600,
    use_spot_instances=True,
    max_wait=7200,
)

estimator.fit({
    "train": f"s3://{BUCKET}/titanic/train.csv",
    "validation": f"s3://{BUCKET}/titanic/val.csv",
})
```

### 모델 저장 로직 (`train.py`)

```python
# /opt/ml/model/ 에 저장 → SageMaker가 자동으로 S3에 model.tar.gz로 업로드
model_dir = Path(args.model_dir)  # default: /opt/ml/model
model_dir.mkdir(parents=True, exist_ok=True)

joblib.dump(model, model_dir / "model.joblib")
joblib.dump(label_encoders, model_dir / "label_encoders.joblib")
with open(model_dir / "metrics.json", "w") as f:
    json.dump(metrics, f, indent=2)
```

## 트러블슈팅 로그

### 1. Exec format error — Docker 이미지 아키텍처 불일치

- **증상**: SageMaker Training Job이 시작 직후 실패, `exec format error` 로그
- **원인**: Apple Silicon(ARM) Mac에서 `docker build`하면 `linux/arm64` 이미지가 생성되는데, SageMaker는 `linux/amd64`(x86_64) 환경에서 실행
- **해결**: SageMaker Notebook Instance(x86_64) 터미널에서 빌드하여 네이티브 amd64 이미지 생성. 로컬 Mac에서 빌드 시에는 `docker build --platform linux/amd64` 사용

### 2. libgomp.so.1 누락 — LightGBM 런타임 의존성

- **증상**: 컨테이너 시작 후 `ImportError: libgomp.so.1: cannot open shared object file`
- **원인**: `python:3.12-slim` 베이스 이미지에 LightGBM이 필요로 하는 OpenMP 런타임(`libgomp1`)이 미포함
- **해결**: Dockerfile에 `RUN apt-get update && apt-get install -y --no-install-recommends libgomp1` 추가

### 3. unrecognized arguments: train — SageMaker 컨테이너 호출 규약

- **증상**: 컨테이너 실행 시 `error: unrecognized arguments: train` 으로 즉시 종료
- **원인**: SageMaker는 Training 컨테이너를 `python train.py train` 형태로 호출하는데, argparse에서 `train` 위치 인수를 처리하지 않음
- **해결**: `parser.add_argument("command", nargs="?", default="train")` 추가하여 `train` 커맨드 인수를 수용

### 4. Could not assume role — IAM Role ARN 경로 오류

- **증상**: `Estimator.fit()` 호출 시 `ClientError: Could not assume role` 에러
- **원인**: SageMaker 콘솔에서 자동 생성된 역할의 ARN에 `/service-role/` 경로가 포함되어 있었으나, 코드에서 `role/AmazonSageMaker-...`로만 지정
- **해결**: `sagemaker.get_execution_role()`로 정확한 ARN(`role/service-role/AmazonSageMaker-...`) 확인 후 수정

## 프로젝트 구조

```
├── train.py              # 학습 엔트리 스크립트 (LightGBM) — SageMaker 컨테이너에서 실행
├── run_training.py       # SageMaker Estimator.fit() 실행 — 로컬에서 실행
├── split_data.py         # 데이터 80:20 Split — 로컬에서 실행
├── Dockerfile            # 학습 컨테이너 이미지
├── requirements.txt      # 컨테이너 Python 의존성
├── data/                 # 원본 Titanic CSV
│   └── output/           # Split 결과 (train.csv, val.csv)
├── progress.md           # 진행 기록 + 트러블슈팅
├── notion_submission.md  # Notion 제출용 페이지 내용
└── staff_exercise.md     # 과제 요구사항
```

**[progress.md](progress.md)** — AWS 리소스 설정, Service Quota, Training Job 결과, 트러블슈팅 4건, IAM 최소권한 정책 초안, 비용 최적화 근거 등 전체 진행 기록이 담긴 핵심 문서입니다.

## 하이퍼파라미터

> **하이퍼파라미터**란? 모델 학습 전에 사람이 미리 정해주는 설정값입니다. `run_training.py`에서 값을 지정하면 SageMaker가 컨테이너 안의 `train.py`에 자동으로 전달합니다. 코드를 직접 수정할 필요 없이 `run_training.py`의 값만 바꾸면 됩니다.

| 파라미터 | 값 | 설명 |
|---------|-----|------|
| `n_estimators` | 100 | 트리 수 — 많을수록 정교하지만 과적합 위험 |
| `max_depth` | 5 | 트리 최대 깊이 — 깊을수록 복잡한 패턴 학습 |
| `random_state` | 42 | 랜덤 시드 — 같은 값이면 동일한 결과 재현 |

```python
estimator = Estimator(
    ...
    hyperparameters={
        "n_estimators": "100",
        "max_depth": "5",
        "random_state": "42",
    },
)
```

`train.py`의 argparse(명령줄 인수 파서)가 이를 수신하여 `LGBMClassifier`에 적용합니다. 값을 변경하려면 `run_training.py`의 `hyperparameters`만 수정하면 됩니다. 이 설정으로 학습한 결과는 [Training 결과](#training-결과) 참조.

## 인스턴스 타입 선택 근거

> **인스턴스 타입**이란? AWS 클라우드 서버의 사양(CPU, 메모리)을 정해둔 규격입니다. 작은 인스턴스일수록 저렴합니다. **Spot Instance**는 AWS의 남는 자원을 할인 가격(최대 90% 할인)으로 사용하는 방식입니다.

**ml.m5.large** (2 vCPU, 8GB RAM, Spot Instance)

- Titanic 데이터(891행, 7 피처)는 1k 미만 tabular 데이터로 GPU/Compute-optimized 인스턴스 불필요
- LightGBM은 경량 트리 모델로 대용량 메모리나 다수 코어를 요구하지 않음
- ml.m5.large(2 vCPU, 8 GiB)는 m5 범용 계열의 최소 사이즈이며, 실제 학습 54초 완료로 적합성 검증됨
- Spot Instance 적용으로 On-Demand(정가) 대비 최대 70~90% 비용 절감
- 상세 분석은 [progress.md 섹션 10 — 비용 최적화](progress.md#10-비용-최적화) 참조

## Training 결과

| 지표 | 값 | 의미 |
|------|-----|------|
| train_accuracy | 0.9171 | 학습 데이터 정확도 (91.7%) |
| val_accuracy | 0.7877 | 검증 데이터 정확도 (78.8%) — 실제 성능 지표 |
| val_f1 | 0.7031 | F1 점수 (정밀도와 재현율의 조화 평균) |
| val_auc | 0.7980 | AUC (분류 모델의 종합 성능, 1.0이 최고) |

- **Training Job**: `cheonhj-titanic-2026-02-02-08-58-02-217`
- **Training seconds**: 54 (실제 학습 소요 시간)
- **Billable seconds**: 20 (Spot 63% 절감)

## 평가 체크리스트 진단 결과

### Pass/Fail (필수) — 6/6

| # | 요구사항 | 상태 | 증빙 |
|---|---------|------|------|
| 1 | Dockerfile로 이미지 직접 빌드 | PASS | `Dockerfile` |
| 2 | ECR로 push 성공 (Image URI 증빙) | PASS | `155954279556.dkr.ecr.us-east-1.amazonaws.com/cheonhj-titanic-training:latest` |
| 3 | `Estimator.fit()`로 Training Job 성공 종료 | PASS | `run_training.py` — `sagemaker.estimator.Estimator` + `estimator.fit()` |
| 4 | 최소 인스턴스 사용 및 근거 제시 | PASS | ml.m5.large — [인스턴스 선택 근거](#인스턴스-타입-선택-근거) |
| 5 | 모델 아티팩트 S3 저장 경로 명시 + 실제 존재 | PASS | `s3://cheonhj-mlops-edu-202602/output/cheonhj-titanic-2026-02-02-08-58-02-217/output/model.tar.gz` |
| 6 | 제3자가 따라할 수 있는 재현 절차 제공 | PASS | README 재현 절차 5단계 복사-붙여넣기 커맨드 |

### Plus (가산점) — 4/4

| # | 항목 | 상태 | 증빙 |
|---|------|------|------|
| 1 | 하이퍼파라미터를 Estimator hyperparameters로 주입 | PASS | `run_training.py` hyperparameters 딕셔너리 → argparse 수신 |
| 2 | `metrics.json` 저장 및 로그 출력 정리 | PASS | `train.py` — `/opt/ml/model/metrics.json` + CloudWatch 로그 |
| 3 | IAM 최소권한 정책 초안 제시 | PASS | [progress.md](progress.md) 섹션 9 — CLI 사용자 + SageMaker 역할 JSON |
| 4 | 비용/시간 최적화 근거 있는 적용 | PASS | Spot Instance 63% 절감, [progress.md](progress.md) 섹션 10 |

### Excellence Rubric — 5/5

| 기준 | 상태 | 비고 |
|------|------|------|
| End-to-End 재현 가능 | OK | README 5단계 커맨드로 완주 가능 |
| 요구사항 100% 충족 | OK | 6개 필수 항목 모두 충족 |
| SageMaker 컨테이너 규약 준수 | OK | `/opt/ml/input/data/`, `/opt/ml/model/` 사용 |
| 운영 관점 품질 | OK | IAM 정책 초안, 트러블슈팅 4건, 아티팩트 경로 명확 |
| 문서/코드 품질 | OK | README + Dockerfile + train.py + run_training.py 구성 명료 |
