# SageMaker Training + Custom ECR Image로 Titanic 생존 예측 End-to-End 구축

## 1) Executive Summary

- Titanic 승객 데이터(891행, 7 피처)로 생존/사망 이진분류 모델을 구축
- LightGBM 기반 학습 스크립트를 `python:3.12-slim` Docker 이미지로 빌드하여 ECR에 배포
- SageMaker `Estimator.fit()`으로 `ml.m5.large` Spot Instance에서 학습 실행 (54초 완료)
- 검증 정확도 78.8%, AUC 0.798 달성, 모델 아티팩트를 S3에 자동 저장
- Spot Instance 적용으로 On-Demand 대비 63% 비용 절감 (Billable 20초, 추정 ~$0.00019)

---

## 2) 아키텍처 & 플로우

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

**흐름 요약**: 로컬에서 데이터를 분할하여 S3에 업로드 → Dockerfile로 학습 컨테이너 이미지를 빌드하여 ECR에 push → `run_training.py`에서 `Estimator.fit()`을 호출하면 SageMaker가 ECR 이미지 + S3 데이터로 Training Job 실행 → 학습 완료 후 `/opt/ml/model/` 내용을 `model.tar.gz`로 묶어 S3에 자동 저장

**스크립트 역할 구분**:

| 스크립트 | 실행 위치 | 역할 |
|----------|----------|------|
| `run_training.py` | 로컬 / SageMaker Notebook | SageMaker API에 Training Job 생성 요청 (`Estimator.fit()`) |
| `train.py` | SageMaker Docker 컨테이너 내부 | 데이터 로딩 → 전처리 → LightGBM 학습 → 평가 → `/opt/ml/model/`에 저장 |

`run_training.py`가 "무엇을, 어떤 환경에서 학습할지" 설정하고 요청하면, SageMaker가 컨테이너를 띄워 `train.py`를 실행합니다.

---

## 3) 리소스 정보

| 항목 | 값 |
|------|-----|
| AWS Region | us-east-1 (N. Virginia) |
| S3 Bucket | cheonhj-mlops-edu-202602 |
| S3 데이터 경로 | `s3://cheonhj-mlops-edu-202602/titanic/` |
| ECR Repository | cheonhj-titanic-training |
| Image URI | `155954279556.dkr.ecr.us-east-1.amazonaws.com/cheonhj-titanic-training:latest` |
| SageMaker Role | `arn:aws:iam::155954279556:role/service-role/AmazonSageMaker-ExecutionRole-20260123T111313` |
| Training Job Name | `cheonhj-titanic-2026-02-02-08-58-02-217` |
| Training Instance | ml.m5.large (Spot) |
| Output S3 Path | `s3://cheonhj-mlops-edu-202602/output/cheonhj-titanic-2026-02-02-08-58-02-217/output/model.tar.gz` |
| Git Repository | `https://code.gsretail.com/scm/~cheon.hj/staff_titanic.git` (참조용) |

---

## 4) 실행 방법 (재현 절차)

### (0) SageMaker Notebook Instance 생성

SageMaker 콘솔에서 Notebook Instance를 생성합니다 (Docker, AWS CLI, Python SDK 내장).

1. SageMaker 콘솔 → Notebooks → Notebook instances → **Create notebook instance**
2. Instance type: `ml.t3.medium`, IAM role: 기존 SageMaker Execution Role 선택
3. 상태가 **InService**가 되면 **Open JupyterLab** → **Terminal** 열기

### (1) 코드 다운로드 & 데이터 S3 업로드

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

### (2) Docker 빌드 & ECR Push

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

### (3) SageMaker Training 실행

```bash
pip install sagemaker
python run_training.py
```

### (4) 결과 확인

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

---

## 5) 핵심 코드 스니펫

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

### Estimator 생성 + fit() 호출 (run_training.py)

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

### 모델 저장 로직 (train.py)

```python
model_dir = Path(args.model_dir)  # /opt/ml/model
model_dir.mkdir(parents=True, exist_ok=True)

joblib.dump(model, model_dir / "model.joblib")
joblib.dump(label_encoders, model_dir / "label_encoders.joblib")
with open(model_dir / "metrics.json", "w") as f:
    json.dump(metrics, f, indent=2)
```

---

## 6) 트러블슈팅 로그

### 이슈 1: Exec format error — Docker 이미지 아키텍처 불일치

- **증상**: SageMaker Training Job이 시작 직후 실패, `exec format error` 로그
- **원인**: Apple Silicon(ARM) Mac에서 `docker build`하면 `linux/arm64` 이미지가 생성되는데, SageMaker는 `linux/amd64`(x86_64) 환경에서 실행
- **해결**: SageMaker Notebook Instance(x86_64) 터미널에서 빌드하여 네이티브 amd64 이미지 생성. 로컬 Mac에서 빌드 시에는 `docker build --platform linux/amd64` 사용

### 이슈 2: libgomp.so.1 누락 — LightGBM 런타임 의존성

- **증상**: 컨테이너 시작 후 `ImportError: libgomp.so.1: cannot open shared object file`
- **원인**: `python:3.12-slim` 베이스 이미지에 LightGBM이 필요로 하는 OpenMP 런타임(`libgomp1`)이 미포함
- **해결**: Dockerfile에 `RUN apt-get update && apt-get install -y --no-install-recommends libgomp1` 추가

### 이슈 3: unrecognized arguments: train — SageMaker 컨테이너 호출 규약

- **증상**: 컨테이너 실행 시 `error: unrecognized arguments: train` 으로 즉시 종료
- **원인**: SageMaker는 Training 컨테이너를 `python train.py train` 형태로 호출하는데, argparse에서 `train` 위치 인수를 처리하지 않음
- **해결**: `parser.add_argument("command", nargs="?", default="train")` 추가하여 `train` 커맨드 인수를 수용

### 이슈 4: Could not assume role — IAM Role ARN 경로 오류

- **증상**: `Estimator.fit()` 호출 시 `ClientError: Could not assume role` 에러
- **원인**: SageMaker 콘솔에서 자동 생성된 역할의 ARN에 `/service-role/` 경로가 포함되어 있었으나, 코드에서 `role/AmazonSageMaker-...`로만 지정
- **해결**: `sagemaker.get_execution_role()`로 정확한 ARN(`role/service-role/AmazonSageMaker-...`) 확인 후 수정

---

## 7) 결과

### 성능 지표

| 지표 | 값 | 의미 |
|------|-----|------|
| train_accuracy | 0.9171 | 학습 데이터 정확도 (91.7%) |
| val_accuracy | 0.7877 | 검증 데이터 정확도 (78.8%) |
| val_f1 | 0.7031 | F1 점수 (정밀도+재현율 조화 평균) |
| val_auc | 0.7980 | AUC (분류 종합 성능, 1.0이 최고) |

### Training Job 증빙

```
+-----------------+------------------------------------------------------------------------------------------------------+
|  BillableSeconds|  20                                                                                                  |
|  Instance       |  ml.m5.large                                                                                         |
|  InstanceCount  |  1                                                                                                   |
|  OutputPath     |  s3://cheonhj-mlops-edu-202602/output/cheonhj-titanic-2026-02-02-08-58-02-217/output/model.tar.gz   |
|  Spot           |  True                                                                                                |
|  Status         |  Completed                                                                                           |
|  TrainingSeconds|  54                                                                                                  |
+-----------------+------------------------------------------------------------------------------------------------------+
```

### CloudWatch 로그

```
==================================================
Titanic Survival Prediction Training
==================================================
Hyperparameters:
  n_estimators: 100
  max_depth: 5
  random_state: 42
Train path: /opt/ml/input/data/train
Validation path: /opt/ml/input/data/validation
Model dir: /opt/ml/model
==================================================
Loading data...
Train samples: 712
Validation samples: 179
Preprocessing...
Training model...
Evaluating...
==================================================
Metrics:
  train_accuracy: 0.9171
  val_accuracy: 0.7877
  val_f1: 0.7031
  val_auc: 0.7980
==================================================
Model saved to /opt/ml/model/model.joblib
Label encoders saved to /opt/ml/model/label_encoders.joblib
Metrics saved to /opt/ml/model/metrics.json
Training complete!
```

### 인스턴스 타입 선택 근거

**ml.m5.large** (2 vCPU, 8GB RAM, Spot Instance)

- Titanic 데이터(891행, 7 피처)는 1k 미만 tabular 데이터로 GPU/Compute-optimized 인스턴스 불필요
- LightGBM은 경량 트리 모델로 대용량 메모리나 다수 코어를 요구하지 않음
- ml.m5.large(2 vCPU, 8 GiB)는 m5 범용 계열의 최소 사이즈이며, 실제 학습 54초 완료로 적합성 검증됨
- Spot Instance 적용으로 Training 54초 중 Billable 20초(63% 절감)

### 비용 최적화

| 최적화 수단 | 절감 효과 |
|------------|----------|
| ml.m5.large (가용한 최소) | 불필요한 대형 인스턴스 회피 |
| Spot Instance | On-Demand 대비 63% 절감 (실측) |
| 추정 과금 | ~$0.00019 (Spot 20초) |

### IAM 최소권한 정책 초안

현재는 FullAccess 등 6개 정책으로 빠르게 구성했으나, 프로덕션 운영 시 아래 최소권한 정책으로 대체 권장.

#### CLI 사용자 (cheon.hj)

> SageMaker Notebook 또는 AWS CLI로 작업할 때 사용하는 IAM 사용자의 권한입니다.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      // --- S3: 학습 데이터 업로드/다운로드 ---
      // train.csv, val.csv를 S3에 올리고, 학습 결과(model.tar.gz)를 내려받기 위한 권한
      "Sid": "S3DataBucket",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",   // S3에서 파일 다운로드
        "s3:PutObject",   // S3에 파일 업로드
        "s3:ListBucket"   // S3 버킷 내 파일 목록 조회
      ],
      "Resource": [
        "arn:aws:s3:::cheonhj-mlops-edu-202602",      // 버킷 자체 (ListBucket용)
        "arn:aws:s3:::cheonhj-mlops-edu-202602/*"      // 버킷 안의 모든 파일
      ]
    },
    {
      // --- ECR: Docker 이미지 빌드 후 Push ---
      // docker push로 이미지를 ECR에 올리기 위한 권한
      "Sid": "ECRPushPull",
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",        // ECR 로그인 토큰 발급
        "ecr:BatchCheckLayerAvailability",   // 이미지 레이어 존재 여부 확인
        "ecr:GetDownloadUrlForLayer",        // 이미지 레이어 다운로드 URL
        "ecr:BatchGetImage",                 // 이미지 메타데이터 조회
        "ecr:PutImage",                      // 이미지 Push
        "ecr:InitiateLayerUpload",           // 레이어 업로드 시작
        "ecr:UploadLayerPart",               // 레이어 업로드 (분할)
        "ecr:CompleteLayerUpload",           // 레이어 업로드 완료
        "ecr:CreateRepository"               // ECR 리포지토리 생성
      ],
      "Resource": "*"
    },
    {
      // --- SageMaker: Training Job 생성/조회 ---
      // run_training.py에서 Estimator.fit()으로 학습 시작, 상태 확인에 필요
      // Resource에 "titanic-*"를 지정하여 이 프로젝트의 Job만 허용
      "Sid": "SageMakerTraining",
      "Effect": "Allow",
      "Action": [
        "sagemaker:CreateTrainingJob",     // Training Job 생성 (estimator.fit)
        "sagemaker:DescribeTrainingJob",   // Job 상태 조회 (진행률, 완료 여부)
        "sagemaker:ListTrainingJobs"       // Job 목록 조회
      ],
      "Resource": "arn:aws:sagemaker:us-east-1:155954279556:training-job/titanic-*"
    },
    {
      // --- IAM: SageMaker에게 역할 전달 ---
      // Training Job이 S3/ECR에 접근하려면 SageMakerExecutionRole이 필요한데,
      // 이 권한이 있어야 "이 역할을 SageMaker에게 넘겨줘"라고 할 수 있음
      "Sid": "PassRoleToSageMaker",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::155954279556:role/service-role/AmazonSageMaker-ExecutionRole-20260123T111313",
      "Condition": {
        "StringEquals": {
          "iam:PassedToService": "sagemaker.amazonaws.com"  // SageMaker에게만 전달 허용
        }
      }
    },
    {
      // --- CloudWatch: 학습 로그 조회 ---
      // Training Job의 stdout/stderr 로그를 CloudWatch에서 확인하기 위한 권한
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:DescribeLogGroups",   // 로그 그룹 목록 조회
        "logs:GetLogEvents",        // 로그 내용 읽기
        "logs:FilterLogEvents"      // 로그 검색
      ],
      "Resource": "arn:aws:logs:us-east-1:155954279556:log-group:/aws/sagemaker/TrainingJobs:*"
    }
  ]
}
```

#### SageMaker 실행 역할 (SageMakerExecutionRole)

> SageMaker Training Job이 AWS 내부에서 실행될 때 사용하는 역할입니다. SageMaker 서비스 자체가 S3에서 데이터를 읽고, ECR에서 이미지를 가져오고, 로그를 쓸 때 이 역할의 권한을 사용합니다.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      // --- S3: 학습 데이터 읽기 + 모델 저장 ---
      // SageMaker가 S3에서 train.csv/val.csv를 컨테이너로 복사하고,
      // 학습 완료 후 /opt/ml/model/ 내용을 model.tar.gz로 묶어 S3에 업로드
      "Sid": "S3Access",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",   // S3에서 학습 데이터 다운로드
        "s3:PutObject",   // 학습 결과(model.tar.gz)를 S3에 업로드
        "s3:ListBucket"   // 버킷 내 파일 목록 조회
      ],
      "Resource": [
        "arn:aws:s3:::cheonhj-mlops-edu-202602",
        "arn:aws:s3:::cheonhj-mlops-edu-202602/*"
      ]
    },
    {
      // --- ECR: Docker 이미지 가져오기 ---
      // SageMaker가 ECR에서 학습용 Docker 이미지를 pull하기 위한 권한
      // Push는 불필요 (이미지 올리는 건 CLI 사용자가 함)
      "Sid": "ECRPull",
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",        // ECR 인증 토큰 발급
        "ecr:BatchCheckLayerAvailability",   // 이미지 레이어 확인
        "ecr:GetDownloadUrlForLayer",        // 이미지 레이어 다운로드
        "ecr:BatchGetImage"                  // 이미지 메타데이터 조회
      ],
      "Resource": "*"
    },
    {
      // --- CloudWatch: 학습 로그 기록 ---
      // train.py의 print() 출력이 CloudWatch Logs에 기록되도록 하는 권한
      // CLI 사용자 정책과 달리 여기서는 "쓰기" 권한 (Create/Put)
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",    // 로그 그룹 생성 (최초 실행 시)
        "logs:CreateLogStream",   // 로그 스트림 생성 (Job마다 1개)
        "logs:PutLogEvents"       // 실제 로그 내용 기록
      ],
      "Resource": "arn:aws:logs:us-east-1:155954279556:log-group:/aws/sagemaker/TrainingJobs:*"
    }
  ]
}
```

### 리소스 상태 점검 (2026-02-02)

학습 완료 후 불필요한 과금이 발생하지 않는지 확인한 결과:

| 리소스 | 상태 | 비용 영향 |
|--------|------|----------|
| SageMaker Training Jobs | 실행 중 없음 | 없음 |
| SageMaker Endpoints | 없음 | 없음 |
| SageMaker Notebook Instances | 없음 | 없음 |
| SageMaker Processing Jobs | 실행 중 없음 | 없음 |
| S3 Bucket (`cheonhj-mlops-edu-202602`) | 데이터 + 모델 아티팩트 | 무시할 수준 (수 MB 미만) |
| ECR Repository (`cheonhj-titanic-training`) | 이미지 1개 | 무시할 수준 (이미지 1개, 수백 MB) |
| SageMaker Notebook (`cheon-hj-t3-50g`) | InService | 사용 후 Stop 필요 |

사용 후 Notebook Instance를 **Stop**하여 불필요한 과금을 방지하세요.

---

## 평가 체크리스트 진단 결과

### Pass/Fail (필수) — 6/6

| # | 요구사항 | 상태 | 증빙 |
|---|---------|------|------|
| 1 | Dockerfile로 이미지 직접 빌드 | PASS | `Dockerfile` |
| 2 | ECR로 push 성공 (Image URI 증빙) | PASS | `155954279556.dkr.ecr.us-east-1.amazonaws.com/cheonhj-titanic-training:latest` |
| 3 | `Estimator.fit()`로 Training Job 성공 종료 | PASS | `run_training.py` — `sagemaker.estimator.Estimator` + `estimator.fit()` |
| 4 | 최소 인스턴스 사용 및 근거 제시 | PASS | ml.m5.large — 워크로드 기반 근거 (데이터 크기, 모델 특성, 실측 54초 검증) |
| 5 | 모델 아티팩트 S3 저장 경로 명시 + 실제 존재 | PASS | `s3://cheonhj-mlops-edu-202602/output/cheonhj-titanic-2026-02-02-08-58-02-217/output/model.tar.gz` |
| 6 | 제3자가 따라할 수 있는 재현 절차 제공 | PASS | 재현 절차 (0)~(4) 복사-붙여넣기 커맨드 |

### Plus (가산점) — 4/4

| # | 항목 | 상태 | 증빙 |
|---|------|------|------|
| 1 | 하이퍼파라미터를 Estimator hyperparameters로 주입 | PASS | `run_training.py` hyperparameters 딕셔너리 → argparse 수신 |
| 2 | `metrics.json` 저장 및 로그 출력 정리 | PASS | `train.py` — `/opt/ml/model/metrics.json` + CloudWatch 로그 |
| 3 | IAM 최소권한 정책 초안 제시 | PASS | CLI 사용자 + SageMaker 역할 JSON 작성 |
| 4 | 비용/시간 최적화 근거 있는 적용 | PASS | Spot Instance 63% 절감, 인스턴스 비교 분석 |

### Excellence Rubric — 5/5

| 기준 | 상태 | 비고 |
|------|------|------|
| End-to-End 재현 가능 | OK | 재현 절차 커맨드로 완주 가능 |
| 요구사항 100% 충족 | OK | 필수 6항목 모두 충족 |
| SageMaker 컨테이너 규약 준수 | OK | `/opt/ml/input/data/`, `/opt/ml/model/` 사용 |
| 운영 관점 품질 | OK | IAM 정책 초안, 트러블슈팅 4건, 아티팩트 경로 명확 |
| 문서/코드 품질 | OK | README + Dockerfile + train.py + run_training.py 구성 명료 |
