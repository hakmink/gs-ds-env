# Titanic SageMaker Training 프로젝트 진행 기록

## 날짜: 2026-01-29

---

## 1. 프로젝트 초기 설정

### AWS 계정 정보
- **리전**: us-east-1 (N. Virginia)
- **계정 ID**: 155954279556
- **IAM 사용자**: cheon.hj

### IAM User (cheon.hj)

- **ARN**: `arn:aws:iam::155954279556:user/cheon.hj`
- 관리자가 필요한 권한을 사전 설정함

### IAM Role (AmazonSageMaker-ExecutionRole)

> **IAM Role**은 IAM User와 다릅니다. User는 "사람(CLI)"이 쓰는 것이고, Role은 "AWS 서비스(SageMaker)"가 쓰는 것입니다. Training Job이 실행되면 SageMaker가 이 Role을 입고 S3에서 데이터를 읽고, ECR에서 이미지를 가져옵니다.

- **ARN**: `arn:aws:iam::155954279556:role/service-role/AmazonSageMaker-ExecutionRole-20260123T111313`
- 관리자가 사전 생성한 역할 (SageMaker Notebook Instance에 연결)

---

## 2. 데이터 준비

### 데이터 Split (80:20)
```bash
uv run python split_data.py
```
- **원본**: 891 rows
- **Train**: 712 rows (80%)
- **Val**: 179 rows (20%)
- **출력**: `data/output/train.csv`, `data/output/val.csv`

### S3 업로드

> **S3 (Simple Storage Service)** 는 AWS의 파일 저장소입니다. "버킷"이라는 폴더 같은 단위로 파일을 관리합니다. SageMaker는 학습 데이터를 S3에서 읽어오므로, 먼저 여기에 올려놔야 합니다.

```bash
# SageMaker Notebook 터미널에서 실행
aws s3 cp data/output/train.csv s3://cheonhj-mlops-edu-202602/titanic/train.csv
aws s3 cp data/output/val.csv s3://cheonhj-mlops-edu-202602/titanic/val.csv
```
- **S3 버킷**: `cheonhj-mlops-edu-202602`
- **경로**: `s3://cheonhj-mlops-edu-202602/titanic/`

---

## 3. 학습 스크립트 작성

### train.py

> SageMaker 컨테이너 안에서 실행되는 학습 스크립트입니다. 내 PC가 아니라 AWS 클라우드의 Docker 컨테이너 안에서 돌아갑니다.

- **모델**: LightGBM (LGBMClassifier) — 빠르고 가벼운 트리 기반 분류 모델
- **하이퍼파라미터**: n_estimators(트리 수), max_depth(트리 깊이), random_state(재현 시드)
  - `argparse`로 수신: SageMaker가 컨테이너에 `--n_estimators 100` 같은 형태로 전달
- **전처리**: 결측치 처리(빈 값을 중앙값 등으로 채움), LabelEncoder(문자열을 숫자로 변환)
- **평가 지표**: Accuracy(정확도), F1(정밀도+재현율 조합), AUC(분류 성능 종합 지표)
- **SageMaker 규약 준수** (이 경로를 지켜야 SageMaker가 데이터/모델을 자동 처리):
  - 입력: `/opt/ml/input/data/train/`, `/opt/ml/input/data/validation/` — SageMaker가 S3 데이터를 여기에 복사해줌
  - 출력: `/opt/ml/model/` (model.joblib, metrics.json) — 여기에 저장하면 SageMaker가 자동으로 S3에 업로드

### requirements.txt
```
pandas
scikit-learn
lightgbm
joblib
```

### pyproject.toml vs requirements.txt — 왜 2개인가?

> 이 프로젝트에는 Python 라이브러리를 설치하는 파일이 2개 있습니다. 실행 환경이 다르기 때문입니다.

```
내 Mac (로컬)                     AWS 클라우드 (Docker 컨테이너)
─────────────                     ──────────────────────────
pyproject.toml                    requirements.txt
├── pandas                        ├── pandas
├── scikit-learn                  ├── scikit-learn
└── sagemaker ← 학습 요청용       ├── lightgbm ← 실제 학습용
                                  └── joblib
         │                                 │
    run_training.py                    train.py
    "AWS야, 학습해줘"                "데이터 읽고, 모델 학습"
```

- **`pyproject.toml`**: 내 Mac에서 `uv sync`로 설치. `sagemaker` 포함 (AWS에 Training Job을 요청하는 라이브러리)
- **`requirements.txt`**: Docker 이미지 빌드 시 `pip install`로 설치. `lightgbm` 포함 (실제 모델 학습 라이브러리)

`sagemaker`는 컨테이너 안에서 쓸 일이 없고, `lightgbm`은 내 Mac에서 쓸 일이 없어서 각각 필요한 곳에만 넣어둔 것입니다.

---

## 4. Docker 이미지 빌드 & ECR 배포

### Dockerfile

> Dockerfile은 Docker 이미지를 만드는 "레시피"입니다. 어떤 OS를 쓰고, 어떤 라이브러리를 설치하고, 어떤 코드를 실행할지 한 줄씩 지시합니다.

```dockerfile
FROM python:3.12-slim                # 베이스 이미지: Python 3.12이 설치된 가벼운 Linux
WORKDIR /opt/program                 # 작업 디렉토리 설정 (이후 명령은 이 폴더에서 실행)
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
                                     # libgomp1: LightGBM이 필요로 하는 병렬처리 라이브러리
COPY requirements.txt .              # 로컬의 requirements.txt를 컨테이너에 복사
RUN pip install --no-cache-dir -r requirements.txt   # Python 라이브러리 설치
COPY train.py .                      # 학습 스크립트를 컨테이너에 복사
ENTRYPOINT ["python", "train.py"]    # 컨테이너가 시작되면 이 명령을 실행
```

> Dockerfile 관련 트러블슈팅(Exec format error, libgomp 누락, unrecognized arguments)은 [섹션 8 — 트러블슈팅 기록](#트러블슈팅-기록) 참조.

### 빌드 & Push
```bash
# SageMaker Notebook 터미널에서 실행
docker build -t cheonhj-titanic-training .

# ECR Repository 생성
aws ecr create-repository --repository-name cheonhj-titanic-training --region us-east-1

# ECR 로그인
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 155954279556.dkr.ecr.us-east-1.amazonaws.com

# 태그 & Push
docker tag cheonhj-titanic-training:latest 155954279556.dkr.ecr.us-east-1.amazonaws.com/cheonhj-titanic-training:latest
docker push 155954279556.dkr.ecr.us-east-1.amazonaws.com/cheonhj-titanic-training:latest
```

### ECR 정보
- **Repository**: cheonhj-titanic-training
- **Image URI**: `155954279556.dkr.ecr.us-east-1.amazonaws.com/cheonhj-titanic-training:latest`

---

## 5. SageMaker Training 스크립트

### run_training.py (Estimator API — 과제 필수)

> 내 로컬 PC에서 실행하는 스크립트입니다. AWS에 "이 Docker 이미지로, 이 데이터로, 이 인스턴스에서 학습해줘"라고 요청하는 역할입니다.

```python
from sagemaker.estimator import Estimator

estimator = Estimator(
    image_uri=IMAGE_URI,          # ECR에 올린 Docker 이미지 주소
    role=ROLE_ARN,                # SageMaker가 사용할 IAM Role (S3/ECR 접근용)
    instance_count=1,             # 학습에 사용할 인스턴스 수 (1대)
    instance_type="ml.m5.large",  # 인스턴스 사양 — 선택 근거: 섹션 6, 비용 분석: 섹션 10
    output_path=f"s3://{BUCKET}/output",      # 학습 결과(model.tar.gz) 저장 위치
    base_job_name="cheonhj-titanic",          # Job 이름 접두사 (뒤에 타임스탬프 자동 추가)
    hyperparameters={             # train.py에 전달할 하이퍼파라미터
        "n_estimators": "100",    #   트리 수
        "max_depth": "5",         #   트리 최대 깊이
        "random_state": "42",     #   랜덤 시드 (재현성)
    },
    max_run=3600,                 # 최대 실행 시간 (초) — 1시간 넘으면 자동 중단
    use_spot_instances=True,      # Spot Instance 사용 — 상세 분석: 섹션 10
    max_wait=7200,                # Spot 포함 최대 대기 시간 (초) — Spot 할당 대기 포함
)

# fit()을 호출하면 AWS에 Training Job이 생성되고, 완료될 때까지 자동 대기
# "train", "validation" 채널로 S3 데이터를 컨테이너에 전달
estimator.fit({
    "train": f"s3://{BUCKET}/titanic/train.csv",
    "validation": f"s3://{BUCKET}/titanic/val.csv",
})
```

---

## 6. 인스턴스 타입 선택 근거

### 선택: `ml.m5.large`
- 2 vCPU, 8GB RAM
- General Purpose (범용)

### 근거
- Titanic 데이터(891행, 7 피처)는 1k 미만 tabular 데이터로 GPU/Compute-optimized 인스턴스 불필요
- LightGBM은 경량 트리 모델로 대용량 메모리나 다수 코어를 요구하지 않음
- ml.m5.large(2 vCPU, 8 GiB)는 m5 범용 계열의 최소 사이즈이며, 실제 학습 54초 완료로 적합성 검증됨
- **워크로드 대비 충분한 최소 범용 인스턴스인 `ml.m5.large` 선택**

> ml.t3 계열과의 비교, Spot Instance 적용 효과, 실제 청구 비용 등 상세 분석은 [섹션 10 — 비용 최적화](#10-비용-최적화) 참조.

---

## 7. 현재 상태 & 다음 단계

### 완료
- [x] 데이터 준비 (split, S3 업로드)
- [x] train.py 작성 (LightGBM, SageMaker 규약 준수)
- [x] Dockerfile 작성
- [x] Docker 이미지 빌드
- [x] ECR 배포
- [x] SageMakerExecutionRole 생성
- [x] run_training.py 작성 (SageMaker SDK v2 Estimator API)

### 대기 중
- [x] **Service Quota 증가 승인 완료** (2026-02-02 확인)
  - `ml.m5.large for training job usage`: 0 → **15** (승인됨)
  - `ml.m5.large for spot training job usage`: 4 → **10** (승인됨)
- [x] `run_training.py`를 `Estimator.fit()` 기반으로 수정 완료
- [x] Training Job 실행 및 완료 확인 (2026-02-02)

---

## 8. Service Quota 증가 요청

### 요청 정보
- **Quota Name**: ml.m5.large for training job usage
- **Quota Code**: L-611FA074
- **요청 값**: 0 → 1
- **요청 시간**: 2026-01-29 17:02:47 (KST)
- **현재 상태**: APPROVED (2026-02-02 확인, Applied: 15)

### 요청 이유

> **Service Quota**란? AWS는 각 서비스별로 "최대 몇 개까지 쓸 수 있는지" 제한을 둡니다. 무료 계정은 SageMaker Training용 인스턴스 한도가 **0**으로 시작하므로, 증가를 요청해야 Training Job을 실행할 수 있습니다.

AWS 무료 플랜 계정에서는 SageMaker Training Job 인스턴스 quota가 기본적으로 **0**으로 설정되어 있음.
Training Job을 실행하려면 최소 1개의 인스턴스 quota가 필요하여 증가 요청함.

### 상태 확인 방법

**CLI로 확인:**
```bash
# 내가 요청한 quota 변경 이력을 테이블로 조회
aws service-quotas list-requested-service-quota-change-history-by-quota \
  --service-code sagemaker \        # 대상 서비스: SageMaker
  --quota-code L-611FA074 \         # ml.m5.large training job의 quota 코드
  --region ap-northeast-2 \
  --query "RequestedQuotas[*].{Status:Status,Created:Created,DesiredValue:DesiredValue}" \
  --output table                    # 결과를 표 형태로 출력
```

**콘솔에서 확인:**
[Service Quotas - 요청 기록](https://ap-northeast-2.console.aws.amazon.com/servicequotas/home/requests)

### 상태 값 의미
| Status | 의미 |
|--------|------|
| PENDING | 검토 대기 중 |
| CASE_OPENED | AWS Support 케이스 열림 |
| APPROVED | 승인됨 |
| DENIED | 거부됨 |
| CASE_CLOSED | 케이스 종료 |

### 승인 후 확인
```bash
# 현재 적용된 quota 값을 확인 (1.0 이상이면 Training Job 실행 가능)
aws service-quotas get-service-quota \
  --service-code sagemaker \
  --quota-code L-611FA074 \
  --region ap-northeast-2 \
  --query "Quota.Value"             # 결과에서 quota 숫자만 추출
```
결과가 `1.0`이면 승인 완료

### 승인 후 진행 (2026-02-02 완료)
- [x] SageMaker Notebook 터미널에서 `python run_training.py` 실행
- [x] Training Job 완료 확인: `cheonhj-titanic-2026-02-02-08-58-02-217`
- [x] 모델 아티팩트 S3 저장 확인: `s3://cheonhj-mlops-edu-202602/output/cheonhj-titanic-2026-02-02-08-58-02-217/output/model.tar.gz`

### Training 결과
- **Training Job**: `cheonhj-titanic-2026-02-02-08-58-02-217`
- **Training seconds**: 54
- **Billable seconds**: 20 (Spot 할인 63%)
- **Metrics**:
  - train_accuracy: 0.9171
  - val_accuracy: 0.7877
  - val_f1: 0.7031
  - val_auc: 0.7980
- **Model artifacts**: `s3://cheonhj-mlops-edu-202602/output/cheonhj-titanic-2026-02-02-08-58-02-217/output/model.tar.gz`

### Training Job 증빙 (describe-training-job)

> 아래 명령어로 Training Job의 상태, 사용 인스턴스, Spot 여부, 소요 시간, 모델 저장 경로 등을 한눈에 확인할 수 있습니다.

```bash
# Training Job 상세 정보 조회 (--query로 필요한 필드만 추출)
aws sagemaker describe-training-job \
  --training-job-name cheonhj-titanic-2026-02-02-08-58-02-217 \
  --region us-east-1 \
  --query '{Status:TrainingJobStatus,Instance:ResourceConfig.InstanceType,InstanceCount:ResourceConfig.InstanceCount,Spot:EnableManagedSpotTraining,TrainingSeconds:TrainingTimeInSeconds,BillableSeconds:BillableTimeInSeconds,OutputPath:ModelArtifacts.S3ModelArtifacts}' \
  --output table
```

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
- **Log Group**: `/aws/sagemaker/TrainingJobs`
- **Log Stream**: `cheonhj-titanic-2026-02-02-08-58-02-217/algo-1-*`

```bash
# CloudWatch 로그 조회 커맨드
aws logs get-log-events \
  --log-group-name /aws/sagemaker/TrainingJobs \
  --log-stream-name cheonhj-titanic-2026-02-02-08-58-02-217/algo-1-1738483147 \
  --region us-east-1 \
  --query 'events[*].message' \
  --output text
```

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

### 트러블슈팅 기록

#### 이슈 1: Exec format error — Docker 이미지 아키텍처 불일치

- **증상**: SageMaker Training Job이 시작 직후 실패, `exec format error` 로그
- **원인**: Apple Silicon(ARM) Mac에서 `docker build`하면 `linux/arm64` 이미지가 생성되는데, SageMaker는 `linux/amd64`(x86_64) 환경에서 실행
- **해결**: SageMaker Notebook Instance(x86_64) 터미널에서 빌드하여 네이티브 amd64 이미지 생성. 로컬 Mac에서 빌드 시에는 `docker build --platform linux/amd64` 사용

#### 이슈 2: libgomp.so.1 누락 — LightGBM 런타임 의존성

- **증상**: 컨테이너 시작 후 `ImportError: libgomp.so.1: cannot open shared object file`
- **원인**: `python:3.12-slim` 베이스 이미지에 LightGBM이 필요로 하는 OpenMP 런타임(`libgomp1`)이 미포함
- **해결**: Dockerfile에 `RUN apt-get update && apt-get install -y --no-install-recommends libgomp1` 추가

#### 이슈 3: unrecognized arguments: train — SageMaker 컨테이너 호출 규약

- **증상**: 컨테이너 실행 시 `error: unrecognized arguments: train` 으로 즉시 종료
- **원인**: SageMaker는 Training 컨테이너를 `python train.py train` 형태로 호출하는데, argparse에서 `train` 위치 인수를 처리하지 않음
- **해결**: `parser.add_argument("command", nargs="?", default="train")` 추가하여 `train` 커맨드 인수를 수용

#### 이슈 4: Could not assume role — IAM Role ARN 경로 오류

- **증상**: `Estimator.fit()` 호출 시 `ClientError: Could not assume role` 에러
- **원인**: SageMaker 콘솔에서 자동 생성된 역할의 ARN에 `/service-role/` 경로가 포함되어 있었으나, 코드에서 `role/AmazonSageMaker-...`로만 지정
- **해결**: `sagemaker.get_execution_role()`로 정확한 ARN(`role/service-role/AmazonSageMaker-...`) 확인 후 수정

---

## 리소스 정보 요약

| 항목 | 값 |
|------|-----|
| AWS Region | us-east-1 (N. Virginia) |
| S3 Bucket | cheonhj-mlops-edu-202602 |
| S3 데이터 경로 | s3://cheonhj-mlops-edu-202602/titanic/ |
| ECR Repository | cheonhj-titanic-training |
| Image URI | 155954279556.dkr.ecr.us-east-1.amazonaws.com/cheonhj-titanic-training:latest |
| SageMaker Role | arn:aws:iam::155954279556:role/service-role/AmazonSageMaker-ExecutionRole-20260123T111313 |
| Training Instance | ml.m5.large (Spot) |
| Output S3 Path | s3://cheonhj-mlops-edu-202602/output/cheonhj-titanic-2026-02-02-08-58-02-217/output/model.tar.gz |

---

## 9. IAM 최소권한 정책 초안

현재 `cheon.hj` 사용자에는 관리자가 설정한 정책이 부여되어 있음. 프로덕션 운영 시에는 아래 최소권한 정책으로 대체 권장.

> **최소권한 원칙이란?** "필요한 권한만 딱 필요한 만큼만 부여한다"는 보안 원칙입니다. FullAccess는 해당 서비스의 모든 작업을 허용하므로 편하지만, 실수나 보안 사고 시 피해 범위가 커집니다. 아래 정책은 이 프로젝트에 실제로 필요한 권한만 나열한 것입니다.

### CLI 사용자 (cheon.hj) 최소권한

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

### SageMaker 실행 역할 (SageMakerExecutionRole) 최소권한

> SageMaker Training Job이 AWS 내부에서 실행될 때 사용하는 역할입니다. 내 PC가 아니라 **SageMaker 서비스 자체**가 S3에서 데이터를 읽고, ECR에서 이미지를 가져오고, 로그를 쓸 때 이 역할의 권한을 사용합니다.

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

---

## 10. 비용 최적화

> 인스턴스 타입 선택 요약은 [섹션 6 — 인스턴스 타입 선택 근거](#6-인스턴스-타입-선택-근거), Service Quota 이력은 [섹션 8 — Service Quota 증가 요청](#8-service-quota-증가-요청) 참조.

### 인스턴스 타입 선택 분석

#### 후보 인스턴스 비교 (us-east-1)

| 인스턴스 | 유형 | vCPU | RAM | On-Demand 시간당 (추정) | 계정 Training Quota |
|---------|------|------|-----|----------------------|-------------------|
| **ml.t3.medium** | Burstable | 2 | 4 GiB | ~$0.065 | **0** (미할당) |
| **ml.t3.large** | Burstable | 2 | 8 GiB | ~$0.130 | **0** (미할당) |
| ml.t3.xlarge | Burstable | 4 | 16 GiB | ~$0.260 | **0** (미할당) |
| **ml.m5.large** | General Purpose | 2 | 8 GiB | ~$0.134 | **15** (승인됨) |
| ml.m5.xlarge | General Purpose | 4 | 16 GiB | ~$0.269 | 0 |

> 가격 출처: AWS SageMaker 공식 가격표 기준 추정 (ap-northeast-3 Osaka 참조, Seoul은 유사 수준)

#### ml.t3.large vs ml.m5.large 직접 비교

동일 스펙(2 vCPU, 8 GiB)으로, 가격 차이가 거의 없다.

| 항목 | ml.t3.large | ml.m5.large | 차이 |
|------|------------|-------------|------|
| vCPU | 2 | 2 | 동일 |
| RAM | 8 GiB | 8 GiB | 동일 |
| 유형 | Burstable | General Purpose | Burstable은 장시간 시 throttle |
| On-Demand 시간당 | ~$0.131 | ~$0.134 | **~2% (~$0.003 차이)** |
| 60초 학습 비용 | ~$0.00218 | ~$0.00223 | **~$0.00005 차이** |
| Training Job Quota | **0** (미할당) | **15** (승인됨) | ml.m5.large만 즉시 사용 가능 |
| Spot Training Quota | **0** (미할당) | **10** (승인됨) | ml.m5.large만 즉시 사용 가능 |

ml.t3.large는 quota 증가 요청 + 승인 대기가 필요하면서 절약 가능 금액이 $0.00005 수준이므로, 이미 승인된 ml.m5.large를 선택한 것이 합리적.

#### ml.t3.medium은?

Titanic 데이터(891행, 7 피처)는 극히 가벼운 워크로드이므로 **ml.t3.medium (2 vCPU, 4 GiB)** 으로도 충분히 학습 가능했다. 그러나 **ml.t3.medium은 SageMaker Training Job을 지원하지 않는 인스턴스 타입**이다. Service Quotas에 `ml.t3.medium for training job usage` 항목 자체가 존재하지 않으며, notebook instance(6)와 processing job(10) 용도로만 사용 가능하다.

그리고 **ml.t3 계열은 현재 계정에서 Training Job quota가 전부 0**이다:

```
ml.t3.medium for training job usage       → 0
ml.t3.large for training job usage        → 0
ml.t3.xlarge for training job usage       → 0
ml.t3.medium for spot training job usage  → 0
ml.t3.large for spot training job usage   → 0
ml.t3.xlarge for spot training job usage  → 0
```

quota 증가를 요청할 수 있지만(Adjustable: True), 승인까지 수 시간~수 일 소요된다. ml.m5.large는 이미 quota 15로 승인된 상태였으므로 **즉시 실행 가능한 최소 인스턴스**로 ml.m5.large를 선택했다.

#### ml.t3 vs ml.m5 특성 비교

| 특성 | ml.t3 (Burstable) | ml.m5 (General Purpose) |
|------|-------------------|------------------------|
| CPU 모델 | Burstable (크레딧 기반) | 고정 성능 |
| 짧은 학습 | 적합 (크레딧 충분) | 적합 |
| 장시간 학습 | 부적합 (크레딧 소진 시 throttle) | 적합 |
| 가격 | 저렴 | 약간 비쌈 |
| 본 워크로드 적합성 | 충분 (60초 학습) | 과잉이지만 안정적 |

**결론**: ml.t3.medium은 Training Job 미지원. ml.t3.large는 동일 스펙에서 ~2% 저렴하지만 quota 0. 이미 승인된 ml.m5.large가 실질적 최소 인스턴스.

### Spot Instance 적용

> **Spot Instance**란? AWS가 남는 서버 자원을 할인 판매하는 것입니다. On-Demand(정가)보다 최대 90% 저렴하지만, AWS가 자원이 필요하면 중간에 회수(중단)할 수 있습니다. 짧은 학습에는 중단 리스크가 거의 없어서 매우 유리합니다.

- `run_training.py`에 `use_spot_instances=True`, `max_wait=7200` 설정
- On-Demand 대비 최대 70~90% 비용 절감
- Titanic 학습은 54초 내 완료되므로 Spot 중단 리스크 극히 낮음
- us-east-1에서 ml.m5.large Spot Training 사용 가능 확인

### 실제 청구 비용 (본 Training Job)

| 항목 | 값 |
|------|-----|
| Training seconds | 54 |
| Billable seconds | **20** |
| Spot 절감률 | **63%** |
| 인스턴스 | ml.m5.large |
| On-Demand 시 추정 비용 | ~$0.115 × (54/3600) = **~$0.0017** |
| Spot 실제 추정 비용 | ~$0.035 × (20/3600) = **~$0.00019** |

### 비용 요약

| 최적화 수단 | 절감 효과 |
|------------|----------|
| ml.m5.large (가용한 최소) | 불필요한 대형 인스턴스 회피 |
| Spot Instance | On-Demand 대비 63% 절감 (실측) |

---

## 11. 리소스 상태 점검 (2026-02-02)

> 학습 완료 후 불필요한 과금이 발생하지 않는지 확인한 결과입니다.

### 점검 결과

| 리소스 | 상태 | 비용 영향 |
|--------|------|----------|
| SageMaker Training Jobs | 실행 중 없음 | 없음 |
| SageMaker Endpoints | 없음 | 없음 |
| SageMaker Notebook Instances | 없음 | 없음 |
| SageMaker Processing Jobs | 실행 중 없음 | 없음 |
| S3 Bucket (`cheonhj-mlops-edu-202602`) | 데이터 + 모델 아티팩트 | 무시할 수준 (수 MB 미만) |
| ECR Repository (`cheonhj-titanic-training`) | 이미지 1개 | 무시할 수준 (이미지 1개, 수백 MB) |
| SageMaker Notebook (`cheon-hj-t3-50g`) | 사용 후 Stop 필요 | Stop 상태면 과금 없음 |

### 점검 커맨드

```bash
# SageMaker Notebook 터미널에서 실행
aws sagemaker list-training-jobs --status-equals InProgress --region us-east-1
aws sagemaker list-endpoints --region us-east-1
aws sagemaker list-notebook-instances --query "NotebookInstances[?NotebookInstanceStatus=='InService']" --region us-east-1

# S3 버킷 사용량 확인
aws s3 ls s3://cheonhj-mlops-edu-202602/ --recursive --summarize

# ECR 리포지토리 확인
aws ecr describe-repositories --region us-east-1
```

### 리소스 정리 (필요 시)

> 프로젝트 완료 후 비용을 완전히 차단하려면 아래 명령어로 리소스를 삭제할 수 있습니다. **삭제하면 복구 불가**이므로 신중하게 실행하세요.

```bash
# Notebook Instance 정지 (콘솔에서 Stop 또는)
aws sagemaker stop-notebook-instance --notebook-instance-name cheon-hj-t3-50g --region us-east-1

# S3 버킷 비우기 + 삭제
aws s3 rm s3://cheonhj-mlops-edu-202602 --recursive
aws s3 rb s3://cheonhj-mlops-edu-202602

# ECR 이미지 삭제
aws ecr batch-delete-image --repository-name cheonhj-titanic-training --image-ids imageTag=latest --region us-east-1
aws ecr delete-repository --repository-name cheonhj-titanic-training --region us-east-1
```
