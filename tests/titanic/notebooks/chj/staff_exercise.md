# Staff Exercise

## Excellence Rubric (합격 기준)

- **End-to-End 재현 가능**: 제3자가 그대로 실행해도 *동일하게* ECR 푸시 → SageMaker Training → S3 산출물 저장까지 완주 가능
- **요구사항 100% 충족**: (1) 직접 빌드한 Docker 이미지 (2) ECR 배포 (3) `Estimator.fit()`로 트레이닝 (4) 가능한 최소 인스턴스 사용 근거 (5) S3에 결과 저장
- **SageMaker 컨테이너 규약 준수**: `/opt/ml/input`, `/opt/ml/model`, `/opt/ml/output` 등 표준 경로 기반으로 동작
- **운영 관점 품질**: IAM 최소권한, 로그/에러 트러블슈팅 내역, 실행 파라미터/버전/아티팩트 경로 명확
- **문서/코드 품질**: README + 노트북 + Dockerfile + 학습 스크립트 구성 명료, 커맨드 한 줄로 재현

---

# [Exercise] SageMaker Training + Custom ECR Image로 Titanic 생존 예측 End-to-End 구축

## 1) 목적

스태프는 “코드 작성”이 아니라 **실행 가능한 실험 환경을 직접 만들고, 클라우드 표준 런타임(SageMaker)에서 학습을 재현**할 수 있어야 합니다.

본 과제는 아래 기술 적용을 검증합니다.

- Docker 기반 ML 트레이닝 이미지 작성
- ECR 빌드/푸시 및 IAM/권한 이해
- SageMaker Training(Job) 실행 모델 이해 (`Estimator.fit`)
- S3 입출력(데이터/모델 아티팩트) 파이프라인 구성

---

## 2) 미션 요약 (한 줄)

**Titanic 생존 예측 모델을 직접 만든 Docker 이미지로 ECR에 배포하고, SageMaker Training(`Estimator.fit`)으로 학습한 뒤, 모델 산출물을 S3에 저장하라.**

---

## 3) 필수 요구사항 (Must)

### A. 데이터 준비

- Titanic 데이터로 이진분류(생존/사망) 수행
- 학습 데이터는 **S3에서 입력**받아야 함
    - 예: `s3://<bucket>/titanic/train.csv`, `s3://<bucket>/titanic/val.csv` 등
- 데이터 소스는 자유(직접 업로드/공개 데이터 활용 등). 단, **제출 문서에 “데이터를 S3에 올리는 절차”가 포함**되어야 함

### B. Docker 이미지 제작

- 본인이 작성한 **Dockerfile**로 이미지를 빌드할 것
- 컨테이너는 SageMaker Training 규약을 따라야 함
    - 학습 스크립트가 `/opt/ml/input/data/<channel_name>/`에서 데이터를 읽고
    - 모델 파일을 `/opt/ml/model/`에 저장해야 함
- 학습 스크립트는 최소 아래를 포함
    - 데이터 로딩/전처리
    - 모델 학습
    - 평가(간단 지표 1개 이상: Accuracy/AUC/F1 등)
    - 모델 저장

### C. ECR 배포

- 본인 AWS 계정에 **ECR Repository 생성** 후
- Docker 이미지 **태그 지정** → **ECR push**까지 수행
- 제출물에 아래 정보가 반드시 포함
    - ECR Repo 이름
    - Image URI (예: `<account>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>`)
    - 빌드/푸시 커맨드 기록

### D. SageMaker Training 실행

- SageMaker Python SDK 사용
- **`sagemaker.estimator.Estimator` + `estimator.fit()` 필수 사용**
- 학습 입력은 `fit({"train": <s3_input>, "validation": <s3_input>})`처럼 **채널 기반**으로 구성 권장
- 학습 결과 모델 아티팩트는 **S3에 저장**되어야 함
    - 기본적으로 SageMaker는 `output_path` 또는 기본 버킷에 `model.tar.gz` 저장
    - 제출 문서에 **S3 결과 경로**를 명시

### E. 인스턴스 타입: “가능한 최소”

- Training 인스턴스는 **본인 환경에서 가능한 최소 스펙**을 사용
- 단순히 “작다”가 아니라,
    - *왜 그 인스턴스가 최소인지* (계정/리전 가용성, 할당량, 정책, 비용 등)
    - *실제 학습이 정상 완료되는지*
        
        를 **근거로 설명**
        
- 예: “해당 리전에서 Training에 사용 가능한 타입 중 최소”, “할당량 제한으로 이 타입이 최소” 등

---

## 4) 권장 구현 가이드 (Strongly Recommended)

### 컨테이너 구조 권장

- `train.py` : 학습 엔트리 스크립트
- `requirements.txt` : 필요 라이브러리 명시
- `Dockerfile` : 베이스 이미지 + 의존성 설치 + 엔트리포인트 지정
- 엔트리포인트는 `python train.py` 형태로 고정하고, 하이퍼파라미터는 argparse로 받기

### 모델 선택 (권장)

- **LightGBM / XGBoost / Logistic Regression** 중 택1
    - “최소 인스턴스에서도 빠르게 끝나는” 구성이 유리

### 학습 산출물 (권장)

- `/opt/ml/model/`에 아래 중 하나 이상 저장
    - `model.joblib` / `model.pkl`
    - `metrics.json` (지표 기록)
- 이를 S3 에 저장

---

## 5) 제출물 (Deliverables)

제출은 **Notion 페이지 1개**에 아래 구조로 정리하세요.

### 1) Executive Summary (5줄 내)

- 무엇을 만들었고, 어떤 이미지로, 어떤 인스턴스로, 어디(S3)에 저장했는지 요약

### 2) 아키텍처 & 플로우

- 데이터(S3) → Training(SageMaker) → 아티팩트(S3) 흐름을 글/다이어그램(간단)으로 설명

### 3) 리소스 정보 (필수)

- AWS Region:
- S3 Bucket / Prefix:
- ECR Repo:
- Image URI:
- SageMaker Training Job Name:
- Output S3 Path (model.tar.gz 위치):

### 4) 실행 방법 (재현 절차)

아래 형태로 “복사-붙여넣기 가능한 커맨드” 중심:

- (1) 데이터 S3 업로드 방법
- (2) ECR 로그인/리포 생성/빌드/푸시 커맨드
- (3) SageMaker 노트북(또는 로컬)에서 Training 실행 커맨드/코드
- (4) 결과 확인(CloudWatch logs, S3 경로)

### 5) 핵심 코드 스니펫

- Dockerfile 핵심 부분
- `Estimator(...)` 생성 코드
- `estimator.fit(...)` 호출 코드
- 모델 저장 로직 (`/opt/ml/model`)

### 6) 트러블슈팅 로그 (필수)

- 막혔던 이슈 1개 이상과 해결 과정
    - 예: ECR auth, IAM 권한, SageMaker 경로, 엔트리포인트 오류 등

### 7) 결과

- 지표(Accuracy/AUC 등) 1개 이상
- 학습 완료 스크린샷(선택) 또는 CloudWatch 로그 일부(선택)

---

## 6) 평가 체크리스트 (채점 기준)

### Pass/Fail (필수)

- [ ]  Dockerfile로 이미지 직접 빌드
- [ ]  ECR로 push 성공 (Image URI 증빙)
- [ ]  `Estimator.fit()`로 Training Job 성공 종료
- [ ]  최소 인스턴스 사용 및 근거 제시
- [ ]  모델 아티팩트 S3 저장 경로 명시 + 실제 존재
- [ ]  제3자가 따라할 수 있는 재현 절차 제공

### Plus (가산점)

- [ ]  하이퍼파라미터를 Estimator hyperparameters로 주입
- [ ]  `metrics.json` 저장 및 로그 출력 정리
- [ ]  IAM 최소권한 정책 초안 제시
- [ ]  비용/시간 최적화(스팟, 데이터 샘플링 등) 근거 있는 적용

---

## 7) 흔한 실패 포인트 (미리 경고)

- ECR push 권한 부족 (`ecr:*`, `sts:GetCallerIdentity` 등)
- SageMaker Training 컨테이너가 `/opt/ml/input/data/...` 경로를 못 읽음
- 모델을 로컬에 저장하고 `/opt/ml/model`에 저장하지 않아 아티팩트가 비어 있음
- `image_uri`는 맞는데 엔트리포인트/커맨드가 없어 컨테이너가 바로 종료됨
- 인스턴스 “최소”라고 주장했지만 근거/증빙이 없음