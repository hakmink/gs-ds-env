# Titanic Survival Prediction - ML Pipeline 샘플

타이타닉 생존 예측을 예제로, 로컬 개발부터 SageMaker Training Job 실행까지의 전체 ML 파이프라인 워크플로우를 보여줍니다.

## 전체 워크플로우

```
prepare_input/   →   modeling/   →   docker/
  (데이터 준비)      (모델링 개발)     (SageMaker 실행)
```

### 1단계: 입력 데이터 준비 (`prepare_input/`)

`prepare.ipynb`를 실행하면 로컬의 `conf/`, `data/` 폴더 파일들을 S3에 업로드합니다.

```
[로컬]                          [S3]
conf/env.yml      →   s3://gs-retail-awesome-conf-{region}/{env}/{user_id}/{project}/{experiment}/
conf/meta.yml
conf/model.yml

data/train.csv    →   s3://gs-retail-awesome-data-{region}/{env}/{user_id}/{project}/{version}/data/
data/validation.csv
data/test.csv
```

### 2단계: 로컬 모델링 개발 (`modeling/`)

1. **`load_conf_datasets.ipynb`** 실행: S3에서 `conf/`, `data/` 폴더를 로컬로 다운로드하여 모델링 환경 구성
2. **`modeling.ipynb`** 실행: 로컬 `conf/`, `data/` 파일을 이용하여 모델링 수행
3. **`run_pm.py` 테스트**: papermill로 노트북이 정상 실행되는지 사전 검증

```bash
python run_pm.py \
  --conf-s3-path s3://gs-retail-awesome-conf-us-west-2/dev/sample/titanic-survival-prediction/baseline-sean-v1/ \
  --notebook-path ./modeling.ipynb
```

### 3단계: SageMaker Training Job (`docker/`)

`run_pm.py` 테스트가 통과되면, Docker 이미지를 빌드하여 SageMaker estimator로 실행합니다.

- `run_sm_job.ipynb`: `estimator.fit()`에 Docker 이미지 경로와 `--conf-s3-path`를 지정하면 SageMaker Training Job으로 `run_pm.py`가 실행됩니다.

## S3 버킷 구조

| 버킷 | 용도 |
|------|------|
| `gs-retail-awesome-conf-{region}` | 설정 파일 (yml), 모델링 노트북 |
| `gs-retail-awesome-data-{region}` | 학습/검증/테스트 데이터 |
| `gs-retail-awesome-model-{region}` | 모델링 결과물 (artifacts) |

## 폴더 구조

```
samples/sean/
├── prepare_input/          # 1단계: 데이터 준비
│   ├── prepare.ipynb       # conf, data → S3 업로드
│   ├── conf/               # 설정 파일 (env.yml, meta.yml, model.yml)
│   └── data/               # 원본 데이터 (train, validation, test csv)
│
├── modeling/               # 2단계: 모델링 개발 및 검증
│   ├── load_conf_datasets.ipynb  # S3 → 로컬 conf, data 다운로드
│   ├── modeling.ipynb            # 모델링 탐색 및 개발
│   ├── modeling.ipynb            # 최종 모델링 노트북 (run_pm.py 대상)
│   └── run_pm.py                 # papermill 로컬 테스트 실행기
│
└── docker/                 # 3단계: SageMaker 실행
    ├── Dockerfile
    ├── run_pm.py            # SageMaker Training entry point
    ├── requirements.txt
    ├── build_and_push_sm.sh
    └── run_sm_job.ipynb     # SageMaker Training Job 실행
```
