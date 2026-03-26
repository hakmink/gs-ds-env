# modeling - 모델링 개발 및 검증

S3에서 입력 데이터를 받아 모델링을 개발하고, SageMaker 실행 전 로컬에서 사전 검증합니다.

## 실행 순서

### Step 1. `load_conf_datasets.ipynb` 실행

S3에서 `conf/`, `data/` 파일을 현재 폴더 하위로 다운로드하여 모델링 환경을 구성합니다.

```
[S3]                                                    [로컬]
gs-retail-awesome-conf-{region}/.../experiment/  →  conf/env.yml
                                                     conf/meta.yml
                                                     conf/model.yml

gs-retail-awesome-data-{region}/.../version/data/  →  data/train.csv
                                                       data/validation.csv
                                                       data/test.csv
```

### Step 2. `modeling.ipynb` 실행

로컬의 `conf/`, `data/` 파일을 이용하여 모델 탐색 및 개발을 수행합니다.
EDA, feature engineering, 하이퍼파라미터 튜닝 등 반복적인 실험 작업을 여기서 진행합니다.

### Step 3. `run_pm.py` 로컬 테스트

모델링이 완성되면 `modeling.ipynb`를 papermill로 실행하여 SageMaker 환경 진입점(`run_pm.py`)이 정상 동작하는지 사전 검증합니다.

```bash
python run_pm.py \
  --conf-s3-path s3://gs-retail-awesome-conf-us-west-2/dev/sample/titanic-survival-prediction/baseline-sean-v1/ \
  --notebook-path ./modeling.ipynb
```

이 테스트가 통과되면 Docker 이미지를 SageMaker estimator에 연결했을 때도 동일하게 동작하는 것을 보장합니다.

## `run_pm.py` 동작 방식

1. `--conf-s3-path`에서 yml 파일 → `conf/` 폴더로 다운로드
2. `env.yml` 기반으로 data S3 경로 구성 후 `data/` 폴더로 다운로드
3. `--notebook-path`로 지정된 노트북을 papermill로 실행
4. 실행 결과(executed notebook 등)를 model S3 버킷에 업로드

## 폴더 구조

```
modeling/
├── load_conf_datasets.ipynb  # S3 → 로컬 conf, data 다운로드
├── modeling.ipynb            # 모델링 탐색·개발 및 run_pm.py 실행 대상 노트북
├── run_pm.py                 # papermill 기반 파이프라인 실행기
├── nb_run_pm_py.ipynb        # run_pm.py 노트북 테스트용
├── upload_artifacts.ipynb    # 결과물 S3 업로드
└── run_sm_job.ipynb          # SageMaker Training Job 실행
```

> `conf/`, `data/` 폴더는 `load_conf_datasets.ipynb` 실행 후 자동 생성됩니다. git에 커밋하지 않습니다.

## 다음 단계

로컬 `run_pm.py` 테스트 통과 후, `docker/` 폴더의 `run_sm_job.ipynb`를 실행하여 SageMaker Training Job으로 제출합니다.
