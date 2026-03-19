# prepare_input - 데이터 준비

로컬의 설정 파일과 데이터를 S3에 업로드하여 모델링 파이프라인의 입력을 준비합니다.

## 실행 순서

### `prepare.ipynb` 실행

로컬 `conf/`, `data/` 폴더의 파일들을 S3에 업로드합니다.

**업로드 대상**

| 로컬 경로 | S3 경로 |
|-----------|---------|
| `conf/env.yml` | `s3://gs-retail-awesome-conf-{region}/{env}/{user_id}/{project}/{experiment}/` |
| `conf/meta.yml` | 위와 동일 |
| `conf/model.yml` | 위와 동일 |
| `data/train.csv` | `s3://gs-retail-awesome-data-{region}/{env}/{user_id}/{project}/{version}/data/` |
| `data/validation.csv` | 위와 동일 |
| `data/test.csv` | 위와 동일 |

**S3 경로 예시** (conf/env.yml, meta.yml 기준)

```
Conf: s3://gs-retail-awesome-conf-us-west-2/dev/sample/titanic-survival-prediction/baseline-sean-v1/
Data: s3://gs-retail-awesome-data-us-west-2/dev/sample/titanic-survival-prediction/v1.0/data/
```

## 폴더 구조

```
prepare_input/
├── prepare.ipynb           # S3 업로드 실행 노트북
├── conf/
│   ├── env.yml             # 환경 설정 (region, env, S3 버킷명)
│   ├── meta.yml            # 메타 정보 (user_id, project, experiment, version)
│   └── model.yml           # 모델 하이퍼파라미터 및 알고리즘 설정
└── data/
    ├── train.csv
    ├── validation.csv
    └── test.csv
```

## 설정 파일 구조

- **`env.yml`**: AWS region, 환경(dev/prod), S3 버킷명
- **`meta.yml`**: user_id, project명, experiment명, 데이터 version
- **`model.yml`**: 알고리즘명, 하이퍼파라미터, suffix 등

## 다음 단계

업로드 완료 후 `modeling/` 폴더의 `load_conf_datasets.ipynb`를 실행하여 S3에서 파일을 다운로드하고 모델링 환경을 구성합니다.
