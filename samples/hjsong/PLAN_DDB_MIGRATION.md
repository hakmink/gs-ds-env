# DynamoDB 마이그레이션 플랜: hjsong ML Pipeline

## 개요

기존 S3 기반 파이프라인을 DynamoDB 기반으로 전환한다.
기존 파일은 수정하지 않고 `ddb/` 폴더에 새 파일을 생성한다.
`modeling_ddb.ipynb`는 MLflow 모델 등록도 함께 수행한다.

**DynamoDB 테이블:** `gsretail-mlops-edu-hjsong`
**Billing:** PAY_PER_REQUEST (테이블이 없으면 자동 생성)

---

## 폴더 구조

```
samples/hjsong/
├── PLAN_DDB_MIGRATION.md
├── README.md
├── prepare_input/           (기존 S3 파이프라인 — 변경 없음)
│   ├── prepare.ipynb
│   ├── conf/
│   └── data/
├── modeling/                (기존 S3 파이프라인 — 변경 없음)
│   ├── modeling.ipynb
│   ├── modeling_mlflow.ipynb
│   └── ...
└── ddb/                     (DynamoDB 전용 파이프라인)
    ├── ddb_store.py         ← DynamoDB 유틸리티 모듈
    ├── prepare_ddb.ipynb    ← conf/data → DDB 등록
    ├── load_from_ddb.ipynb  ← DDB → 로컬 conf/data 생성
    └── modeling_ddb.ipynb   ← DDB 로드 + 학습 + DDB/MLflow 저장
```

---

## DynamoDB 테이블 스키마

**키 구조:**
- **HASH key:** `experiment_id`
- **RANGE key:** `entity_type`

| experiment_id | entity_type | 내용 |
|--------------|-------------|------|
| `EXP#{user_id}#{project}#{experiment}` | `META` | 실험 식별 메타데이터 |
| `EXP#{user_id}#{project}#{experiment}` | `CONF` | env/meta/model YAML → JSON Map |
| `EXP#{user_id}#{project}#{experiment}` | `DATA#{split}` | CSV → base64 |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `MANIFEST` | 실행 컨텍스트 |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `METRICS` | 지표, confusion matrix |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `DATA_REF` | 데이터 출처 참조 |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `CONFIG` | 실행 시 config 스냅샷 |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `MODEL#chunk_{n:03d}` | model.pkl → base64 청킹 |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `CHARTS` | PNG 4개 base64 map |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `EXPLAINABILITY` | feature_impact PNG base64 |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `REPORT` | HTML report 텍스트 |

**바이너리 처리:**
- CSV → base64 (split당 ~80KB, 단일 아이템)
- model.pkl → base64 후 250KB 청킹 (`MODEL#chunk_000`, `MODEL#chunk_001`, ...)
- PNG 차트 4개 → `CHARTS` 단일 아이템 (~336KB)
- HTML report → plain text

---

## 파일별 역할

### `ddb/ddb_store.py`
DynamoDB 전용 유틸리티 클래스 `DDBStore`. 모든 read/write 로직 캡슐화.

키 생성:
- `make_exp_pk(user_id, project, experiment)` → `EXP#...`
- `make_run_pk(user_id, project, experiment, run_id)` → `RUN#...`

### `ddb/prepare_ddb.ipynb`
`prepare_input/conf/`와 `prepare_input/data/`를 읽어 DynamoDB에 등록.
- conf 경로: `../prepare_input/conf/`
- data 경로: `../prepare_input/data/`

### `ddb/load_from_ddb.ipynb`
DynamoDB → 로컬 `ddb/conf/`, `ddb/data/` 생성.
기존 `modeling.ipynb` 실행 전 데이터 준비 용도.

### `ddb/modeling_ddb.ipynb`
DDB에서 직접 로드 → LightGBM 학습 → 결과를 DDB + MLflow에 저장.

**MLflow 저장 항목:**
- `log_params`: 하이퍼파라미터 + 학습 샘플 수 + feature 수
- `log_metric`: valid/train accuracy, precision, recall, f1, auc_roc, log_loss
- `log_artifacts`: 차트 5개 PNG (`charts/` artifact path)
- `lightgbm.log_model`: LightGBM 모델
- `set_tag`: run_id, user_id, project, experiment, algorithm, env, 커스텀 태그

**MLflow run_name 규칙:**
```
{project}__{experiment}__{user_id}__{algorithm}
예: titanic-survival-prediction__tuned-hjsong-v1__hjsong__lightgbm
```

**시스템 메트릭 자동 수집:**
`mlflow.enable_system_metrics_logging()` 호출로 학습 중 CPU 사용률, 메모리,
디스크 I/O가 `system/` prefix로 자동 수집됨 (MLflow >= 2.8 필요).

---

## 실행 순서

```
1단계: ddb/prepare_ddb.ipynb 실행
   → gsretail-mlops-edu-hjsong 테이블에 META/CONF/DATA 등록

2단계: ddb/modeling_ddb.ipynb 실행
   → DDB에서 config/data 로드
   → LightGBM 학습
   → DDB에 MANIFEST/METRICS/MODEL/CHARTS/EXPLAINABILITY/REPORT 저장
   → MLflow에 params/metrics/charts/model 등록
```

선택적 (기존 modeling.ipynb를 DDB 데이터로 실행하고 싶을 때):
```
1단계: ddb/prepare_ddb.ipynb
2단계: ddb/load_from_ddb.ipynb  ← DDB → 로컬 파일
3단계: modeling/modeling.ipynb  ← 기존 파이프라인 그대로
```

---

## 검증 방법

```python
# prepare_ddb 완료 확인
for sk in ['META','CONF','DATA#train','DATA#validation','DATA#test']:
    resp = table.get_item(Key={'experiment_id': EXP_PK, 'entity_type': sk})
    print('OK' if 'Item' in resp else 'MISSING', sk)

# modeling_ddb 완료 확인
resp = table.query(KeyConditionExpression=Key('experiment_id').eq(RUN_PK))
for item in resp['Items']:
    print(item['entity_type'])

# 모델 로드
model = store.get_model(RUN_PK)
model.predict(X_test)
```
