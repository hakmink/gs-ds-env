# DynamoDB 마이그레이션 플랜: hjsong ML Pipeline

## 개요

기존 S3 기반 파이프라인을 DynamoDB 기반으로 전환한다. 기존 파일은 수정하지 않고 새 파일을 추가로 생성한다.

**DynamoDB 테이블:** `gsretail-mlops-edu-hjsong`
**Billing:** PAY_PER_REQUEST (테이블이 없으면 자동 생성)

---

## 기존 파일 vs 신규 파일

| 기존 (S3) | 신규 (DynamoDB) | 역할 |
|-----------|----------------|------|
| `prepare_input/prepare.ipynb` | `prepare_input/prepare_ddb.ipynb` | conf + data → DDB 등록 |
| `modeling/load_conf_datasets.ipynb` | `modeling/load_from_ddb.ipynb` | DDB → 로컬 conf/ data/ 생성 |
| `modeling/modeling.ipynb` | `modeling/modeling_ddb.ipynb` | DDB 로드 + 학습 + 결과 DDB 저장 |
| _(없음)_ | `modeling/ddb_store.py` | DynamoDB 유틸리티 모듈 |

---

## DynamoDB 테이블 스키마

### 키 구조

| PK | SK | entity_type | 내용 |
|----|-----|-------------|------|
| `EXP#{user_id}#{project}#{experiment}` | `META` | experiment_meta | 실험 식별 메타 |
| `EXP#{user_id}#{project}#{experiment}` | `CONF` | experiment_conf | env/meta/model YAML → Map |
| `EXP#{user_id}#{project}#{experiment}` | `DATA#{split}` | dataset_split | CSV → base64 |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `MANIFEST` | run_manifest | 실행 컨텍스트 |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `METRICS` | run_metrics | 지표, confusion matrix |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `DATA_REF` | run_data_ref | 데이터 출처 참조 |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `CONFIG` | run_config_snapshot | 실행 시 config 스냅샷 |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `MODEL#chunk_{n:03d}` | model_chunk | model.pkl → base64 청킹 |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `CHARTS` | run_charts | PNG 4개 base64 map |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `EXPLAINABILITY` | run_explainability | feature_impact PNG base64 |
| `RUN#{user_id}#{project}#{experiment}#{run_id}` | `REPORT` | run_report | HTML report 텍스트 |

**GSI:** `by-experiment` — GSI PK=`experiment_key` (RUN 아이템에 부여), 실험별 run 조회용

### 바이너리 처리 전략

| 아티팩트 | 크기 | 처리 방법 |
|---------|------|----------|
| CSV 데이터 (split당) | ~60-80KB base64 | 단일 DDB item에 base64 저장 |
| LightGBM model.pkl | ~200-400KB | base64 후 250KB씩 청킹 → `MODEL#chunk_000`, `MODEL#chunk_001` |
| PNG 차트 4개 합산 | ~300-400KB base64 | `CHARTS` 단일 item에 묶음 |
| explainability PNG | ~60KB base64 | `EXPLAINABILITY` 별도 item |
| HTML report | ~4KB | plain text 그대로 저장 |

---

## 실행 순서

```
# 1단계: 데이터 등록 (prepare_input/)
prepare_ddb.ipynb 실행
→ DDB에 META, CONF, DATA#train, DATA#validation, DATA#test 생성

# 2단계 옵션 A: DDB → 로컬 파일화 후 기존 modeling.ipynb 실행
load_from_ddb.ipynb 실행  (conf/, data/ 로컬 생성)
modeling.ipynb 실행        (기존 그대로)

# 2단계 옵션 B: DDB만 사용 (권장)
modeling_ddb.ipynb 실행
→ DDB에서 데이터 로드 → 학습 → 결과 DDB 저장
```

---

## 핵심 파일 위치

```
samples/hjsong/
├── PLAN_DDB_MIGRATION.md          ← 이 파일
├── prepare_input/
│   ├── prepare.ipynb              (기존, S3)
│   └── prepare_ddb.ipynb          (신규, DDB)
└── modeling/
    ├── ddb_store.py               (신규, DDB 유틸리티)
    ├── load_conf_datasets.ipynb   (기존, S3)
    ├── load_from_ddb.ipynb        (신규, DDB)
    ├── modeling.ipynb             (기존, S3)
    └── modeling_ddb.ipynb         (신규, DDB)
```

---

## 검증 방법

- `prepare_ddb.ipynb` 마지막 셀: 5개 SK 존재 확인 (META, CONF, DATA#train, DATA#validation, DATA#test)
- `modeling_ddb.ipynb` 마지막 셀: RUN PK로 query → 전체 SK 목록 + 크기 출력
- 모델 로드 테스트: `store.get_model(RUN_PK).predict(X_test)`
