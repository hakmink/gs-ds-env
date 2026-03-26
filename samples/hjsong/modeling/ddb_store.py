"""
DynamoDB 전용 스토어 유틸리티
테이블: gsretail-mlops-edu-hjsong

키 구조 (기존 테이블 스키마):
  experiment_id (HASH)  |  entity_type (RANGE)

아이템 패턴:
  experiment_id: EXP#{user_id}#{project}#{experiment}
    entity_type: META / CONF / DATA#train / DATA#validation / DATA#test

  experiment_id: RUN#{user_id}#{project}#{experiment}#{run_id}
    entity_type: MANIFEST / METRICS / DATA_REF / CONFIG /
                 MODEL#chunk_{n} / CHARTS / EXPLAINABILITY / REPORT
"""

import base64
import io
import math
import pickle
from datetime import datetime
from decimal import Decimal

import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

TABLE_NAME = "gsretail-mlops-edu-hjsong"
CHUNK_SIZE = 250_000  # base64 문자 단위 청크 크기 (~187KB raw)


# ── 타입 변환 헬퍼 ──────────────────────────────────────────────────────────

def _to_ddb(obj):
    """float를 DynamoDB 호환 Decimal로 재귀 변환"""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, bool):
        return obj
    elif isinstance(obj, dict):
        return {k: _to_ddb(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_to_ddb(v) for v in obj]
    return obj


def _from_ddb(obj):
    """Decimal을 float/int로 재귀 변환 (DynamoDB 로드 후 파이썬 표준 타입으로)"""
    if isinstance(obj, Decimal):
        f = float(obj)
        return int(f) if f == int(f) else f
    elif isinstance(obj, dict):
        return {k: _from_ddb(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_from_ddb(v) for v in obj]
    return obj


# ── DDBStore 클래스 ────────────────────────────────────────────────────────

class DDBStore:
    """gsretail-mlops-edu-hjsong DynamoDB 테이블 전용 스토어"""

    def __init__(self, region: str = "us-east-1"):
        self.region = region
        self.ddb = boto3.resource("dynamodb", region_name=region)
        self.client = boto3.client("dynamodb", region_name=region)
        self.table = self.ddb.Table(TABLE_NAME)

    # ── 키 생성 ──────────────────────────────────────────────────────────────

    @staticmethod
    def make_exp_pk(user_id: str, project: str, experiment: str) -> str:
        return f"EXP#{user_id}#{project}#{experiment}"

    @staticmethod
    def make_run_pk(user_id: str, project: str, experiment: str, run_id: str) -> str:
        return f"RUN#{user_id}#{project}#{experiment}#{run_id}"

    # ── 테이블 관리 ───────────────────────────────────────────────────────────

    def ensure_table_exists(self):
        """테이블이 없으면 PAY_PER_REQUEST 모드로 생성"""
        try:
            self.client.describe_table(TableName=TABLE_NAME)
            print(f"   테이블 '{TABLE_NAME}' 존재 확인")
            return True
        except self.client.exceptions.ResourceNotFoundException:
            print(f"   테이블 '{TABLE_NAME}' 생성 중...")
            self.client.create_table(
                TableName=TABLE_NAME,
                KeySchema=[
                    {"AttributeName": "experiment_id", "KeyType": "HASH"},
                    {"AttributeName": "entity_type",   "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "experiment_id", "AttributeType": "S"},
                    {"AttributeName": "entity_type",   "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            self.client.get_waiter("table_exists").wait(TableName=TABLE_NAME)
            print(f"   테이블 '{TABLE_NAME}' 생성 완료")
            return True

    # ── 실험 데이터 쓰기 ──────────────────────────────────────────────────────

    def put_experiment_meta(self, exp_pk: str, user_id: str, project: str,
                            experiment: str, version: str, env: str, region: str):
        """experiment_id=EXP#... / entity_type=META"""
        now = datetime.utcnow().isoformat()
        self.table.put_item(Item={
            "experiment_id": exp_pk,
            "entity_type":   "META",
            "user_id":    user_id,
            "project":    project,
            "experiment": experiment,
            "version":    version,
            "env":        env,
            "region":     region,
            "created_at": now,
            "updated_at": now,
            "status":     "active",
        })

    def put_experiment_conf(self, exp_pk: str, env_config: dict,
                            meta_config: dict, model_config: dict):
        """experiment_id=EXP#... / entity_type=CONF"""
        now = datetime.utcnow().isoformat()
        self.table.put_item(Item=_to_ddb({
            "experiment_id": exp_pk,
            "entity_type":   "CONF",
            "env_yml":       env_config,
            "meta_yml":      meta_config,
            "model_yml":     model_config,
            "uploaded_at":   now,
            "uploaded_by":   meta_config.get("user_id", ""),
        }))

    def put_dataset_split(self, exp_pk: str, split: str, version: str,
                          csv_bytes: bytes, row_count: int):
        """experiment_id=EXP#... / entity_type=DATA#{split}"""
        import hashlib
        now = datetime.utcnow().isoformat()
        csv_b64   = base64.b64encode(csv_bytes).decode("utf-8")
        checksum  = "sha256:" + hashlib.sha256(csv_bytes).hexdigest()[:16] + "..."
        self.table.put_item(Item={
            "experiment_id": exp_pk,
            "entity_type":   f"DATA#{split}",
            "split":         split,
            "version":       version,
            "row_count":     row_count,
            "size_bytes":    len(csv_bytes),
            "checksum":      checksum,
            "csv_b64":       csv_b64,
            "uploaded_at":   now,
        })

    # ── 실험 데이터 읽기 ──────────────────────────────────────────────────────

    def get_experiment_conf(self, exp_pk: str) -> dict:
        """CONF 아이템 → {env_yml, meta_yml, model_yml, ...}"""
        resp = self.table.get_item(Key={"experiment_id": exp_pk, "entity_type": "CONF"})
        if "Item" not in resp:
            raise KeyError(f"CONF not found: experiment_id={exp_pk}")
        return _from_ddb(resp["Item"])

    def get_dataset_split(self, exp_pk: str, split: str) -> pd.DataFrame:
        """DATA#{split} 아이템 → pd.DataFrame"""
        resp = self.table.get_item(Key={"experiment_id": exp_pk, "entity_type": f"DATA#{split}"})
        if "Item" not in resp:
            raise KeyError(f"DATA#{split} not found: experiment_id={exp_pk}")
        csv_bytes = base64.b64decode(resp["Item"]["csv_b64"])
        return pd.read_csv(io.BytesIO(csv_bytes))

    # ── Run 결과 쓰기 ─────────────────────────────────────────────────────────

    def put_run_config_snapshot(self, run_pk: str, exp_pk: str,
                                env_config: dict, meta_config: dict, model_config: dict):
        """experiment_id=RUN#... / entity_type=CONFIG"""
        self.table.put_item(Item=_to_ddb({
            "experiment_id":  run_pk,
            "entity_type":    "CONFIG",
            "experiment_key": exp_pk,
            "env_yml":        env_config,
            "meta_yml":       meta_config,
            "model_yml":      model_config,
            "saved_at":       datetime.utcnow().isoformat(),
        }))

    def put_run_metrics(self, run_pk: str, exp_pk: str, metrics: dict):
        """experiment_id=RUN#... / entity_type=METRICS"""
        self.table.put_item(Item=_to_ddb({
            "experiment_id":  run_pk,
            "entity_type":    "METRICS",
            "experiment_key": exp_pk,
            **metrics,
        }))

    def put_run_data_ref(self, run_pk: str, exp_pk: str, ref: dict):
        """experiment_id=RUN#... / entity_type=DATA_REF"""
        self.table.put_item(Item=_to_ddb({
            "experiment_id":  run_pk,
            "entity_type":    "DATA_REF",
            "experiment_key": exp_pk,
            **ref,
        }))

    def put_run_manifest(self, run_pk: str, exp_pk: str, manifest: dict):
        """experiment_id=RUN#... / entity_type=MANIFEST"""
        self.table.put_item(Item=_to_ddb({
            "experiment_id":  run_pk,
            "entity_type":    "MANIFEST",
            "experiment_key": exp_pk,
            **manifest,
        }))

    def put_model_chunked(self, run_pk: str, exp_pk: str, model_obj,
                          algorithm: str, suffix: str) -> int:
        """모델 pickle → base64 → 250KB 청킹 후 저장. 청크 수 반환."""
        pkl_bytes    = pickle.dumps(model_obj)
        b64_str      = base64.b64encode(pkl_bytes).decode("utf-8")
        total_chunks = math.ceil(len(b64_str) / CHUNK_SIZE)
        now          = datetime.utcnow().isoformat()

        for i in range(total_chunks):
            chunk_data = b64_str[i * CHUNK_SIZE: (i + 1) * CHUNK_SIZE]
            self.table.put_item(Item={
                "experiment_id":  run_pk,
                "entity_type":    f"MODEL#chunk_{i:03d}",
                "experiment_key": exp_pk,
                "chunk_index":    i,
                "total_chunks":   total_chunks,
                "algorithm":      algorithm,
                "suffix":         suffix,
                "format":         "pickle_base64",
                "data":           chunk_data,
                "saved_at":       now,
            })
        return total_chunks

    def put_charts(self, run_pk: str, exp_pk: str, charts_bytes: dict):
        """
        charts_bytes: {'feature_importance': bytes, 'roc_curve': bytes,
                       'confusion_matrix': bytes, 'learning_curve': bytes,
                       'feature_impact_summary': bytes}
        training 4개 → entity_type=CHARTS
        explainability → entity_type=EXPLAINABILITY
        """
        training_names = {"feature_importance", "roc_curve", "confusion_matrix", "learning_curve"}
        now = datetime.utcnow().isoformat()

        training_charts = {
            name: base64.b64encode(bts).decode("utf-8")
            for name, bts in charts_bytes.items()
            if name in training_names
        }
        if training_charts:
            self.table.put_item(Item={
                "experiment_id":  run_pk,
                "entity_type":    "CHARTS",
                "experiment_key": exp_pk,
                "charts":         training_charts,
                "saved_at":       now,
            })

        expl_charts = {
            name: base64.b64encode(bts).decode("utf-8")
            for name, bts in charts_bytes.items()
            if name not in training_names
        }
        if expl_charts:
            self.table.put_item(Item={
                "experiment_id":  run_pk,
                "entity_type":    "EXPLAINABILITY",
                "experiment_key": exp_pk,
                "charts":         expl_charts,
                "saved_at":       now,
            })

    def put_report(self, run_pk: str, exp_pk: str, html_content: str):
        """experiment_id=RUN#... / entity_type=REPORT"""
        self.table.put_item(Item={
            "experiment_id":  run_pk,
            "entity_type":    "REPORT",
            "experiment_key": exp_pk,
            "format":         "html",
            "content":        html_content,
            "saved_at":       datetime.utcnow().isoformat(),
        })

    # ── Run 결과 읽기 ─────────────────────────────────────────────────────────

    def get_model(self, run_pk: str):
        """청크 아이템들을 순서대로 조합해 model 객체 반환"""
        resp  = self.table.query(
            KeyConditionExpression=Key("experiment_id").eq(run_pk) & Key("entity_type").begins_with("MODEL#chunk_")
        )
        items = sorted(resp["Items"], key=lambda x: int(x["chunk_index"]))
        if not items:
            raise KeyError(f"Model chunks not found: experiment_id={run_pk}")
        b64_str = "".join(item["data"] for item in items)
        return pickle.loads(base64.b64decode(b64_str))

    def get_run_metrics(self, run_pk: str) -> dict:
        resp = self.table.get_item(Key={"experiment_id": run_pk, "entity_type": "METRICS"})
        if "Item" not in resp:
            raise KeyError(f"METRICS not found: experiment_id={run_pk}")
        return _from_ddb(resp["Item"])

    def get_chart_bytes(self, run_pk: str, chart_name: str) -> bytes:
        """chart_name: 'feature_importance'|'roc_curve'|'confusion_matrix'|
                       'learning_curve'|'feature_impact_summary'"""
        et   = "EXPLAINABILITY" if chart_name == "feature_impact_summary" else "CHARTS"
        resp = self.table.get_item(Key={"experiment_id": run_pk, "entity_type": et})
        if "Item" not in resp:
            raise KeyError(f"{et} not found: experiment_id={run_pk}")
        charts = resp["Item"]["charts"]
        if chart_name not in charts:
            raise KeyError(f"Chart '{chart_name}' not found in {et}")
        return base64.b64decode(charts[chart_name])

    def get_report_html(self, run_pk: str) -> str:
        resp = self.table.get_item(Key={"experiment_id": run_pk, "entity_type": "REPORT"})
        if "Item" not in resp:
            raise KeyError(f"REPORT not found: experiment_id={run_pk}")
        return resp["Item"]["content"]

    def list_run_items(self, run_pk: str) -> list:
        """RUN experiment_id 아래의 모든 entity_type 목록 반환"""
        resp = self.table.query(
            KeyConditionExpression=Key("experiment_id").eq(run_pk),
            ProjectionExpression="entity_type",
        )
        return [item["entity_type"] for item in resp["Items"]]
