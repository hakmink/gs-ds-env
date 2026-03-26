"""
DynamoDB 전용 스토어 유틸리티
테이블: gsretail-mlops-edu-hjsong

PK/SK 구조:
  EXP#{user_id}#{project}#{experiment}  |  META / CONF / DATA#{split}
  RUN#{user_id}#{project}#{experiment}#{run_id}  |  MANIFEST / METRICS / DATA_REF /
                                                     CONFIG / MODEL#chunk_{n} /
                                                     CHARTS / EXPLAINABILITY / REPORT
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
    """float/int를 DynamoDB 호환 Decimal로 재귀 변환"""
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
    """Decimal을 float로 재귀 변환 (DynamoDB 로드 후 파이썬 표준 타입으로)"""
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
            print(f"   ✅ 테이블 '{TABLE_NAME}' 존재 확인")
            return True
        except self.client.exceptions.ResourceNotFoundException:
            print(f"   🆕 테이블 '{TABLE_NAME}' 생성 중...")
            self.client.create_table(
                TableName=TABLE_NAME,
                KeySchema=[
                    {"AttributeName": "PK", "KeyType": "HASH"},
                    {"AttributeName": "SK", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "PK", "AttributeType": "S"},
                    {"AttributeName": "SK", "AttributeType": "S"},
                    {"AttributeName": "experiment_key", "AttributeType": "S"},
                ],
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "by-experiment",
                        "KeySchema": [
                            {"AttributeName": "experiment_key", "KeyType": "HASH"},
                            {"AttributeName": "SK", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = self.client.get_waiter("table_exists")
            waiter.wait(TableName=TABLE_NAME)
            print(f"   ✅ 테이블 '{TABLE_NAME}' 생성 완료")
            return True

    # ── 실험 데이터 쓰기 ──────────────────────────────────────────────────────

    def put_experiment_meta(self, exp_pk: str, user_id: str, project: str,
                            experiment: str, version: str, env: str, region: str):
        """EXP#...#META: 실험 식별 메타데이터"""
        now = datetime.utcnow().isoformat()
        self.table.put_item(Item={
            "PK": exp_pk, "SK": "META",
            "entity_type": "experiment_meta",
            "user_id": user_id, "project": project,
            "experiment": experiment, "version": version,
            "env": env, "region": region,
            "created_at": now, "updated_at": now,
            "status": "active",
        })

    def put_experiment_conf(self, exp_pk: str, env_config: dict,
                            meta_config: dict, model_config: dict):
        """EXP#...#CONF: 3개 YAML을 JSON Map으로 저장"""
        now = datetime.utcnow().isoformat()
        self.table.put_item(Item=_to_ddb({
            "PK": exp_pk, "SK": "CONF",
            "entity_type": "experiment_conf",
            "env_yml": env_config,
            "meta_yml": meta_config,
            "model_yml": model_config,
            "uploaded_at": now,
            "uploaded_by": meta_config.get("user_id", ""),
        }))

    def put_dataset_split(self, exp_pk: str, split: str, version: str,
                          csv_bytes: bytes, row_count: int):
        """EXP#...#DATA#{split}: CSV를 base64로 인코딩해서 저장"""
        import hashlib
        now = datetime.utcnow().isoformat()
        csv_b64 = base64.b64encode(csv_bytes).decode("utf-8")
        checksum = "sha256:" + hashlib.sha256(csv_bytes).hexdigest()[:16] + "..."
        self.table.put_item(Item={
            "PK": exp_pk, "SK": f"DATA#{split}",
            "entity_type": "dataset_split",
            "split": split, "version": version,
            "row_count": row_count,
            "size_bytes": len(csv_bytes),
            "checksum": checksum,
            "csv_b64": csv_b64,
            "uploaded_at": now,
        })

    # ── 실험 데이터 읽기 ──────────────────────────────────────────────────────

    def get_experiment_conf(self, exp_pk: str) -> dict:
        """CONF 아이템 조회 → {env_yml, meta_yml, model_yml, ...}"""
        resp = self.table.get_item(Key={"PK": exp_pk, "SK": "CONF"})
        if "Item" not in resp:
            raise KeyError(f"CONF not found: PK={exp_pk}")
        return _from_ddb(resp["Item"])

    def get_dataset_split(self, exp_pk: str, split: str) -> pd.DataFrame:
        """DATA#{split} 아이템 → pd.DataFrame"""
        resp = self.table.get_item(Key={"PK": exp_pk, "SK": f"DATA#{split}"})
        if "Item" not in resp:
            raise KeyError(f"DATA#{split} not found: PK={exp_pk}")
        csv_bytes = base64.b64decode(resp["Item"]["csv_b64"])
        return pd.read_csv(io.BytesIO(csv_bytes))

    # ── Run 결과 쓰기 ─────────────────────────────────────────────────────────

    def put_run_config_snapshot(self, run_pk: str, exp_pk: str,
                                env_config: dict, meta_config: dict, model_config: dict):
        """RUN#...#CONFIG: 실행 시점 config 스냅샷"""
        self.table.put_item(Item=_to_ddb({
            "PK": run_pk, "SK": "CONFIG",
            "entity_type": "run_config_snapshot",
            "experiment_key": exp_pk,
            "env_yml": env_config,
            "meta_yml": meta_config,
            "model_yml": model_config,
            "saved_at": datetime.utcnow().isoformat(),
        }))

    def put_run_metrics(self, run_pk: str, exp_pk: str, metrics: dict):
        """RUN#...#METRICS: 학습 지표, confusion matrix, feature importance"""
        self.table.put_item(Item=_to_ddb({
            "PK": run_pk, "SK": "METRICS",
            "entity_type": "run_metrics",
            "experiment_key": exp_pk,
            **metrics,
        }))

    def put_run_data_ref(self, run_pk: str, exp_pk: str, ref: dict):
        """RUN#...#DATA_REF: 데이터 출처 참조"""
        self.table.put_item(Item=_to_ddb({
            "PK": run_pk, "SK": "DATA_REF",
            "entity_type": "run_data_ref",
            "experiment_key": exp_pk,
            **ref,
        }))

    def put_run_manifest(self, run_pk: str, exp_pk: str, manifest: dict):
        """RUN#...#MANIFEST: 실행 컨텍스트 및 요약"""
        self.table.put_item(Item=_to_ddb({
            "PK": run_pk, "SK": "MANIFEST",
            "entity_type": "run_manifest",
            "experiment_key": exp_pk,
            **manifest,
        }))

    def put_model_chunked(self, run_pk: str, exp_pk: str, model_obj,
                          algorithm: str, suffix: str) -> int:
        """모델 pickle → base64 → 250KB 청킹 후 저장. 청크 수 반환."""
        pkl_bytes = pickle.dumps(model_obj)
        b64_str = base64.b64encode(pkl_bytes).decode("utf-8")
        total_chunks = math.ceil(len(b64_str) / CHUNK_SIZE)
        now = datetime.utcnow().isoformat()

        for i in range(total_chunks):
            chunk_data = b64_str[i * CHUNK_SIZE: (i + 1) * CHUNK_SIZE]
            self.table.put_item(Item={
                "PK": run_pk, "SK": f"MODEL#chunk_{i:03d}",
                "entity_type": "model_chunk",
                "experiment_key": exp_pk,
                "chunk_index": i,
                "total_chunks": total_chunks,
                "algorithm": algorithm,
                "suffix": suffix,
                "format": "pickle_base64",
                "data": chunk_data,
                "saved_at": now,
            })
        return total_chunks

    def put_charts(self, run_pk: str, exp_pk: str, charts_bytes: dict):
        """
        charts_bytes: {'feature_importance': bytes, 'roc_curve': bytes,
                       'confusion_matrix': bytes, 'learning_curve': bytes,
                       'feature_impact_summary': bytes}
        training 4개 → CHARTS, explainability → EXPLAINABILITY
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
                "PK": run_pk, "SK": "CHARTS",
                "entity_type": "run_charts",
                "experiment_key": exp_pk,
                "charts": training_charts,
                "saved_at": now,
            })

        expl_charts = {
            name: base64.b64encode(bts).decode("utf-8")
            for name, bts in charts_bytes.items()
            if name not in training_names
        }
        if expl_charts:
            self.table.put_item(Item={
                "PK": run_pk, "SK": "EXPLAINABILITY",
                "entity_type": "run_explainability",
                "experiment_key": exp_pk,
                "charts": expl_charts,
                "saved_at": now,
            })

    def put_report(self, run_pk: str, exp_pk: str, html_content: str):
        """RUN#...#REPORT: HTML report 텍스트 저장"""
        self.table.put_item(Item={
            "PK": run_pk, "SK": "REPORT",
            "entity_type": "run_report",
            "experiment_key": exp_pk,
            "format": "html",
            "content": html_content,
            "saved_at": datetime.utcnow().isoformat(),
        })

    # ── Run 결과 읽기 ─────────────────────────────────────────────────────────

    def get_model(self, run_pk: str):
        """청크 아이템들을 순서대로 조합해 model 객체 반환"""
        resp = self.table.query(
            KeyConditionExpression=Key("PK").eq(run_pk) & Key("SK").begins_with("MODEL#chunk_")
        )
        items = sorted(resp["Items"], key=lambda x: int(x["chunk_index"]))
        if not items:
            raise KeyError(f"Model chunks not found: PK={run_pk}")
        b64_str = "".join(item["data"] for item in items)
        return pickle.loads(base64.b64decode(b64_str))

    def get_run_metrics(self, run_pk: str) -> dict:
        resp = self.table.get_item(Key={"PK": run_pk, "SK": "METRICS"})
        if "Item" not in resp:
            raise KeyError(f"METRICS not found: PK={run_pk}")
        return _from_ddb(resp["Item"])

    def get_chart_bytes(self, run_pk: str, chart_name: str) -> bytes:
        """chart_name: 'feature_importance'|'roc_curve'|'confusion_matrix'|
                       'learning_curve'|'feature_impact_summary'"""
        sk = "EXPLAINABILITY" if chart_name == "feature_impact_summary" else "CHARTS"
        resp = self.table.get_item(Key={"PK": run_pk, "SK": sk})
        if "Item" not in resp:
            raise KeyError(f"{sk} not found: PK={run_pk}")
        charts = resp["Item"]["charts"]
        if chart_name not in charts:
            raise KeyError(f"Chart '{chart_name}' not found in {sk}")
        return base64.b64decode(charts[chart_name])

    def get_report_html(self, run_pk: str) -> str:
        resp = self.table.get_item(Key={"PK": run_pk, "SK": "REPORT"})
        if "Item" not in resp:
            raise KeyError(f"REPORT not found: PK={run_pk}")
        return resp["Item"]["content"]

    def list_run_items(self, run_pk: str) -> list:
        """RUN PK 아래의 모든 (SK, entity_type) 반환"""
        resp = self.table.query(
            KeyConditionExpression=Key("PK").eq(run_pk),
            ProjectionExpression="SK, entity_type",
        )
        return [(item["SK"], item.get("entity_type", "")) for item in resp["Items"]]
