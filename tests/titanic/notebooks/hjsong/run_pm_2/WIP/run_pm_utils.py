"""
run_pm_utils.py - SageMaker Training 공통 유틸리티

설정 파일 구조:
  env.yml   - 인프라/환경  (S3 버킷, Athena, 로컬 경로)
  meta.yml  - 실험 식별자  (project, experiment, MLFlow)
  model.yml - 모델 학습    (알고리즘, 하이퍼파라미터, 데이터 기간, 피처)
"""

import os
import io
import boto3
import yaml
import uuid
import getpass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


# ============================================================
# YAML Loaders  (로컬 / S3 모두 지원)
# ============================================================

def load_yaml(path: str) -> dict:
    """로컬 yml 파일 로드"""
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}


def _parse_s3_uri(s3_uri: str):
    """s3://bucket/key  →  (bucket, key)"""
    s3_uri = s3_uri.strip()
    assert s3_uri.startswith("s3://"), f"S3 URI must start with s3://: {s3_uri}"
    parts = s3_uri[5:].split("/", 1)
    bucket = parts[0]
    key    = parts[1] if len(parts) > 1 else ""
    return bucket, key


def load_yaml_from_s3(s3_uri: str) -> dict:
    """S3의 yml 파일을 직접 메모리로 로드"""
    bucket, key = _parse_s3_uri(s3_uri)
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    content = obj['Body'].read().decode('utf-8')
    return yaml.safe_load(content) or {}


def load_yml_auto(path_or_uri: str) -> dict:
    """
    로컬 경로 또는 S3 URI를 자동 판별하여 yml 로드
      - 'S3://' 또는 's3://' 로 시작하면 S3에서 로드
      - 그 외는 로컬 파일로 로드
    """
    if path_or_uri.lower().startswith("s3://"):
        print(f"  [yml] S3 로드: {path_or_uri}")
        return load_yaml_from_s3(path_or_uri)
    else:
        print(f"  [yml] 로컬 로드: {path_or_uri}")
        return load_yaml(path_or_uri)


# ============================================================
# Configuration Classes
# ============================================================

class InputConfig:
    """Input S3 설정 (데이터셋) — env.yml + meta.yml 기반"""

    _DEFAULTS = {
        "region":  "ap-northeast-2",
        "env":     "dev",
        "project": "gs25-sales-forecast",
        "version": "v1.0",
    }

    def __init__(self, env: dict = None, meta: dict = None):
        e   = env  or {}
        m   = meta or {}
        inp = e.get("input", {})

        self.region  = inp.get("region",  self._DEFAULTS["region"])
        self.env     = e.get("env",       self._DEFAULTS["env"])
        self.bucket  = inp.get("bucket",  f"gs-retail-awesome-data-{self.region}")
        self.project = m.get("project",   self._DEFAULTS["project"])
        self.version = m.get("version",   self._DEFAULTS["version"])
        self.user_id = getpass.getuser()

    def get_s3_prefix(self, user_id=None, version=None, project=None, env=None):
        return (
            f"env={env or self.env}/"
            f"user={user_id or self.user_id}/"
            f"project={project or self.project}/"
            f"version={version or self.version}"
        )

    def get_s3_uri(self, **kwargs):
        return f"s3://{self.bucket}/{self.get_s3_prefix(**kwargs)}/"


class OutputConfig:
    """Output S3 설정 (모델 결과물) — env.yml + meta.yml + model.yml 기반"""

    _DEFAULTS = {
        "region":     "ap-northeast-2",
        "env":        "dev",
        "project":    "gs25-sales-forecast",
        "experiment": "baseline-v1",
        "model":      "store-daily-sales-forecast",
        "algo":       "lightgbm",
    }

    def __init__(self, env: dict = None, meta: dict = None, model_cfg: dict = None):
        e   = env       or {}
        m   = meta      or {}
        mc  = model_cfg or {}
        out = e.get("output", {})
        alg = mc.get("algorithm", {})

        self.region     = out.get("region",    self._DEFAULTS["region"])
        self.env        = e.get("env",          self._DEFAULTS["env"])
        self.bucket     = out.get("bucket",     f"gs-retail-awesome-model-{self.region}")
        self.project    = m.get("project",      self._DEFAULTS["project"])
        self.experiment = m.get("experiment",   self._DEFAULTS["experiment"])
        self.model      = m.get("model",        self._DEFAULTS["model"])
        self.algo       = alg.get("name",       self._DEFAULTS["algo"])
        self.user_id    = getpass.getuser()

    @staticmethod
    def generate_run_id() -> str:
        return datetime.utcnow().strftime(f"%Y%m%dT%H%M%SZ_{uuid.uuid4().hex[:8]}")

    @staticmethod
    def get_run_date() -> str:
        return datetime.utcnow().strftime("%Y-%m-%d")

    def get_s3_prefix(
        self,
        user_id=None, project=None, experiment=None,
        model=None, algo=None, run_date=None, run_id=None, env=None,
    ):
        return (
            f"env={env or self.env}/"
            f"user={user_id or self.user_id}/"
            f"project={project or self.project}/"
            f"experiment={experiment or self.experiment}/"
            f"model={model or self.model}/"
            f"algo={algo or self.algo}/"
            f"run_date={run_date or self.get_run_date()}/"
            f"run_id={run_id or self.generate_run_id()}"
        )

    def get_s3_uri(self, **kwargs):
        return f"s3://{self.bucket}/{self.get_s3_prefix(**kwargs)}/"


class LocalConfig:
    """로컬 디렉토리 설정 — env.yml 기반"""

    _DEFAULTS = {
        "root":       "run_pm",
        "input_dir":  "input",
        "output_dir": "output",
        "output_subdirs": [
            "metadata", "config", "data_refs",
            "artifacts/model", "artifacts/metrics",
            "artifacts/charts", "artifacts/explainability",
            "reports",
        ],
    }

    def __init__(self, env: dict = None):
        c = (env or {}).get("local", {})
        self.root           = c.get("root",           self._DEFAULTS["root"])
        self.input_dir      = c.get("input_dir",      self._DEFAULTS["input_dir"])
        self.output_dir     = c.get("output_dir",     self._DEFAULTS["output_dir"])
        self.output_subdirs = c.get("output_subdirs", self._DEFAULTS["output_subdirs"])


# ============================================================
# S3 Functions
# ============================================================

def ensure_bucket_exists(bucket: str, region: str = "ap-northeast-2") -> bool:
    """S3 버킷이 없으면 생성"""
    s3_client = boto3.client('s3', region_name=region)
    try:
        s3_client.head_bucket(Bucket=bucket)
        print(f"✓ Bucket exists: {bucket}")
        return True
    except s3_client.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code in ['404', 'NoSuchBucket']:
            print(f"⚠ Bucket not found, creating: {bucket}")
            try:
                if region == 'us-east-1':
                    s3_client.create_bucket(Bucket=bucket)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                print(f"✓ Bucket created: {bucket} (region: {region})")
                return True
            except Exception as create_error:
                print(f"✗ Failed to create bucket: {create_error}")
                return False
        else:
            print(f"✗ Bucket access error: {e}")
            return False


def download_from_s3(bucket: str, s3_prefix: str, local_dir: str, dry_run: bool = False) -> List[str]:
    """S3 prefix 하위 전체를 로컬로 다운로드"""
    s3_client = boto3.client('s3')
    downloaded = []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix):
            for obj in page.get('Contents', []):
                s3_key      = obj['Key']
                rel_path    = s3_key[len(s3_prefix):].lstrip('/')
                local_path  = os.path.join(local_dir, rel_path)
                if dry_run:
                    print(f"  [DRY RUN] s3://{bucket}/{s3_key} -> {local_path}")
                else:
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    s3_client.download_file(bucket, s3_key, local_path)
                    print(f"  ✓ Downloaded: {local_path}")
                downloaded.append(local_path)
    except Exception as e:
        print(f"✗ Download error: {e}")
    return downloaded


def upload_to_s3(local_path: str, bucket: str, s3_key: str, dry_run: bool = False) -> str:
    """단일 파일 S3 업로드"""
    s3_uri = f"s3://{bucket}/{s3_key}"
    if dry_run:
        print(f"  [DRY RUN] {local_path} -> {s3_uri}")
    else:
        boto3.client('s3').upload_file(local_path, bucket, s3_key)
        print(f"  ✓ Uploaded: {s3_uri}")
    return s3_uri


def upload_directory_tree(local_base: str, bucket: str, s3_prefix: str, dry_run: bool = False) -> List[str]:
    """디렉토리 트리 전체를 S3에 업로드"""
    uploaded = []
    for root, dirs, files in os.walk(local_base):
        for filename in files:
            local_path   = os.path.join(root, filename)
            relative     = os.path.relpath(local_path, local_base)
            s3_key       = f"{s3_prefix}/{relative}"
            uri          = upload_to_s3(local_path, bucket, s3_key, dry_run)
            uploaded.append(uri)
    return uploaded


# ============================================================
# File Functions
# ============================================================

def create_file(filepath: str, content=None, file_type: str = "yaml") -> str:
    """파일 생성"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    if file_type == "touch" or content is None:
        Path(filepath).touch()
    elif file_type == "yaml":
        with open(filepath, 'w', encoding='utf-8') as f:
            yaml.dump(content, f, default_flow_style=False, allow_unicode=True)
    elif file_type == "json":
        import json
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(content, f, indent=2, ensure_ascii=False)
    else:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(str(content))
    return filepath


# ============================================================
# Print Helpers
# ============================================================

def print_header(title: str, width: int = 60):
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)


def print_separator(width: int = 60):
    print("-" * width)
