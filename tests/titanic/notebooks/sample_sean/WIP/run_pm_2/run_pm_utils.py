"""
run_pm_utils.py - SageMaker Training 공통 유틸리티

공통 Config, S3 함수, 파일 함수 등을 제공합니다.
"""

import os
import boto3
import yaml
import uuid
import getpass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


# ============================================================
# Configuration Classes
# ============================================================

class InputConfig:
    """Input S3 설정 (데이터셋)"""
    
    REGION = "ap-northeast-2"
    BUCKET = f"gs-retail-awesome-data-{REGION}"
    ENV = "dev"
    USER_ID = getpass.getuser()
    PROJECT = "gs25-sales-forecast"
    VERSION = "v1.0"
    
    @classmethod
    def get_s3_prefix(cls, env=None, user_id=None, project=None, version=None):
        return (
            f"env={env or cls.ENV}/"
            f"user={user_id or cls.USER_ID}/"
            f"project={project or cls.PROJECT}/"
            f"version={version or cls.VERSION}"
        )
    
    @classmethod
    def get_s3_uri(cls, **kwargs):
        prefix = cls.get_s3_prefix(**kwargs)
        return f"s3://{cls.BUCKET}/{prefix}/"


class OutputConfig:
    """Output S3 설정 (모델 결과물)"""
    
    REGION = "ap-northeast-2"
    BUCKET = f"gs-retail-awesome-model-{REGION}"
    ENV = "dev"
    USER_ID = getpass.getuser()
    PROJECT = "gs25-sales-forecast"
    EXPERIMENT = "baseline-v1"
    MODEL = "store-daily-sales-forecast"
    ALGO = "lightgbm"
    
    @classmethod
    def generate_run_id(cls):
        now = datetime.utcnow()
        short_uuid = uuid.uuid4().hex[:8]
        return now.strftime(f"%Y%m%dT%H%M%SZ_{short_uuid}")
    
    @classmethod
    def get_run_date(cls):
        return datetime.utcnow().strftime("%Y-%m-%d")
    
    @classmethod
    def get_s3_prefix(cls, env=None, user_id=None, project=None, experiment=None,
                      model=None, algo=None, run_date=None, run_id=None):
        return (
            f"env={env or cls.ENV}/"
            f"user={user_id or cls.USER_ID}/"
            f"project={project or cls.PROJECT}/"
            f"experiment={experiment or cls.EXPERIMENT}/"
            f"model={model or cls.MODEL}/"
            f"algo={algo or cls.ALGO}/"
            f"run_date={run_date or cls.get_run_date()}/"
            f"run_id={run_id or cls.generate_run_id()}"
        )
    
    @classmethod
    def get_s3_uri(cls, **kwargs):
        prefix = cls.get_s3_prefix(**kwargs)
        return f"s3://{cls.BUCKET}/{prefix}/"


class LocalConfig:
    """로컬 디렉토리 설정"""
    
    ROOT = "run_pm"
    INPUT_DIR = "input"
    OUTPUT_DIR = "output"
    
    OUTPUT_SUBDIRS = [
        "metadata",
        "config",
        "data_refs",
        "artifacts/model",
        "artifacts/metrics",
        "artifacts/charts",
        "artifacts/explainability",
        "reports"
    ]


# ============================================================
# S3 Functions
# ============================================================

def ensure_bucket_exists(bucket: str, region: str = None) -> bool:
    """S3 버킷이 없으면 생성"""
    region = region or OutputConfig.REGION
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
    """S3에서 로컬로 다운로드"""
    s3_client = boto3.client('s3')
    downloaded = []
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=s3_prefix)
        
        for page in pages:
            for obj in page.get('Contents', []):
                s3_key = obj['Key']
                relative_path = s3_key[len(s3_prefix):].lstrip('/')
                local_path = os.path.join(local_dir, relative_path)
                
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
        s3_client = boto3.client('s3')
        s3_client.upload_file(local_path, bucket, s3_key)
        print(f"  ✓ Uploaded: {s3_uri}")
    
    return s3_uri


def upload_directory_tree(local_base: str, bucket: str, s3_prefix: str, dry_run: bool = False) -> List[str]:
    """디렉토리 트리 전체를 S3에 업로드"""
    uploaded = []
    
    for root, dirs, files in os.walk(local_base):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, local_base)
            s3_key = f"{s3_prefix}/{relative_path}"
            
            uri = upload_to_s3(local_path, bucket, s3_key, dry_run)
            uploaded.append(uri)
    
    return uploaded


# ============================================================
# File Functions
# ============================================================

def create_directories(base_path: str, subdirs: List[str]) -> Dict[str, str]:
    """디렉토리 구조 생성"""
    paths = {'base': base_path}
    os.makedirs(base_path, exist_ok=True)
    
    for subdir in subdirs:
        full_path = os.path.join(base_path, subdir)
        os.makedirs(full_path, exist_ok=True)
        paths[subdir] = full_path
    
    return paths


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
    """헤더 출력"""
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)


def print_separator(width: int = 60):
    """구분선 출력"""
    print("-" * width)