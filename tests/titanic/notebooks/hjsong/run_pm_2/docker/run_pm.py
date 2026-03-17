#!/usr/bin/env python3
"""
run_pm.py - GS Retail ML Pipeline Runner

S3 conf 경로에서 모든 파일을 가져와 모델링을 실행하고 결과를 S3에 저장합니다.
- yml 파일들 → conf/ 폴더
- 나머지 파일들 (py, ipynb 등) → 현재 작업 폴더

Usage:
    python run_pm.py --conf-s3-path s3://bucket/path/to/conf/

Example:
    python run_pm.py --conf-s3-path s3://gs-retail-awesome-conf-us-east-1/dev/sean/titanic-survival-prediction/baseline-v1/ --dry-run
"""

import os
import sys
import yaml
import boto3
import shutil
import logging
import argparse
import papermill as pm
from pathlib import Path
from datetime import datetime
from uuid import uuid4
from urllib.parse import urlparse
from botocore.exceptions import ClientError

# ============================================================
# Logging 설정
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================
# 유틸리티 함수
# ============================================================
def parse_s3_uri(s3_uri: str) -> tuple:
    """S3 URI를 bucket과 key로 파싱"""
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    return bucket, key


def load_yaml(filepath: Path) -> dict:
    """YAML 파일 로드"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def ensure_dir(path: Path):
    """디렉토리 생성 (존재하면 스킵)"""
    path.mkdir(parents=True, exist_ok=True)


def is_yaml_file(filename: str) -> bool:
    """YAML 파일 여부 확인"""
    return filename.endswith('.yml') or filename.endswith('.yaml')


# ============================================================
# S3 Helper 클래스
# ============================================================
class S3Helper:
    """S3 작업을 위한 헬퍼 클래스"""
    
    def __init__(self, region: str = None):
        self.region = region
        self.client = boto3.client('s3', region_name=region)
    
    def download_file(self, s3_uri: str, local_path: Path) -> bool:
        """S3에서 파일 다운로드"""
        bucket, key = parse_s3_uri(s3_uri)
        try:
            ensure_dir(local_path.parent)
            self.client.download_file(bucket, key, str(local_path))
            logger.info(f"    ✅ {key.split('/')[-1]} -> {local_path}")
            return True
        except ClientError as e:
            logger.error(f"    ❌ Failed to download {s3_uri}: {e}")
            return False
    
    def list_objects(self, s3_uri: str) -> list:
        """S3 prefix 아래 모든 객체 목록 조회"""
        bucket, prefix = parse_s3_uri(s3_uri)
        if not prefix.endswith('/'):
            prefix += '/'
        
        objects = []
        paginator = self.client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                key = obj['Key']
                # prefix 자체는 스킵
                if key == prefix or key.endswith('/'):
                    continue
                
                filename = key.split('/')[-1]
                objects.append({
                    'key': key,
                    'filename': filename,
                    's3_uri': f"s3://{bucket}/{key}",
                    'size': obj['Size']
                })
        
        return objects
    
    def download_prefix(self, s3_uri: str, local_dir: Path) -> list:
        """S3 prefix 아래 모든 파일 다운로드 (하위 폴더 구조 유지)"""
        bucket, prefix = parse_s3_uri(s3_uri)
        if not prefix.endswith('/'):
            prefix += '/'
        
        downloaded = []
        objects = self.list_objects(s3_uri)
        
        for obj in objects:
            key = obj['key']
            relative_path = key[len(prefix):]
            local_path = local_dir / relative_path
            
            if self.download_file(obj['s3_uri'], local_path):
                downloaded.append({
                    'filename': obj['filename'],
                    'local_path': local_path,
                    's3_uri': obj['s3_uri']
                })
        
        return downloaded
    
    def upload_file(self, local_path: Path, s3_uri: str) -> bool:
        """파일을 S3에 업로드"""
        bucket, key = parse_s3_uri(s3_uri)
        try:
            self.client.upload_file(str(local_path), bucket, key)
            logger.info(f"    ✅ {local_path.name} -> {s3_uri}")
            return True
        except ClientError as e:
            logger.error(f"    ❌ Failed to upload {local_path}: {e}")
            return False
    
    def upload_directory(self, local_dir: Path, s3_uri: str) -> list:
        """디렉토리 전체를 S3에 업로드"""
        bucket, prefix = parse_s3_uri(s3_uri)
        if not prefix.endswith('/'):
            prefix += '/'
        
        uploaded = []
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                local_path = Path(root) / file
                relative_path = local_path.relative_to(local_dir)
                s3_key = prefix + str(relative_path).replace('\\', '/')
                s3_file_uri = f"s3://{bucket}/{s3_key}"
                
                if self.upload_file(local_path, s3_file_uri):
                    uploaded.append(s3_file_uri)
        
        return uploaded
    
    def ensure_bucket_exists(self, bucket_name: str) -> bool:
        """버킷이 없으면 생성"""
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.info(f"  🆕 Creating bucket: {bucket_name}")
                try:
                    if self.region == 'us-east-1':
                        self.client.create_bucket(Bucket=bucket_name)
                    else:
                        self.client.create_bucket(
                            Bucket=bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.region}
                        )
                    return True
                except ClientError as create_error:
                    logger.error(f"  ❌ Failed to create bucket: {create_error}")
                    return False
            elif error_code == '403':
                logger.warning(f"  ⚠️  Bucket exists but no access: {bucket_name}")
                return True
            else:
                raise e


# ============================================================
# Pipeline Runner 클래스
# ============================================================
class PipelineRunner:
    """ML Pipeline Runner"""
    
    def __init__(self, conf_s3_path, work_dir=None, notebook_path=None):
        self.conf_s3_path = conf_s3_path.rstrip('/')
        self.work_dir = work_dir or Path.cwd() 
        self.notebook_path = Path(notebook_path) if notebook_path else None
        
        # 로컬 디렉토리 구조
        self.conf_dir = self.work_dir / 'conf'
        self.data_dir = self.work_dir 
        self.output_dir = self.work_dir / 'output'
        
        # 설정 및 S3 헬퍼 (나중에 초기화)
        self.env_config = None
        self.meta_config = None
        self.model_config = None
        self.s3 = None
        self.run_id = None
        
        # 다운로드된 실행 파일들
        self.notebooks = []
        self.scripts = []


    
    def setup_directories(self):
        """로컬 작업 디렉토리 생성"""
        logger.info("📁 Setting up local directories...")
        
        for dir_path in [self.conf_dir, self.data_dir, self.output_dir]:
            ensure_dir(dir_path)
            logger.info(f"    📁 {dir_path}")
    
    def download_conf_files(self):
        """
        S3 conf 경로에서 모든 파일 다운로드
        - yml 파일 → conf/ 폴더
        - 그 외 파일 → work_dir (현재 작업 폴더)
        """
        logger.info("📥 Downloading files from conf S3 path...")
        logger.info(f"    Source: {self.conf_s3_path}")
        
        # 임시 S3 헬퍼 (region 모름)
        temp_s3 = S3Helper()
        
        # S3에서 파일 목록 조회
        objects = temp_s3.list_objects(self.conf_s3_path)
        
        if not objects:
            raise RuntimeError(f"No files found in {self.conf_s3_path}")
        
        logger.info(f"    Found {len(objects)} files")
        
        yml_files = []
        other_files = []
        
        for obj in objects:
            filename = obj['filename']
            s3_uri = obj['s3_uri']
            
            if is_yaml_file(filename):
                # yml 파일 → conf/ 폴더
                local_path = self.conf_dir / filename
                yml_files.append((s3_uri, local_path, filename))
            else:
                # 그 외 파일 → work_dir
                local_path = self.work_dir / filename
                other_files.append((s3_uri, local_path, filename))
        
        # yml 파일 다운로드
        logger.info(f"\n  📂 YAML files → conf/")
        for s3_uri, local_path, filename in yml_files:
            if not temp_s3.download_file(s3_uri, local_path):
                raise RuntimeError(f"Failed to download {filename}")
        
        # 그 외 파일 다운로드
        if other_files:
            logger.info(f"\n  📂 Other files → work_dir/")
            for s3_uri, local_path, filename in other_files:
                temp_s3.download_file(s3_uri, local_path)
                
                # 파일 유형별 분류
                if filename.endswith('.ipynb'):
                    self.notebooks.append(local_path)
                elif filename.endswith('.py'):
                    self.scripts.append(local_path)
        
        # 필수 설정 파일 확인 및 로드
        required_configs = ['env.yml', 'meta.yml', 'model.yml']
        for config_file in required_configs:
            config_path = self.conf_dir / config_file
            if not config_path.exists():
                raise RuntimeError(f"Required config file not found: {config_file}")
        
        # 설정 파일 로드
        self.env_config = load_yaml(self.conf_dir / 'env.yml')
        self.meta_config = load_yaml(self.conf_dir / 'meta.yml')
        self.model_config = load_yaml(self.conf_dir / 'model.yml')
        
        # region 정보로 S3 헬퍼 재초기화
        self.s3 = S3Helper(region=self.env_config['region'])
        
        logger.info(f"\n  ✅ Config files loaded")
        logger.info(f"      env: {self.env_config['env']}")
        logger.info(f"      region: {self.env_config['region']}")
        logger.info(f"      project: {self.meta_config['project']}")
        logger.info(f"      experiment: {self.meta_config['experiment']}")
        
        if self.notebooks:
            logger.info(f"\n  📓 Notebooks found: {[n.name for n in self.notebooks]}")
        if self.scripts:
            logger.info(f"  📜 Scripts found: {[s.name for s in self.scripts]}")
    
    def download_data_files(self):
        """S3에서 데이터 파일 다운로드"""
        logger.info("\n📥 Downloading data files from S3...")
        
        # Data S3 경로 구성
        env = self.env_config['env']
        user_id = self.meta_config['user_id']
        project = self.meta_config['project']
        version = self.meta_config['version']
        data_bucket = self.env_config['s3']['data_bucket']
        
        data_s3_path = f"s3://{data_bucket}/{env}/{user_id}/{project}/{version}/"
        
        logger.info(f"    Source: {data_s3_path}")
        
        # 데이터 파일 다운로드 (하위 구조 유지)
        downloaded = self.s3.download_prefix(data_s3_path, self.data_dir)
        
        if not downloaded:
            logger.warning("  ⚠️  No data files downloaded!")
        else:
            logger.info(f"\n  ✅ Downloaded {len(downloaded)} data files")
    
    def generate_run_id(self) -> str:
        """Run ID 생성"""
        run_date = datetime.now().strftime('%Y%m%d')
        run_uuid = uuid4().hex[:8]
        algorithm = self.model_config['algorithm']['name']
        suffix = self.model_config['algorithm']['suffix']
        
        self.run_id = f"{run_date}_{algorithm}_{suffix}_{run_uuid}"
        logger.info(f"\n🏷️  Run ID: {self.run_id}")
        return self.run_id
    
    def find_main_notebook(self) -> Path:
        """실행할 메인 노트북 찾기"""
    
        # 외부 경로 지정 시 우선 사용
        if self.notebook_path:
            if not self.notebook_path.exists():
                raise RuntimeError(f"Notebook not found: {self.notebook_path}")
            logger.info(f"    Using external notebook: {self.notebook_path}")
            return self.notebook_path
    
        # S3에서 다운로드된 노트북 사용
        if not self.notebooks:
            raise RuntimeError("No notebook found in conf S3 path")
        
        # 1. model.yml에 notebook 지정되어 있는지 확인
        if 'notebook' in self.model_config:
            notebook_name = self.model_config['notebook']
            for nb in self.notebooks:
                if nb.name == notebook_name:
                    return nb
        
        # 2. *_modeling.ipynb 패턴 찾기
        for nb in self.notebooks:
            if 'modeling' in nb.name.lower():
                return nb
        
        # 3. 첫 번째 노트북 사용
        return self.notebooks[0]
    
    def run_notebook(self):
        """Papermill로 노트북 실행"""
        logger.info("\n🚀 Running notebook with Papermill...")
        
        # 메인 노트북 찾기
        notebook_path = self.find_main_notebook()
        logger.info(f"    Notebook: {notebook_path.name}")
        
        # 출력 노트북 경로
        output_notebook = self.output_dir / self.run_id / 'executed_notebook.ipynb'
        ensure_dir(output_notebook.parent)
        
        # 노트북 실행을 위한 작업 디렉토리 설정
        original_cwd = os.getcwd()
        
        try:
            os.chdir(self.work_dir)
            logger.info(f"    Working directory: {self.work_dir}")
            
            # Papermill 실행
            pm.execute_notebook(
                str(notebook_path),
                str(output_notebook),
                parameters={},
                cwd=str(self.work_dir),
                progress_bar=True,
                log_output=True
            )
            
            logger.info(f"\n  ✅ Notebook executed successfully")
            logger.info(f"    Output: {output_notebook}")
            
        except Exception as e:
            logger.error(f"\n  ❌ Notebook execution failed: {e}")
            raise
        finally:
            os.chdir(original_cwd)
    
    def upload_artifacts(self) -> str:
        """결과물을 S3에 업로드"""
        logger.info("\n📤 Uploading artifacts to S3...")
        
        # Model S3 경로 구성
        env = self.env_config['env']
        user_id = self.meta_config['user_id']
        project = self.meta_config['project']
        experiment = self.meta_config['experiment']
        model_bucket = self.env_config['s3']['model_bucket']
        
        model_s3_path = f"s3://{model_bucket}/{env}/{user_id}/{project}/{experiment}/{self.run_id}/"
        
        logger.info(f"    Destination: {model_s3_path}")
        
        # 버킷 존재 확인
        self.s3.ensure_bucket_exists(model_bucket)
        
        # Output 디렉토리에서 run_id 폴더 찾기
        run_output_dir = self.output_dir / self.run_id
        
        if not run_output_dir.exists():
            # output 폴더 내에서 가장 최근 폴더 찾기
            output_subdirs = [d for d in self.output_dir.iterdir() if d.is_dir()]
            if output_subdirs:
                run_output_dir = max(output_subdirs, key=lambda x: x.stat().st_mtime)
                logger.info(f"    Using output dir: {run_output_dir}")
            else:
                logger.warning(f"  ⚠️  No output directory found")
                return model_s3_path
        
        # 업로드
        uploaded = self.s3.upload_directory(run_output_dir, model_s3_path)
        
        logger.info(f"\n  ✅ Uploaded {len(uploaded)} files to S3")
        return model_s3_path
    
    def run(self):
        """전체 파이프라인 실행"""
        start_time = datetime.now()
        logger.info("=" * 70)
        logger.info("🚀 GS Retail ML Pipeline Runner")
        logger.info("=" * 70)
        logger.info(f"  Conf S3 Path: {self.conf_s3_path}")
        logger.info(f"  Work Dir:     {self.work_dir}")
        logger.info("=" * 70)
        
        try:
            # 1. 디렉토리 설정
            self.setup_directories()
            
            # 2. Conf S3에서 모든 파일 다운로드
            #    - yml → conf/
            #    - 나머지 (ipynb, py 등) → work_dir/
            self.download_conf_files()
            
            # 3. 데이터 파일 다운로드
            self.download_data_files()
            
            # 4. Run ID 생성
            self.generate_run_id()
            
            # 5. 노트북 실행
            self.run_notebook()
            
            # 6. 결과물 S3 업로드
            model_s3_path = self.upload_artifacts()
            
            # 완료
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("")
            logger.info("=" * 70)
            logger.info("✅ Pipeline completed successfully!")
            logger.info("=" * 70)
            logger.info(f"  Run ID:      {self.run_id}")
            logger.info(f"  Duration:    {duration:.1f} seconds")
            logger.info(f"  Output S3:   {model_s3_path}")
            logger.info("=" * 70)
            
            return {
                'status': 'success',
                'run_id': self.run_id,
                'duration_seconds': duration,
                'output_s3_path': model_s3_path
            }
            
        except Exception as e:
            logger.error("")
            logger.error("=" * 70)
            logger.error(f"❌ Pipeline failed: {e}")
            logger.error("=" * 70)
            raise


# ============================================================
# CLI 인터페이스
# ============================================================
def parse_args():
    """CLI 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='GS Retail ML Pipeline Runner',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 기본 실행
  python run_pm.py --conf-s3-path s3://gs-retail-awesome-conf-us-east-1/dev/sean/titanic-survival-prediction/baseline-v1/

  # 작업 디렉토리 지정
  python run_pm.py --conf-s3-path s3://bucket/path/ --work-dir /tmp/ml_run

  # Dry run (다운로드만)
  python run_pm.py --conf-s3-path s3://bucket/path/ --dry-run

S3 Conf 경로 구조:
  s3://gs-retail-awesome-conf-{region}/{env}/{user_id}/{project}/{experiment}/
    ├── env.yml              → conf/env.yml
    ├── meta.yml             → conf/meta.yml
    ├── model.yml            → conf/model.yml
    ├── titanic_modeling.ipynb  → work_dir/titanic_modeling.ipynb
    └── utils.py             → work_dir/utils.py (선택)

S3 Data 경로 (env.yml 참조):
  s3://gs-retail-awesome-data-{region}/{env}/{user_id}/{project}/{version}/
    └── data/
        ├── train.csv
        ├── validation.csv
        └── test.csv

S3 Model 경로 (Output):
  s3://gs-retail-awesome-model-{region}/{env}/{user_id}/{project}/{experiment}/{run_id}/
    ├── metadata/
    ├── config/
    ├── data_refs/
    ├── artifacts/
    └── reports/
        """
    )
    
    parser.add_argument(
        '--conf-s3-path',
        type=str,
        required=True,
        help='S3 path containing config files and notebooks'
    )

    parser.add_argument(
        '--notebook-path',
        type=str,
        default=None,
        help='외부 노트북 절대경로 (지정 시 S3 conf의 노트북 무시)'
    )
    
    parser.add_argument(
        '--work-dir',
        type=str,
        default=None,
        help='Local working directory (default: ./run_pm)'
    )
    
    parser.add_argument(
        '--clean',
        action='store_true',
        help='Clean work directory before running'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Download files only, do not run notebook'
    )
    
    return parser.parse_args()


def main():
    """메인 함수"""
    args = parse_args()
    
    # 작업 디렉토리
    work_dir = Path(args.work_dir) if args.work_dir else None
    
    # 클린 옵션
    if args.clean and work_dir and work_dir.exists():
        logger.info(f"🧹 Cleaning work directory: {work_dir}")
        shutil.rmtree(work_dir)
    
    # 파이프라인 실행
    runner = PipelineRunner(
        conf_s3_path=args.conf_s3_path,
        work_dir=work_dir,
        notebook_path=args.notebook_path,
    )
    
    if args.dry_run:
        logger.info("🔍 Dry run mode - downloading files only")
        runner.setup_directories()
        runner.download_conf_files()
        runner.download_data_files()
        runner.generate_run_id()
        
        logger.info("\n" + "=" * 70)
        logger.info("📋 Dry run summary")
        logger.info("=" * 70)
        logger.info(f"  Work dir:   {runner.work_dir}")
        logger.info(f"  Conf dir:   {runner.conf_dir}")
        logger.info(f"  Data dir:   {runner.data_dir}")
        logger.info(f"  Notebooks:  {[n.name for n in runner.notebooks]}")
        logger.info(f"  Scripts:    {[s.name for s in runner.scripts]}")
        logger.info("=" * 70)
        logger.info("✅ Dry run completed")
    else:
        result = runner.run()
        return result


if __name__ == '__main__':
    main()
