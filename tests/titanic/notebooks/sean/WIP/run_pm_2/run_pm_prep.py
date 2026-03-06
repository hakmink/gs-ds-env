"""
run_pm_prep.py - S3 Input 데이터 로드

S3에서 input 데이터를 로컬로 다운로드합니다.

Usage:
    from run_pm_prep import load_input
    
    result = load_input(user_id="sean", version="v1.0", dry_run=True)
"""

import os
from run_pm_utils import (
    InputConfig,
    LocalConfig,
    download_from_s3,
    print_header,
    print_separator
)


def load_input(
    user_id: str = None,
    version: str = None,
    project: str = None,
    local_root: str = None,
    dry_run: bool = False
) -> dict:
    """
    S3 Input 데이터를 로컬로 다운로드
    
    Args:
        user_id: 사용자 ID
        version: 데이터셋 버전
        project: 프로젝트명
        local_root: 로컬 루트 디렉토리
        dry_run: True면 다운로드 시뮬레이션
    
    Returns:
        dict: {s3_uri, local_dir, downloaded_files}
    """
    local_root = local_root or LocalConfig.ROOT
    local_input_dir = os.path.join(local_root, LocalConfig.INPUT_DIR)
    
    s3_prefix = InputConfig.get_s3_prefix(
        user_id=user_id,
        version=version,
        project=project
    )
    s3_uri = InputConfig.get_s3_uri(
        user_id=user_id,
        version=version,
        project=project
    )
    
    print_header("Step 1: S3 Input 다운로드")
    print(f"Source: {s3_uri}")
    print(f"Target: {local_input_dir}")
    print_separator()
    
    os.makedirs(local_input_dir, exist_ok=True)
    downloaded = download_from_s3(
        InputConfig.BUCKET,
        s3_prefix,
        local_input_dir,
        dry_run
    )
    
    print(f"\n✓ Downloaded {len(downloaded)} files")
    
    return {
        's3_uri': s3_uri,
        'local_dir': local_input_dir,
        'downloaded_files': downloaded
    }


# ============================================================
# CLI Entry Point
# ============================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Load input data from S3")
    parser.add_argument("--user-id", default="sean", help="User ID")
    parser.add_argument("--version", default="v1.0", help="Dataset version")
    parser.add_argument("--project", default=None, help="Project name")
    parser.add_argument("--local-root", default=None, help="Local root directory")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    
    args = parser.parse_args()
    
    result = load_input(
        user_id=args.user_id,
        version=args.version,
        project=args.project,
        local_root=args.local_root,
        dry_run=args.dry_run
    )
    
    print(f"\nResult: {result['s3_uri']}")