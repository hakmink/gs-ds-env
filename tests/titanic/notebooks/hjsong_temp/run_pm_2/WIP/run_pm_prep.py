"""
run_pm_prep.py - S3 Input 데이터 로드

S3에서 input 데이터를 로컬로 다운로드합니다.

Usage:
    from run_pm_prep import load_input

    result = load_input(
        input_cfg=input_cfg,
        local_cfg=local_cfg,
        user_id="sean",
        dry_run=True,
    )
"""

import os
from run_pm_utils import (
    InputConfig,
    LocalConfig,
    download_from_s3,
    print_header,
    print_separator,
)


def load_input(
    input_cfg: InputConfig,
    local_cfg: LocalConfig,
    user_id: str = None,
    version: str = None,
    project: str = None,
    dry_run: bool = False,
) -> dict:
    """
    S3 Input 데이터를 로컬로 다운로드

    Args:
        input_cfg: InputConfig 인스턴스 (env.yml 기반)
        local_cfg: LocalConfig 인스턴스
        user_id:   사용자 ID
        version:   데이터셋 버전 오버라이드
        project:   프로젝트명 오버라이드
        dry_run:   True면 다운로드 시뮬레이션

    Returns:
        dict: {s3_uri, local_dir, downloaded_files}
    """
    local_input_dir = os.path.join(local_cfg.root, local_cfg.input_dir)
    s3_prefix = input_cfg.get_s3_prefix(user_id=user_id, version=version, project=project)
    s3_uri    = input_cfg.get_s3_uri(user_id=user_id, version=version, project=project)

    print_header("Step 1: S3 Input 다운로드")
    print(f"Source: {s3_uri}")
    print(f"Target: {local_input_dir}")
    print_separator()

    os.makedirs(local_input_dir, exist_ok=True)
    downloaded = download_from_s3(input_cfg.bucket, s3_prefix, local_input_dir, dry_run)

    print(f"\n✓ Downloaded {len(downloaded)} files")

    return {
        's3_uri':           s3_uri,
        'local_dir':        local_input_dir,
        'downloaded_files': downloaded,
    }


# ============================================================
# CLI Entry Point
# ============================================================

if __name__ == "__main__":
    import argparse
    from run_pm_utils import load_env

    parser = argparse.ArgumentParser(description="Load input data from S3")
    parser.add_argument("--user_id",  default=None,       help="사용자 ID")
    parser.add_argument("--version",  default=None,       help="데이터셋 버전 오버라이드")
    parser.add_argument("--project",  default=None,       help="프로젝트명 오버라이드")
    parser.add_argument("--env_path", default="env.yml",  help="env.yml 경로")
    parser.add_argument("--dry_run",  action="store_true", help="Dry run 모드")
    args = parser.parse_args()

    env = load_env(args.env_path)
    result = load_input(
        input_cfg=InputConfig(env),
        local_cfg=LocalConfig(env),
        user_id=args.user_id,
        version=args.version,
        project=args.project,
        dry_run=args.dry_run,
    )
    print(f"\nResult: {result['s3_uri']}")
