"""
run_pm_post.py - Artifacts S3 업로드

로컬의 output 결과물을 S3에 업로드합니다.

Usage:
    from run_pm_post import save_output
    
    result = save_output(user_id="sean", run_id="20260303T...", dry_run=True)
"""

import os
from run_pm_utils import (
    OutputConfig,
    LocalConfig,
    ensure_bucket_exists,
    upload_directory_tree,
    print_header,
    print_separator
)


def save_output(
    user_id: str = None,
    run_id: str = None,
    run_date: str = None,
    project: str = None,
    experiment: str = None,
    model: str = None,
    algo: str = None,
    local_root: str = None,
    dry_run: bool = False
) -> dict:
    """
    로컬 Output을 S3에 업로드
    
    Args:
        user_id: 사용자 ID
        run_id: 실행 ID (없으면 자동 생성)
        run_date: 실행 날짜 (없으면 오늘)
        project: 프로젝트명
        experiment: 실험명
        model: 모델명
        algo: 알고리즘명
        local_root: 로컬 루트 디렉토리
        dry_run: True면 업로드 시뮬레이션
    
    Returns:
        dict: {s3_uri, run_id, run_date, uploaded_files}
    """
    local_root = local_root or LocalConfig.ROOT
    local_output_dir = os.path.join(local_root, LocalConfig.OUTPUT_DIR)
    
    run_id = run_id or OutputConfig.generate_run_id()
    run_date = run_date or OutputConfig.get_run_date()
    
    s3_prefix = OutputConfig.get_s3_prefix(
        user_id=user_id,
        project=project,
        experiment=experiment,
        model=model,
        algo=algo,
        run_date=run_date,
        run_id=run_id
    )
    s3_uri = OutputConfig.get_s3_uri(
        user_id=user_id,
        project=project,
        experiment=experiment,
        model=model,
        algo=algo,
        run_date=run_date,
        run_id=run_id
    )
    
    print_header("Step 3: Artifacts S3 업로드")
    print(f"Run ID: {run_id}")
    print(f"Run Date: {run_date}")
    print(f"Source: {local_output_dir}")
    print(f"Target: {s3_uri}")
    print_separator()
    
    # 버킷 존재 확인 및 생성
    if not dry_run:
        if not ensure_bucket_exists(OutputConfig.BUCKET, OutputConfig.REGION):
            raise Exception(f"Failed to ensure bucket exists: {OutputConfig.BUCKET}")
    
    # 업로드
    print("\n[S3 업로드]")
    uploaded = upload_directory_tree(
        local_output_dir,
        OutputConfig.BUCKET,
        s3_prefix,
        dry_run
    )
    
    print(f"\n✓ Uploaded {len(uploaded)} files")
    
    return {
        's3_uri': s3_uri,
        'run_id': run_id,
        'run_date': run_date,
        'local_dir': local_output_dir,
        'uploaded_files': uploaded,
        'dry_run': dry_run
    }


# ============================================================
# CLI Entry Point
# ============================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Upload artifacts to S3")
    parser.add_argument("--user-id", default="sean", help="User ID")
    parser.add_argument("--run-id", default=None, help="Run ID (auto-generated if not provided)")
    parser.add_argument("--run-date", default=None, help="Run date (YYYY-MM-DD)")
    parser.add_argument("--project", default=None, help="Project name")
    parser.add_argument("--experiment", default=None, help="Experiment name")
    parser.add_argument("--local-root", default=None, help="Local root directory")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    
    args = parser.parse_args()
    
    result = save_output(
        user_id=args.user_id,
        run_id=args.run_id,
        run_date=args.run_date,
        project=args.project,
        experiment=args.experiment,
        local_root=args.local_root,
        dry_run=args.dry_run
    )
    
    print(f"\nResult: {result['s3_uri']}")