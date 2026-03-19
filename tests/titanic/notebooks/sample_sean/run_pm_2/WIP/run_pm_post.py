"""
run_pm_post.py - Artifacts S3 업로드

로컬의 output 결과물을 S3에 업로드합니다.

Usage:
    from run_pm_post import save_output

    result = save_output(
        output_cfg=output_cfg,
        local_cfg=local_cfg,
        user_id="sean",
        run_id="20260303T...",
        dry_run=True,
    )
"""

import os
from run_pm_utils import (
    OutputConfig,
    LocalConfig,
    ensure_bucket_exists,
    upload_directory_tree,
    print_header,
    print_separator,
)


def save_output(
    output_cfg: OutputConfig,
    local_cfg: LocalConfig,
    user_id: str = None,
    run_id: str = None,
    run_date: str = None,
    project: str = None,
    experiment: str = None,
    model: str = None,
    algo: str = None,
    dry_run: bool = False,
) -> dict:
    """
    로컬 Output을 S3에 업로드

    Args:
        output_cfg:  OutputConfig 인스턴스 (env.yml + model.yml 기반)
        local_cfg:   LocalConfig 인스턴스
        user_id:     사용자 ID
        run_id:      실행 ID (없으면 자동 생성)
        run_date:    실행 날짜 (YYYY-MM-DD, 없으면 오늘 UTC)
        project:     프로젝트명 오버라이드
        experiment:  실험명 오버라이드
        model:       모델명 오버라이드
        algo:        알고리즘명 오버라이드
        dry_run:     True면 업로드 시뮬레이션

    Returns:
        dict: {s3_uri, run_id, run_date, local_dir, uploaded_files, dry_run}
    """
    local_output_dir = os.path.join(local_cfg.root, local_cfg.output_dir)
    run_id   = run_id   or OutputConfig.generate_run_id()
    run_date = run_date or OutputConfig.get_run_date()

    s3_prefix = output_cfg.get_s3_prefix(
        user_id=user_id, project=project, experiment=experiment,
        model=model, algo=algo, run_date=run_date, run_id=run_id,
    )
    s3_uri = output_cfg.get_s3_uri(
        user_id=user_id, project=project, experiment=experiment,
        model=model, algo=algo, run_date=run_date, run_id=run_id,
    )

    print_header("Step 3: Artifacts S3 업로드")
    print(f"Run ID:   {run_id}")
    print(f"Run Date: {run_date}")
    print(f"Source:   {local_output_dir}")
    print(f"Target:   {s3_uri}")
    print_separator()

    if not dry_run:
        if not ensure_bucket_exists(output_cfg.bucket, output_cfg.region):
            raise Exception(f"Failed to ensure bucket exists: {output_cfg.bucket}")

    print("\n[S3 업로드]")
    uploaded = upload_directory_tree(local_output_dir, output_cfg.bucket, s3_prefix, dry_run)

    print(f"\n✓ Uploaded {len(uploaded)} files")

    return {
        's3_uri':         s3_uri,
        'run_id':         run_id,
        'run_date':       run_date,
        'local_dir':      local_output_dir,
        'uploaded_files': uploaded,
        'dry_run':        dry_run,
    }


# ============================================================
# CLI Entry Point
# ============================================================

if __name__ == "__main__":
    import argparse
    from run_pm_utils import load_env, load_model_cfg

    parser = argparse.ArgumentParser(description="Upload artifacts to S3")
    parser.add_argument("--user_id",    default=None,         help="사용자 ID")
    parser.add_argument("--run_id",     default=None,         help="Run ID (자동 생성 가능)")
    parser.add_argument("--run_date",   default=None,         help="Run Date (YYYY-MM-DD)")
    parser.add_argument("--project",    default=None,         help="프로젝트명 오버라이드")
    parser.add_argument("--experiment", default=None,         help="실험명 오버라이드")
    parser.add_argument("--model",      default=None,         help="모델명 오버라이드")
    parser.add_argument("--algo",       default=None,         help="알고리즘명 오버라이드")
    parser.add_argument("--env_path",   default="env.yml",    help="env.yml 경로")
    parser.add_argument("--model_path", default="model.yml",  help="model.yml 경로")
    parser.add_argument("--dry_run",    action="store_true",  help="Dry run 모드")
    args = parser.parse_args()

    env       = load_env(args.env_path)
    model_cfg = load_model_cfg(args.model_path)

    result = save_output(
        output_cfg=OutputConfig(env, model_cfg),
        local_cfg=LocalConfig(env),
        user_id=args.user_id,
        run_id=args.run_id,
        run_date=args.run_date,
        project=args.project,
        experiment=args.experiment,
        model=args.model,
        algo=args.algo,
        dry_run=args.dry_run,
    )
    print(f"\nResult: {result['s3_uri']}")
