"""
run_pm.py - 모델링 파이프라인 통합 진입점

env.yml / meta.yml / model.yml 의 S3 URI (또는 로컬 경로) 를 args로 받아
3단계 파이프라인을 실행합니다.

  Step 1 (prep)  : S3 Input 데이터 다운로드
  Step 2 (model) : 모델링 수행 및 Output 파일 생성
  Step 3 (post)  : Output 결과물 S3 업로드

─────────────────────────────────────────────────────────────
Usage
─────────────────────────────────────────────────────────────
# S3에서 yml 로드 (프로덕션 / SageMaker Training Job)
python run_pm.py \\
    --env_uri   s3://gs-retail-awesome-config-ap-northeast-2/env/dev/env.yml \\
    --meta_uri  s3://gs-retail-awesome-config-ap-northeast-2/projects/gs25-sales-forecast/experiments/baseline-v1/meta.yml \\
    --model_uri s3://gs-retail-awesome-config-ap-northeast-2/projects/gs25-sales-forecast/experiments/baseline-v1/model.yml \\
    --user_id   sean \\
    --job_type  all \\
    --dry_run

# 로컬 yml 사용 (개발 / 디버그)
python run_pm.py \\
    --env_uri   env.yml \\
    --meta_uri  meta.yml \\
    --model_uri model.yml \\
    --user_id   sean \\
    --dry_run

# 단계별 실행
python run_pm.py --env_uri s3://... --meta_uri s3://... --model_uri s3://... --job_type prep
python run_pm.py --env_uri s3://... --meta_uri s3://... --model_uri s3://... --job_type model --run_id 20260303T120000Z_abc12345
python run_pm.py --env_uri s3://... --meta_uri s3://... --model_uri s3://... --job_type post  --run_id 20260303T120000Z_abc12345
─────────────────────────────────────────────────────────────
"""

import os
import argparse
from datetime import datetime

from run_pm_utils import (
    InputConfig,
    OutputConfig,
    LocalConfig,
    load_yml_auto,
    create_file,
    print_header,
    print_separator,
)
from run_pm_prep import load_input
from run_pm_post import save_output


# ============================================================
# Mockup: Output 파일 정의
# ============================================================

def get_output_mockups(
    run_id: str,
    user_id: str,
    input_cfg: InputConfig,
    meta: dict,
    model_cfg: dict,
) -> dict:
    hp  = model_cfg.get("hyperparameters", {})
    alg = model_cfg.get("algorithm", {})
    rpt = model_cfg.get("report", {})
    mfl = meta.get("mlflow", {})

    return {
        'metadata': {
            'run_manifest.yml': {
                'type': 'yaml',
                'content': {
                    'version':    '1.0',
                    'run_id':     run_id,
                    'created_at': datetime.utcnow().isoformat(),
                    'created_by': user_id,
                    'status':     'completed',
                    'project':    meta.get('project'),
                    'experiment': meta.get('experiment'),
                    'model':      meta.get('model'),
                    'algo':       alg.get('name'),
                    'mlflow': {
                        'experiment_name': mfl.get('experiment_name'),
                        'tracking_uri':    mfl.get('tracking_uri'),
                    },
                    'execution': {
                        'instance_type':    'ml.m5.xlarge',
                        'instance_count':   1,
                        'duration_seconds': 1234,
                    },
                },
            }
        },
        'config': {
            # 재현성을 위해 실제 사용된 yml 3개를 artifacts에 보관
            'env.yml':   {'type': 'touch', 'content': None},
            'meta.yml':  {'type': 'touch', 'content': None},
            'model.yml': {'type': 'yaml',  'content': model_cfg},
        },
        'data_refs': {
            'input_data_ref.yml': {
                'type': 'yaml',
                'content': {
                    'version':       '1.0',
                    'input_s3_uri':  input_cfg.get_s3_uri(user_id=user_id),
                    'data_settings': model_cfg.get('data', {}),
                    'data_sources': {
                        'train':      {'format': 'parquet', 'rows': 100000},
                        'validation': {'format': 'parquet', 'rows': 10000},
                        'test':       {'format': 'parquet', 'rows': 10000},
                    },
                },
            }
        },
        'artifacts/model':         {'.placeholder': {'type': 'touch', 'content': None}},
        'artifacts/metrics': {
            'model_metrics.yml': {
                'type': 'yaml',
                'content': {
                    'version': '1.0',
                    'metrics': {
                        'train':      {'rmse': 0.1234, 'mae': 0.0987, 'r2': 0.9123},
                        'validation': {'rmse': 0.1456, 'mae': 0.1123, 'r2': 0.8901},
                        'test':       {'rmse': 0.1523, 'mae': 0.1198, 'r2': 0.8856},
                    },
                    'feature_importance': {
                        'day_of_week':    0.25,
                        'is_holiday':     0.20,
                        'temperature':    0.18,
                        'promotion_flag': 0.15,
                        'precipitation':  0.12,
                        'store_id':       0.10,
                    },
                },
            }
        },
        'artifacts/charts':         {'.placeholder': {'type': 'touch', 'content': None}},
        'artifacts/explainability': {'.placeholder': {'type': 'touch', 'content': None}},
        'reports': {
            'report_request.yml': {
                'type': 'yaml',
                'content': {
                    'version':          '1.0',
                    'report_type':      rpt.get('type', 'model_training_summary'),
                    'requested_at':     datetime.utcnow().isoformat(),
                    'requested_by':     user_id,
                    'include_sections': rpt.get('include_sections', []),
                },
            }
        },
    }


def create_output_mockups(output_dir: str, mockups: dict) -> list:
    created_files = []
    for subdir, files in mockups.items():
        subdir_path = os.path.join(output_dir, subdir)
        os.makedirs(subdir_path, exist_ok=True)
        for filename, file_info in files.items():
            filepath = os.path.join(subdir_path, filename)
            create_file(filepath, file_info.get('content'), file_info.get('type', 'yaml'))
            created_files.append(filepath)
            print(f"  ✓ Created: {filepath}")
    return created_files


# ============================================================
# Step 2: 모델링
# ============================================================

def run_model(
    output_cfg: OutputConfig,
    local_cfg:  LocalConfig,
    input_cfg:  InputConfig,
    meta:       dict,
    model_cfg:  dict,
    user_id:    str = None,
    run_id:     str = None,
) -> dict:
    local_output_dir = os.path.join(local_cfg.root, local_cfg.output_dir)
    run_id  = run_id  or OutputConfig.generate_run_id()
    user_id = user_id or output_cfg.user_id

    print_header("Step 2: 모델링 수행")
    d = model_cfg.get('data', {})
    print(f"  Run ID    : {run_id}")
    print(f"  Algorithm : {model_cfg.get('algorithm', {}).get('name')}")
    print(f"  Train     : {d.get('train_from')} ~ {d.get('train_to')}")
    print(f"  Test      : {d.get('test_from')}  ~ {d.get('test_to')}")
    print(f"  Output    : {local_output_dir}")
    print_separator()

    print("  ... 데이터 로딩 중 ...")
    print("  ... 전처리 중 ...")
    print("  ... 모델 학습 중 (스킵) ...")
    print("  ... 검증 중 (스킵) ...")
    print("  ... 테스트 중 (스킵) ...")
    print("✓ 모델링 완료 (목업)")

    print("\n[Output 파일 생성]")
    os.makedirs(local_output_dir, exist_ok=True)
    mockups = get_output_mockups(run_id, user_id, input_cfg, meta, model_cfg)
    created_files = create_output_mockups(local_output_dir, mockups)

    print(f"\n✓ Created {len(created_files)} output files")
    return {
        'status':        'completed',
        'run_id':        run_id,
        'output_dir':    local_output_dir,
        'created_files': created_files,
    }


# ============================================================
# CLI Entry Point
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="모델링 파이프라인 실행 (prep → model → post)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # ── yml 위치 (S3 URI 또는 로컬 경로) ────────────────────
    parser.add_argument(
        "--env_uri",
        type=str,
        required=True,
        help="env.yml 경로  (예: s3://bucket/env/dev/env.yml  또는  env.yml)",
    )
    parser.add_argument(
        "--meta_uri",
        type=str,
        required=True,
        help="meta.yml 경로 (예: s3://bucket/projects/.../meta.yml 또는  meta.yml)",
    )
    parser.add_argument(
        "--model_uri",
        type=str,
        required=True,
        help="model.yml 경로 (예: s3://bucket/projects/.../model.yml 또는 model.yml)",
    )

    # ── 실행 제어 ────────────────────────────────────────────
    parser.add_argument(
        "--job_type",
        type=str,
        default="all",
        choices=["all", "prep", "model", "post"],
        help="실행 단계 (all | prep | model | post)",
    )
    parser.add_argument("--dry_run", action="store_true", help="Dry run 모드")

    # ── 실행 식별자 ──────────────────────────────────────────
    parser.add_argument("--user_id",    type=str, default=None, help="사용자 ID")
    parser.add_argument("--run_id",     type=str, default=None, help="Run ID (없으면 자동 생성)")
    parser.add_argument("--run_date",   type=str, default=None, help="Run Date YYYY-MM-DD (없으면 오늘 UTC)")
    parser.add_argument("--task_token", type=str, default=None, help="Step Functions Task Token")

    return parser.parse_args()


def main():
    args = parse_args()

    # ── yml 로드 (S3 or 로컬 자동 판별) ─────────────────────
    print_header("설정 파일 로드")
    env       = load_yml_auto(args.env_uri)
    meta      = load_yml_auto(args.meta_uri)
    model_cfg = load_yml_auto(args.model_uri)

    # ── Config 객체 생성 ─────────────────────────────────────
    input_cfg  = InputConfig(env, meta)
    output_cfg = OutputConfig(env, meta, model_cfg)
    local_cfg  = LocalConfig(env)

    # ── 실행 식별자 확정 ─────────────────────────────────────
    run_id   = args.run_id   or OutputConfig.generate_run_id()
    run_date = args.run_date or OutputConfig.get_run_date()
    user_id  = args.user_id  or output_cfg.user_id

    # ── 파이프라인 시작 로그 ─────────────────────────────────
    print_header("run_pm 파이프라인 시작")
    print(f"  job_type   : {args.job_type}")
    print(f"  user_id    : {user_id}")
    print(f"  run_id     : {run_id}")
    print(f"  run_date   : {run_date}")
    print(f"  --- env ---")
    print(f"  env        : {env.get('env')}")
    print(f"  input      : {input_cfg.bucket}")
    print(f"  output     : {output_cfg.bucket}")
    print(f"  --- meta ---")
    print(f"  project    : {meta.get('project')}")
    print(f"  experiment : {meta.get('experiment')}")
    print(f"  model      : {meta.get('model')}")
    print(f"  version    : {meta.get('version')}")
    print(f"  --- model ---")
    d = model_cfg.get('data', {})
    print(f"  algo       : {model_cfg.get('algorithm', {}).get('name')}")
    print(f"  train      : {d.get('train_from')} ~ {d.get('train_to')}")
    print(f"  test       : {d.get('test_from')}  ~ {d.get('test_to')}")
    print(f"  --- etc ---")
    print(f"  dry_run    : {args.dry_run}")
    if args.task_token:
        print(f"  task_token : {args.task_token[:12]}...")
    print_separator()

    results = {}

    # ── Step 1: Prep ─────────────────────────────────────────
    if args.job_type in ("all", "prep"):
        results['prep'] = load_input(
            input_cfg=input_cfg,
            local_cfg=local_cfg,
            user_id=user_id,
            version=meta.get('version'),
            project=meta.get('project'),
            dry_run=args.dry_run,
        )

    # ── Step 2: Model ────────────────────────────────────────
    if args.job_type in ("all", "model"):
        results['model'] = run_model(
            output_cfg=output_cfg,
            local_cfg=local_cfg,
            input_cfg=input_cfg,
            meta=meta,
            model_cfg=model_cfg,
            user_id=user_id,
            run_id=run_id,
        )
        run_id = results['model']['run_id']   # 자동 생성된 경우 동기화

    # ── Step 3: Post ─────────────────────────────────────────
    if args.job_type in ("all", "post"):
        results['post'] = save_output(
            output_cfg=output_cfg,
            local_cfg=local_cfg,
            user_id=user_id,
            run_id=run_id,
            run_date=run_date,
            project=meta.get('project'),
            experiment=meta.get('experiment'),
            model=meta.get('model'),
            algo=model_cfg.get('algorithm', {}).get('name'),
            dry_run=args.dry_run,
        )

    # ── 최종 결과 출력 ───────────────────────────────────────
    print_header("파이프라인 완료")
    if 'prep'  in results: print(f"  [prep]  {results['prep']['s3_uri']}")
    if 'model' in results: print(f"  [model] run_id={results['model']['run_id']}  status={results['model']['status']}")
    if 'post'  in results: print(f"  [post]  {results['post']['s3_uri']}")

    return results


if __name__ == "__main__":
    main()
