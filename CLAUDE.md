# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GS DS Environment Manager — an MLOps scaffolding system for SageMaker AI Notebook environments. It standardizes creating isolated Python virtual environments, registering Jupyter kernels, and deploying containerized ML training pipelines to AWS SageMaker/ECR.

**Core principle:** "하나의 테마 = 하나의 환경 = 하나의 커널 = 하나의 Docker 이미지" (One theme = One environment = One kernel = One Docker image)

## Key Commands

```bash
# Create a virtual environment + Jupyter kernel (SageMaker Classic Notebook)
bin/create_env.sh <theme> <python_ver> [requirements.txt] [torch_variant]

# Create venv + kernel for SageMaker Studio (uses uv venv, not conda)
bin/gs-env-create-kernel-smus.sh <theme> <python_version> [kernel_name]

# Auto-detect conda envs and create bash aliases
bin/start.sh

# Build Docker image and push to ECR
bin/build_and_push_sm.sh <env_name> [base_ver] [sm_ver]

# Build base Docker image
bin/build_and_push_base.sh <env_name>

# Generate Dockerfile from Jinja2 template
python bin/gen_dockerfile.py --env <env_name> [--base_version 1.0] [--my_version 1.0]

# Remove a conda env and its Jupyter kernel
bin/remove_env.sh <env_name>

# Increase EC2 instance swap (run once if collecting packages gets killed)
bin/increase_swap_size.sh

# Deploy email notification Lambda
cd lambda/email_notify && ./zip_requests_layer.sh && ./build.sh create
```

## Architecture

### Theme-Based Environment Model

Each **theme** (e.g., `tabular312`, `lightgbm311`, `boilerplate312`, `streamlit314`) has three layers:

1. **Kernel** (`<theme>/kernel/requirements.txt`) — Jupyter notebook development packages, installed via `uv`
2. **Base Docker** (`<theme>/base_docker/`) — Ubuntu 20.04 + Python compiled from source; packages installed with three-tier fallback: `--only-binary :all:` → `--prefer-binary` → standard
3. **SageMaker Docker** (`<theme>/sm_docker/`) — Training container that extends the base image via Jinja2-generated Dockerfile, pushed to ECR

Kernel and Docker requirements are intentionally separate (notebook deps ≠ production training deps).

### Naming Conventions

- Conda env / theme: `<name><pyver>` (e.g., `tabular312`)
- Jupyter kernel: `conda_<theme>` (e.g., `conda_tabular312`)
- ECR image: `gs-automl-base-containers/<theme>_sm:<version>`
- Python version shorthand: `3.12 → 312`, `3.10 → 310`

### Working Directory

All environments, caches, and kernel configs are stored under:
```
WORKING_DIR="/home/ec2-user/SageMaker/.myenv"
```
pip cache → `$WORKING_DIR/.pip-cache`, uv cache → `$WORKING_DIR/.uv-cache`. This keeps `/` disk usage contained.

### Run PM Pipeline Pattern

The `run_pm` pattern is the standardized ML pipeline orchestration approach, progressing in phases:

- **run_pm_0**: Direct SageMaker Estimator API training
- **run_pm_1**: Papermill-based notebook execution with structured I/O (`input/` → `output/`)
- **run_pm_2**: Multi-stage DAG with separate Docker build (prepare → train → evaluate)

**run_pm.py workflow** (inside each `sm_docker/` or `modeling/`):
1. Downloads YAML configs from `--conf-s3-path` → local `conf/`
2. Reads `env.yml` to resolve the data S3 path
3. Downloads data files → local `data/`
4. Executes the modeling notebook via Papermill
5. Uploads output (executed notebook + artifacts) to `gs-retail-awesome-model-{region}`

Test papermill execution locally before SageMaker submission:
```bash
python run_pm.py \
  --conf-s3-path s3://gs-retail-awesome-conf-{region}/{env}/{user}/{project}/{experiment}/ \
  --notebook-path ./modeling.ipynb
```

Each `run_pm` directory contains: `conf.py` (AWS account/region), `run_pm_utils.py` (S3/DynamoDB/YAML utilities), YAML configs (`config.yml`, `profile.yml`, `run_manifest.yml`), and `input/`/`output/` directories.

### S3 Bucket Conventions

| Bucket | Contents |
|--------|----------|
| `gs-retail-awesome-conf-{region}` | YAML config files, modeling notebooks |
| `gs-retail-awesome-data-{region}` | Train/validation/test datasets |
| `gs-retail-awesome-model-{region}` | Model artifacts, executed notebooks, metrics |

S3 key paths: `{env}/{user_id}/{project}/{experiment}/`

### Config Generation

Dockerfiles are generated from `<theme>/sm_docker/Dockerfile.template` (Jinja2) using variables `{{ account_id }}`, `{{ region_name }}`, `{{ env_name }}`, `{{ base_version }}`, `{{ my_version }}` via `bin/gen_dockerfile.py`, which fetches account/region from AWS STS automatically.

### conf.py Pattern

Each `sm_docker/` contains a `conf.py` that:
- Hardcodes `kernel_name = "conda_<theme>"`
- Calls `boto3 STS get_caller_identity()` to get `account_id`
- Defaults `region_name` to `'ap-northeast-2'` if `session.region_name` is None
- Also defines `log_table_name = 'automl-logs'` (DynamoDB table for experiment tracking)

### Lambda

`lambda/email_notify/` contains an AWS Lambda function (`src/main.py`) that sends email via the SMTP2GO API. API key is stored in AWS Systems Manager Parameter Store at `/sean-credential/smtp2go-key`. Deploy with `build.sh create` (creates) or `build.sh` (updates).

## Technology Stack

- **Package manager**: `uv` (primary), pip (fallback), Miniconda (base for Classic Notebook)
- **Target platform**: Amazon Linux 2 / SageMaker AI Notebook (Classic) and SageMaker Studio
- **AWS services**: SageMaker, ECR, S3, DynamoDB (`automl-logs`), Secrets Manager, Lambda, SSM Parameter Store
- **ML frameworks**: LightGBM, XGBoost, scikit-learn
- **Pipeline execution**: Papermill (parametric notebook execution)
- **Experiment tracking**: MLflow
- **Documentation language**: Korean

## Test Data and Samples

`tests/titanic/notebooks/` contains multi-user workspaces (`hjsong/`, `kunops/`, `sean/`, `chj/`) each with independent `run_pm_0/`, `run_pm_1/`, `run_pm_2/` implementations using the Titanic dataset (pre-split parquet files).

`samples/` provides reference implementations (`hjsong/`, `sean/`) with the complete three-stage workflow: `prepare_input/` → `modeling/` → `docker/`. Each sample `README.md` is the authoritative guide for that workflow.
