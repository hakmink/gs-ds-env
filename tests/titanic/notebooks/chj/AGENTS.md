# AGENTS.md
This file is for agentic coding assistants working in this repository. It captures repo conventions, safe commands, and guardrails.

Repo: AWS SageMaker Training + Custom ECR Image (Titanic survival prediction)

## Source Of Truth
- Requirements/constraints: `CLAUDE.md`, `staff_exercise.md`
- Excellence Rubric (합격 기준): `staff_exercise.md` 상단 참조
- Execution log + working AWS commands: `progress.md`

## Repo Layout (high-signal)
- `train.py`: training entrypoint for SageMaker container; reads `/opt/ml/input/...`, writes `/opt/ml/model/...`
- `Dockerfile`: training image definition
- `requirements.txt`: deps for the training container image
- `run_training.py`: triggers a SageMaker Training Job (SageMaker SDK v3 API)
- `split_data.py`: creates `data/output/train.csv` and `data/output/val.csv`
- `data/`: local raw data (Kaggle Titanic CSVs)

## Environment
- Python 3.12 (see `.python-version`)
- Project deps live in `pyproject.toml`; `uv.lock` exists
- Repo usage pattern: `uv run ...`

Install `uv` (pick one):
```bash
pipx install uv
brew install uv
```

## Commands

Install/sync deps (preferred):
```bash
uv sync
```

Install deps (fallback):
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run scripts locally:
```bash
uv run python split_data.py
uv run python train.py
uv run python run_training.py
```

Build the training image:
```bash
docker build -t titanic-training:latest .
```

Lint/format:
- No linter/formatter configured (no `ruff`/`black`/etc. config).
- Lightweight sanity checks:
```bash
python -m compileall .
python -m pip check
```
- If adding lint/format tooling, propose first; do not introduce silently.

Tests:
- No `tests/` dir currently, but `pytest` exists in `uv.lock`.
```bash
uv run pytest                          # all tests
uv run pytest tests/test_train.py       # single file
uv run pytest tests/test_train.py::test_preprocess
uv run pytest -k preprocess             # keyword filter
uv run pytest -q                        # quieter
uv run pytest -x                        # stop on first failure
uv run pytest -vv                       # verbose
```

## SageMaker + Container Conventions (Do Not Break)
- Input channels: `/opt/ml/input/data/<channel_name>/` (this repo uses `train`, `validation`)
- Model output dir: `/opt/ml/model/` (SageMaker uploads this as `model.tar.gz`)
- Failure/output dir: `/opt/ml/output/` (SageMaker writes failure details here)
- Common env vars: `SM_MODEL_DIR`, `SM_CHANNEL_TRAIN`, `SM_CHANNEL_VALIDATION`

Current expectations in `train.py`:
- Reads: `train.csv` from the `train` channel, `val.csv` from the `validation` channel
- Writes to model dir: `model.joblib`, `label_encoders.joblib`, `metrics.json`

If you change filenames or channel names, update BOTH:
- `train.py` (reads/writes)
- `split_data.py` outputs and/or `run_training.py` S3 inputs

## Code Style (match existing scripts)

General:
- Keep changes small and targeted; repo is script-centric.
- Prefer readability over abstraction.
- Avoid adding new deps unless clearly needed.

Imports:
- Import order: stdlib, third-party, local; add a blank line between groups.
- Prefer `from pathlib import Path` for filesystem paths.
- Avoid wildcard imports.

Formatting:
- 4 spaces; add blank lines between top-level functions.
- Keep lines ~88-100 chars even without a formatter.

Naming:
- `snake_case` for functions/vars; `UPPER_SNAKE_CASE` for constants (see `run_training.py`).

Types:
- Codebase is mostly untyped; add hints for new non-trivial helpers.
- Avoid heavy typing around pandas; keep it pragmatic.

Error handling:
- Fail fast with clear exceptions (actionable messages).
- Do not swallow exceptions.
- Validate file existence for local reads and SageMaker channel paths.
- Ensure output dirs exist before writing artifacts.

Logging:
- Prefer `print(...)` with stable, scannable messages (SageMaker -> CloudWatch).

## AWS / Security Guardrails
- Never commit secrets (AWS keys, tokens, `.env`).
- Prefer env vars/AWS profiles over hardcoding account IDs/roles.
  Note: `run_training.py` hardcodes account/role/bucket; if you generalize it, keep backward compatibility or document the change.
- Region is `ap-northeast-2` per `CLAUDE.md` and `progress.md`.

## Git / Commit Rules
- Do NOT add `Co-Authored-By:` lines to commit messages (see `CLAUDE.md`).
- Do not create commits unless explicitly requested.

## Cursor / Copilot Rules
- No Cursor rules found (`.cursor/rules/` or `.cursorrules` not present).
- No Copilot instructions found (`.github/copilot-instructions.md` not present).

## When Unsure
Check these first:
1) `CLAUDE.md`
2) `progress.md`
3) `train.py`, `run_training.py`, `split_data.py`
