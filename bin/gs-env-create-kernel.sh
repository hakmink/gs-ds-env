#/bin/bash

set -euo pipefail

# Usage:
#   gs-env-create-kernel.sh <theme> <python_version> [requirements_path] [torch_variant]
#
# Examples:
#   gs-env-create-kernel.sh tsai 3.12
#   gs-env-create-kernel.sh tsai 3.12 /home/ec2-user/SageMaker/.myenv/gs-ds-env/tsai/kernel/requirements.txt cu121
#   gs-env-create-kernel.sh tsai 3.12 "" cpu
#
# Notes:
# - kernel name: <theme>_<pyver_no_dot>  e.g., tsai_312
# - venv path : $WORKING_DIR/<theme>/kernel/.venv
# - default requirements: $WORKING_DIR/<theme>/kernel/requirements.txt
# - torch_variant: cu121 | cpu (default: cu121)

WORKING_DIR="${WORKING_DIR:-/home/ec2-user/SageMaker/.myenv}"
SOURCE_ROOT="/home/ec2-user/SageMaker/gs-ds-env"

THEME="${1:?theme required}"
PYVER="${2:?python version required (e.g., 3.12)}"
REQ_PATH="${3:-${SOURCE_ROOT}/${THEME}/kernel/requirements.txt}"
TORCH_VARIANT="${4:-cu121}"

PYVER_TAG="$(echo "${PYVER}" | tr -d '.')"
KERNEL_NAME="${THEME}_${PYVER_TAG}"
THEME_DIR="${WORKING_DIR}/${THEME}"
KERNEL_DIR="${THEME_DIR}/kernel"
VENV_DIR="${KERNEL_DIR}/.venv"

mkdir -p "${KERNEL_DIR}"
mkdir -p "${THEME_DIR}/docker"

# --- Disk-safe caches (avoid /) ---
export PIP_CACHE_DIR="${WORKING_DIR}/.pip-cache"
export UV_CACHE_DIR="${WORKING_DIR}/.uv-cache"
mkdir -p "${PIP_CACHE_DIR}" "${UV_CACHE_DIR}"

# --- Ensure uv installed ---
if ! command -v uv >/dev/null 2>&1; then
  echo "[INFO] uv not found. Installing uv..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
  # shellcheck disable=SC1090
  source "${HOME}/.bashrc" || true
fi

echo "[INFO] uv: $(uv --version)"

# --- Create venv (pinned python) ---
echo "[INFO] Creating venv: ${VENV_DIR} (python ${PYVER})"
uv venv "${VENV_DIR}" --python "${PYVER}"

# Activate
# shellcheck disable=SC1090
source "${VENV_DIR}/bin/activate"

# --- Install base tooling ---
uv pip install -U pip setuptools wheel
uv pip install ipykernel

# --- Install torch (optional but typically needed) ---
if [[ "${TORCH_VARIANT}" == "cu121" ]]; then
  echo "[INFO] Installing torch (cu121 index)..."
  uv pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121
elif [[ "${TORCH_VARIANT}" == "cpu" ]]; then
  echo "[INFO] Installing torch (cpu)..."
  uv pip install torch torchvision torchaudio
else
  echo "[WARN] Unknown torch_variant='${TORCH_VARIANT}'. Skipping torch install."
fi

# --- Install requirements if present ---
if [[ -n "${REQ_PATH}" && -f "${REQ_PATH}" ]]; then
  echo "[INFO] Installing requirements: ${REQ_PATH}"
  uv pip install -r "${REQ_PATH}"
else
  echo "[WARN] requirements not found at: ${REQ_PATH} (skip)"
fi

# --- Register kernel ---
echo "[INFO] Registering Jupyter kernel: ${KERNEL_NAME}"
python -m ipykernel install --user --name "${KERNEL_NAME}" --display-name "${KERNEL_NAME}"

# --- Persist manifest for traceability ---
MANIFEST="${KERNEL_DIR}/manifest.env"
cat > "${MANIFEST}" << MAN
THEME=${THEME}
PYVER=${PYVER}
KERNEL_NAME=${KERNEL_NAME}
VENV_DIR=${VENV_DIR}
REQ_PATH=${REQ_PATH}
TORCH_VARIANT=${TORCH_VARIANT}
PIP_CACHE_DIR=${PIP_CACHE_DIR}
UV_CACHE_DIR=${UV_CACHE_DIR}
MAN

echo "[OK] Done."
echo "     Kernel name : ${KERNEL_NAME}"
echo "     Venv dir     : ${VENV_DIR}"
echo "     Manifest     : ${MANIFEST}"
echo
echo "Next: In JupyterLab -> Kernel -> Change Kernel -> ${KERNEL_NAME}"
