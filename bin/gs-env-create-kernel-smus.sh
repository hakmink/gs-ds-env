#!/usr/bin/env bash
set -euo pipefail

# SageMaker Studio (sagemaker-user) - uv venv + Jupyter kernel registration
#
# Usage:
#   ./gs-env-create-kernel-smus.sh <theme> <python_version> <requirements_file> [kernel_name]
#
# Examples:
#   ./gs-env-create-kernel-smus.sh streamlit 3.12 ~/gs-ds-env/streamlit/kernel/requirements.txt
#   ./gs-env-create-kernel-smus.sh tsai 3.10 ~/gs-ds-env/tsai/kernel/requirements.txt tsai_310
#
# Notes:
# - venv path: /home/sagemaker-user/.myenv/<theme>/kernel/.venv
# - kernel default: <theme>_<pyver_no_dot> (e.g., streamlit_312)
# - Python version is enforced via: uv venv --python <python_version>

usage() {
  echo "Usage: $0 <theme> <python_version> [kernel_name]"
  echo "  theme            e.g., streamlit"
  echo "  python_version    e.g., 3.12"
  echo "  kernel_name       optional, default: <theme>_<pyver_no_dot>"
  echo "  requirements_file e.g., ~/gs-ds-env/<theme>/kernel/requirements.txt"
}

if [[ $# -lt 2 ]]; then
  usage
  exit 1
fi

THEME="$1"
PYVER="$2"

SOURCE_DIR="/home/sagemaker-user/gs-ds-env"
REQ_FILE="${SOURCE_DIR}/${THEME}/kernel/requirements.txt"
KERNEL_NAME="${3:-}"

if [[ -z "${THEME}" || -z "${PYVER}" || -z "${REQ_FILE}" ]]; then
  usage
  exit 1
fi

if [[ ! -f "${REQ_FILE}" ]]; then
  echo "[ERROR] requirements_file not found: ${REQ_FILE}"
  exit 1
fi

PYVER_TAG="$(echo "${PYVER}" | tr -d '.')"
if [[ -z "${KERNEL_NAME}" ]]; then
  KERNEL_NAME="${THEME}_${PYVER_TAG}"
fi

BASE_DIR="/home/sagemaker-user/.myenv"
KERNEL_DIR="${BASE_DIR}/${THEME}/kernel"
VENV_DIR="${KERNEL_DIR}/.venv"

# Ensure uv installed
if ! command -v uv >/dev/null 2>&1; then
  echo "[INFO] uv not found. Installing uv..."
  pip install uv
  # shellcheck disable=SC1090
  source "${HOME}/.bashrc" || true
fi

echo "[INFO] uv: $(uv --version)"

# Create dirs
mkdir -p "${KERNEL_DIR}"
cd "${KERNEL_DIR}"

# Create venv with explicit Python version (fix)
echo "[INFO] Creating venv with Python ${PYVER}: ${VENV_DIR}"
uv venv "${VENV_DIR}" --python "${PYVER}"

# Activate
# shellcheck disable=SC1090
source "${VENV_DIR}/bin/activate"

# Ensure tooling
uv pip install -U pip setuptools wheel

# Install deps
echo "[INFO] Installing requirements: ${REQ_FILE}"
uv pip install -r "${REQ_FILE}"

# Kernel registration
uv pip install ipykernel
echo "[INFO] Registering kernel: ${KERNEL_NAME}"
python -m ipykernel install --user --name "${KERNEL_NAME}" --display-name "${KERNEL_NAME}"

echo "[OK] Done."
echo "     Theme      : ${THEME}"
echo "     Python     : ${PYVER}"
echo "     Venv       : ${VENV_DIR}"
echo "     Kernel     : ${KERNEL_NAME}"
