#!/bin/bash -e
export PYTHONWARNINGS="ignore"

# ============================================================
# SageMaker Studio Notebook - Conda 환경 설정 스크립트
# 환경 위치: ~/SageMaker/.myenv/miniconda/envs/<env_name>
# ============================================================

# 도움말 함수 정의
show_help() {
    echo "Usage: $0 <env_name> <requirements_path> <python_version>"
    echo
    echo "Arguments:"
    echo "  env_name           Conda environment name"
    echo "  requirements_path  Path to requirements.txt"
    echo "  python_version     Python version (e.g. 3.12)"
    echo
    echo "Example:"
    echo "  $0 streamlit312 /home/ec2-user/SageMaker/gs-ds-env/streamlit312/kernel/requirements.classic.txt 3.12"
}

# 인자 검증
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_help
    exit 0
fi

if [ $# -ne 3 ]; then
    echo "Error: 3개의 인자가 필요합니다." >&2
    show_help
    exit 1
fi

ENV_NAME="$1"
REQUIREMENTS="$2"
PYTHON_VERSION="$3"

if [ ! -f "$REQUIREMENTS" ]; then
    echo "Error: requirements.txt not found: $REQUIREMENTS" >&2
    exit 1
fi

echo "start...."
export CRYPTOGRAPHY_OPENSSL_NO_LEGACY=1
openssl version

WORKING_DIR="/home/ec2-user/SageMaker/.myenv"

echo "============================================"
echo " 환경 이름      : $ENV_NAME"
echo " Python 버전    : $PYTHON_VERSION"
echo " requirements   : $REQUIREMENTS"
echo " 환경 경로      : $WORKING_DIR/miniconda/envs/$ENV_NAME"
echo "============================================"

mkdir -p "${WORKING_DIR}"

# ============================================================
# [1/5] Miniconda 설치
# ============================================================
echo ""
echo "📦 [1/5] Miniconda 설치 중..."

wget https://repo.anaconda.com/miniconda/Miniconda3-py312_25.3.1-1-Linux-x86_64.sh \
    -O "$WORKING_DIR/miniconda.sh" --no-check-certificate

bash "$WORKING_DIR/miniconda.sh" -b -u -p "$WORKING_DIR/miniconda"

echo "before source.."
source "$WORKING_DIR/miniconda/etc/profile.d/conda.sh"
conda init bash

echo "✅ Miniconda 설치 완료"

# ============================================================
# Conda 설정 및 기존 환경 정리
# ============================================================
conda config --set solver classic

if conda env list | grep -q "^$ENV_NAME "; then
    echo "🗑️  기존 환경 삭제 중: $ENV_NAME"
    conda env remove -n "$ENV_NAME" -y
    rm -rf "$WORKING_DIR/miniconda/envs/$ENV_NAME"
fi

# ============================================================
# [2/5] Conda 환경 생성
# ============================================================
echo ""
echo "📦 [2/5] Conda 환경 생성 중: $ENV_NAME (Python $PYTHON_VERSION)"

conda create -n "$ENV_NAME" python="$PYTHON_VERSION" -y --quiet
conda config --append envs_dirs "$WORKING_DIR/miniconda/envs"

echo "✅ Conda 환경 생성 완료"

# ============================================================
# [3/5] 환경 활성화
# ============================================================
echo ""
echo "🔄 [3/5] 환경 활성화 중..."

source $WORKING_DIR/miniconda/etc/profile.d/conda.sh
conda init bash
conda activate "$ENV_NAME"

echo "✅ 환경 활성화 완료 ($(python --version))"

# ============================================================
# [4/5] uv로 requirements.txt 패키지 설치
# ============================================================
echo ""
echo "📥 [4/5] uv로 패키지 설치 중..."

if ! command -v uv &> /dev/null; then
    echo "   uv 설치 중..."
    pip install uv --quiet
fi

uv pip install -r "$REQUIREMENTS"

echo "✅ 패키지 설치 완료"

# ============================================================
# [5/5] ipykernel로 Jupyter 커널 등록
# ============================================================
echo ""
echo "🔧 [5/5] Jupyter 커널 등록 중..."

uv pip install ipykernel

python -m ipykernel install \
    --user \
    --name "$ENV_NAME" \
    --display-name "$ENV_NAME (Python $PYTHON_VERSION)"

echo "✅ 커널 등록 완료"

# ============================================================
# 완료
# ============================================================
echo ""
echo "============================================"
echo " 🎉 설정 완료!"
echo ""
echo " 커널 이름 : $ENV_NAME"
echo " 환경 경로 : $WORKING_DIR/miniconda/envs/$ENV_NAME"
echo ""
echo " 수동 활성화:"
echo "   source $WORKING_DIR/miniconda/etc/profile.d/conda.sh"
echo "   conda activate $ENV_NAME"
echo ""
echo " Jupyter에서 커널 '$ENV_NAME' 을 선택하세요."
echo "============================================"