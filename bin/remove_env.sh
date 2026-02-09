#!/bin/bash -e
export PYTHONWARNINGS="ignore"

# ============================================================
# SageMaker Studio Notebook - Conda 환경 설정 스크립트
# 환경 위치: ~/SageMaker/.myenv/miniconda/envs/<env_name>
# ============================================================

# 도움말 함수 정의
show_help() {
    echo "Usage: $0 <env_name>"
    echo
    echo "Arguments:"
    echo "  env_name           Conda environment name"
    echo
    echo "Example:"
    echo "  $0 streamlit312"
}

# 인자 검증
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_help
    exit 0
fi

if [ $# -ne 1 ]; then
    echo "Error: 1개의 인자가 필요합니다." >&2
    show_help
    exit 1
fi

ENV_NAME="$1"
CONDA_ENV_NAME="$1"


echo "start...."
export CRYPTOGRAPHY_OPENSSL_NO_LEGACY=1
openssl version

WORKING_DIR="/home/ec2-user/SageMaker/.myenv"

echo "============================================"
echo " 환경 이름      : $ENV_NAME"
echo " 환경 경로      : $WORKING_DIR/miniconda/envs/$ENV_NAME"
echo "============================================"

mkdir -p "${WORKING_DIR}"

# ============================================================
# Conda 설정 및 기존 환경 정리
# ============================================================
conda config --set solver classic

if conda env list | grep -q "^$ENV_NAME "; then
    echo "🗑️  기존 환경 삭제 중: $ENV_NAME"
    conda env remove -n "$ENV_NAME" -y
    rm -rf "$WORKING_DIR/miniconda/envs/$ENV_NAME"
fi

if jupyter kernelspec list 2>/dev/null | grep -q "conda_${CONDA_ENV_NAME}"; then
    echo "🗑️  기존 커널 삭제 중: conda_$CONDA_ENV_NAME"
    jupyter kernelspec uninstall "conda_$CONDA_ENV_NAME" -y
fi

echo '######################################'
echo 'Done'

# ============================================================
# envs_dirs 등록 및 커널 확인
# ============================================================
echo ""
echo "🔧 envs_dirs 등록 및 커널 확인..."

conda config --add envs_dirs "$WORKING_DIR/miniconda/envs"
conda env list
jupyter kernelspec list

echo '######################################'
echo 'Cleanup Done'

# ============================================================
# 완료
# ============================================================
echo ""
echo "============================================"
echo " 🎉 삭제 완료!"
echo ""
echo " 커널 이름 : $ENV_NAME / conda_$ENV_NAME"
echo " 환경 경로 : $WORKING_DIR/miniconda/envs/$ENV_NAME"
echo ""
echo "============================================"