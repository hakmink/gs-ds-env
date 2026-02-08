#!/bin/bash -e

# 도움말 함수 정의
show_help() {
    echo "Usage: $0 <env_name>"
    echo
    echo "Arguments:"
    echo "  env_name           Conda environment name"
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


export CRYPTOGRAPHY_OPENSSL_NO_LEGACY=1
openssl version
# sudo apt-get install libssl3

WORKING_DIR="/home/ec2-user/SageMaker/.myenv"


echo '######################################'
echo "ENV Name: $ENV_NAME"


# fix an issue for displaying plotly
# jupyter labextension install jupyterlab-plotly

echo '######################################'
echo 'start init, activate, ipykernel install'

source "$WORKING_DIR/miniconda/etc/profile.d/conda.sh"
conda init bash
conda activate "$WORKING_DIR/miniconda/envs/$ENV_NAME"
conda info --envs

# python -m ipykernel install --user --name=$CONDA_ENV_NAME
python -m ipykernel install --user --name=conda_$CONDA_ENV_NAME

echo '######################################'
echo 'Done'




# Add the following env dir to envs_dirs
conda config --add envs_dirs "$WORKING_DIR/miniconda/envs"

# Activate the kernel by list the envs
conda env list
jupyter kernelspec list

# Optional
#sudo initctl restart jupyter-server --no-wait
#python -m ipykernel install --user --name $ENV_NAME --display-name "$ENV_NAME"


echo '######################################'
echo 'start cleanup'

# Cleanup
conda deactivate
source "${WORKING_DIR}/miniconda/bin/deactivate"



conda activate $ENV_NAME

echo '######################################'
echo 'Cleanup Done'

# 스크립트 종료
exit 0