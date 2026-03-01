#/bin/bash

# Conda 설치 경로 설정
CONDA_BASE="/home/ec2-user/SageMaker/.myenv/miniconda"
CONDA_ENV_NAME=tabular312_langchain

# Conda 초기화
source $CONDA_BASE/etc/profile.d/conda.sh
conda init bash

python -m ipykernel install --user --name=$CONDA_ENV_NAME
python -m ipykernel install --user --name=conda_$CONDA_ENV_NAME

# Add the following env dir to envs_dirs
conda config --add envs_dirs "$CONDA_BASE/envs"

# Activate the kernel by list the envs
conda env list
jupyter kernelspec list

conda activate $CONDA_ENV_NAME

# Optional
#sudo initctl restart jupyter-server --no-wait
#python -m ipykernel install --user --name $ENV_NAME --display-name "$ENV_NAME"

cd ../docker

# font install
sudo yum install -y fontconfig
wget https://hangeul.pstatic.net/hangeul_static/webfont/NanumGothic/NanumGothic.ttf
sudo mkdir -p /usr/share/fonts/nanum/
sudo mv NanumGothic.ttf /usr/share/fonts/nanum/
fc-cache -fv

# 실행
python main.py \
    --table_name="automl-regression-experiment" \
    --project_hashkey="2ee07a49" \
    --experiment_hashkey="fc973d19" \
    --dataset_table_name="automl-dataset" \
    --model_repo_table_name="automl-model-repo" \
    --username="sean@gs.co.kr" \
    --task_token="abc"