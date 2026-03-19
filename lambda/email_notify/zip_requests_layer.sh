#/bin/bash

# Conda 설치 경로 설정
CONDA_BASE="/home/ec2-user/SageMaker/.myenv/miniconda"

# ENV
source ~/user-default-efs/.envs/streamlit312/.venv/bin/activate

pip install requests pytz -t python

zip -r python.zip python

aws lambda publish-layer-version --layer-name python-requests-layer \
    --zip-file fileb://python.zip \
    --compatible-runtimes python3.12 \
    --compatible-architectures "x86_64"
    
rm -rf ./python
rm -f python.zip
