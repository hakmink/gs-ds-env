# Dockerfile
FROM 155954279556.dkr.ecr.us-east-1.amazonaws.com/gs-automl-base-containers/boilerplate312:sean-base

ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_DEFAULT_TIMEOUT=100

RUN apt-get update && apt-get install -y libbz2-dev graphviz

RUN mkdir -p /opt/ml/code/

WORKDIR /opt/ml/

# run_pm.py만 복사 (단일 파일 구조)
COPY run_pm.py /opt/ml/code/

# requirements 설치
COPY requirements.txt /opt/ml/
ENV PIP="pip"
RUN ${PIP} install --no-cache-dir --no-deps --only-binary :all: -r /opt/ml/requirements.txt \
    || ${PIP} install --no-cache-dir --no-deps --prefer-binary -r /opt/ml/requirements.txt \
    || ${PIP} install --no-cache-dir --no-deps -r /opt/ml/requirements.txt \
    || ${PIP} install -r /opt/ml/requirements.txt

# ipykernel 설치 및 커널 등록
# 커널 이름은 run_pm.py 안 노트북에서 사용하는 커널과 맞춰야 함
RUN pip install ipykernel && \
    python -m ipykernel install --user --name boilerplate312 --display-name "boilerplate312"

ENV SAGEMAKER_PROGRAM=run_pm.py
ENV SAGEMAKER_SUBMIT_DIRECTORY=/opt/ml/code
ENV PATH="/opt/ml/code:${PATH}"