# Dockerfile
FROM 155954279556.dkr.ecr.us-east-1.amazonaws.com/gs-automl-base-containers/lightgbm311:1.0

ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_DEFAULT_TIMEOUT=100

RUN apt-get update && apt-get install -y libbz2-dev graphviz

RUN mkdir -p /opt/ml/code/

# 작업 디렉토리 설정
WORKDIR /opt/ml/

# 필요한 파일 복사
COPY run_pm.py run_pm_utils.py conf.py /opt/ml/code/
COPY train_titanic_lightgbm.ipynb /opt/ml/code/

# 필요한 패키지 설치 (예: requirements.txt가 있을 경우)
COPY requirements.txt /opt/ml/
    
# requirements 설치
RUN pip install --no-cache-dir --no-deps --only-binary :all: -r /opt/ml/requirements.txt \
    || pip install --no-cache-dir --no-deps --prefer-binary -r /opt/ml/requirements.txt \
    || pip install --no-cache-dir --no-deps -r /opt/ml/requirements.txt 
# RUN pip install -U pytz mlflow jinja2 sagemaker sagemaker-experiments sagemaker-training sagemaker-core wordcloud awswrangler  pyarrow sqlalchemy psycopg2-binary
# RUN pip install -U -r /opt/ml/requirements.txt

# ipykernel을 설치하고 커널 등록
RUN pip install ipykernel && \
    python -m ipykernel install --user --name conda_lightgbm311 --display-name "conda_lightgbm311"


# 기본 명령어 설정
ENV SAGEMAKER_PROGRAM run_pm.py
ENV SAGEMAKER_SUBMIT_DIRECTORY=/opt/ml/code

# 권장: 코드 디렉토리가 PATH에 잡히면 디버깅이 쉬움
ENV PATH="/opt/ml/code:${PATH}"