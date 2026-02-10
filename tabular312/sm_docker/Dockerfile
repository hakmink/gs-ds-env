# Dockerfile
FROM 155954279556.dkr.ecr.us-east-1.amazonaws.com/gs-automl-base-containers/tabular312:1.0

RUN apt-get update && apt-get install -y libbz2-dev graphviz

RUN mkdir -p /opt/ml/code/

# 작업 디렉토리 설정
WORKDIR /opt/ml/

# 필요한 파일 복사
COPY run_pm.py run_pm_utils.py conf.py /opt/ml/code/
COPY test.csv train.csv gender_submission.csv titanic-competition-step-by-step-using-xgboost.ipynb /opt/ml/code/

# 필요한 패키지 설치 (예: requirements.txt가 있을 경우)
COPY requirements.txt /opt/ml/
    
# requirements 설치
RUN pip install --no-deps --only-binary :all: -r /opt/ml/requirements.txt \
    || pip install --no-deps --prefer-binary -r /opt/ml/requirements.txt \
    || pip install --no-deps -r /opt/ml/requirements.txt || true 
# RUN pip install -U pytz mlflow jinja2 sagemaker sagemaker-experiments sagemaker-training sagemaker-core wordcloud awswrangler  pyarrow sqlalchemy psycopg2-binary
RUN pip install -U --no-deps -r /opt/ml/requirements.txt

# ipykernel을 설치하고 커널 등록
RUN pip install ipykernel && \
    python -m ipykernel install --user --name conda_tabular312 --display-name "conda_tabular312"


# 기본 명령어 설정
ENV SAGEMAKER_PROGRAM run_pm.py
ENV SAGEMAKER_SUBMIT_DIRECTORY=/opt/ml/code

# 권장: 코드 디렉토리가 PATH에 잡히면 디버깅이 쉬움
ENV PATH="/opt/ml/code:${PATH}"