FROM python:3.12-slim

WORKDIR /opt/program

# 시스템 의존성 (LightGBM에 필요한 libgomp)
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*

# Python 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 학습 스크립트 복사
COPY train.py .

# SageMaker는 train 명령어로 실행
ENTRYPOINT ["python", "train.py"]
