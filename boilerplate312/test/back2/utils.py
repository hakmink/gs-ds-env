import yaml
import boto3
from botocore.exceptions import ClientError
import json
import os
import shutil
from time import strftime

import logging

# metric.py 모듈 로거 설정
logger = logging.getLogger(__name__)  # 모듈 로거 생성
logger.setLevel(logging.INFO)

# StreamHandler로 콘솔에 기본 출력 설정
if not logger.handlers:
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s\n%(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


def load_yaml(path: str) -> dict:
    """yaml 파일을 로드한다.
    
    :param path: 로드 경로
    :return: config 정보
    """
    
    with open(path, 'r') as f:
        return yaml.safe_load(f)
    

def get_secret(secret_name, secret_key, region_name):

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        logger.error(e)
        raise e

    secret = get_secret_value_response['SecretString']
    secret = json.loads(secret)

    # Your code goes here.
    return secret[secret_key]


def download_s3_file(s3_url, local_folder):
    # s3:// 형식의 URL을 파싱하여 버킷 이름과 파일 경로 추출
    if not s3_url.startswith("s3://"):
        raise ValueError("S3 URL 형식이 올바르지 않습니다.")
    
    # s3:// 이후의 부분을 분리하여 버킷 이름과 파일 경로를 추출
    s3_url_parts = s3_url[5:].split('/', 1)
    bucket_name = s3_url_parts[0]
    s3_key = s3_url_parts[1]

    # 파일명 추출
    file_name = os.path.basename(s3_key)
    local_file_path = os.path.join(local_folder, file_name)

    # S3 클라이언트 생성
    s3 = boto3.client('s3')

    try:
        # 타겟 디렉토리
        os.makedirs(local_folder, exist_ok=True)
        # 파일 다운로드
        s3.download_file(bucket_name, s3_key, local_file_path)
        logger.info(f"File downloaded successfully: {local_file_path}")
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        raise e

    return local_file_path


def init_wandb(conf: dict):
    
    try:
        """Initialize wandb.

        :param conf: 모델 학습을 위한 메타정보
        :return: 학습 과정을 wandb 정보에 전달
        """

        #-- LOGIN
        secret_name = conf['WANDB']['secret_name']
        secret_key = conf['WANDB']['secret_key']
        region_name = conf['WANDB']['region_name']
        wandb_key = get_secret(secret_name, secret_key, region_name)
        wandb.login(key=wandb_key)

        algo = conf['WANDB']['algorithm_name']
        model_suffix = conf['WANDB']['model_suffix']

        #-- PARAMS
        wandb_config = dict (
          architecture = algo,
          dataset_id = conf['WANDB']['wandb_name'],
          infra = conf['WANDB']['infra'],
        )

        #-- INIT
        wandb_run = wandb.init(
            project= conf['WANDB']['project'],
            name = "{}-{}".format(conf['WANDB']['wandb_name'], strftime('%Y%m%d-%H%M%S')),
            job_type=conf['WANDB']['job_type'],
            notes=f'{algo}-{model_suffix}',
            tags=conf['WANDB']['tags'],
            config=wandb_config,
        )

        return wandb   
    except Exception as e:
        logger.error(e)
        raise e


def close_wandb(wandb):
    try:
        wandb.finish()
    except Exception as e:
        logger.error(e)
        raise e

        
def create_clean_folder(folder_path):
    try:
        # 폴더가 이미 존재하면 삭제
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)

        # 빈 폴더 생성
        os.makedirs(folder_path, exist_ok=True)
    except Exception as e:
        logger.error(e)
        raise e