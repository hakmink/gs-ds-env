import pandas as pd
import json
import yaml
import os
import shutil

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError, BotoCoreError
from boto3.dynamodb.conditions import Key
import time
from datetime import datetime
import pytz
import zlib
import base64
import pickle
import traceback

# user
import conf

import pprint
pp = pprint.PrettyPrinter(indent=4)
    
conf_data = conf.get_info()
region_name = conf_data['region_name']

# S3 클라이언트 초기화
s3 = boto3.client('s3', region_name=region_name)


def get_secret_key(secret_name):

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
        raise e

    secret = json.loads(get_secret_value_response['SecretString'])

    # Your code goes here.
    return secret


def get_experiment_item(table_name, project_hashkey, file_hashkey):
    try:
        # DynamoDB 리소스 생성
        dynamodb = boto3.resource('dynamodb', region_name=region_name)

        # 테이블 객체 생성
        table = dynamodb.Table(table_name)

        # 키 조건 설정
        key = {
            'project_hashkey': project_hashkey,
            'experiment_hashkey': file_hashkey,
        }
        # 항목 조회
        response = table.get_item(Key=key)

        # 항목 반환
        return response.get('Item', None)
    except Exception as e:
        print(f"오류 발생: {e}")
        raise e 
        
        
def get_dataset_item(table_name, project_hashkey, file_hashkey):
    try:
        # DynamoDB 리소스 생성
        dynamodb = boto3.resource('dynamodb', region_name=region_name)

        # 테이블 객체 생성
        table = dynamodb.Table(table_name)

        # 키 조건 설정
        key = {
            'project_hashkey': project_hashkey,
            'file_hashkey': file_hashkey
        }
        # 항목 조회
        response = table.get_item(Key=key)

        # 항목 반환
        return response.get('Item', None)
    except Exception as e:
        print(f"오류 발생: {e}")
        raise e 
        
        
def get_model_repo_item(table_name, model_hashkey):
    try:
        # DynamoDB 리소스 생성
        dynamodb = boto3.resource('dynamodb', region_name=region_name)

        # 테이블 객체 생성
        table = dynamodb.Table(table_name)

        # 키 조건 설정
        key = {
            'model_hashkey': model_hashkey
        }
        # 항목 조회
        response = table.get_item(Key=key)

        # 항목 반환
        return response.get('Item', None)
    except Exception as e:
        print(f"오류 발생: {e}")
        raise e 
        
        
def put_item_to_ddb(table_name, item):
    """
    :param table_name: 조회할 DynamoDB 테이블 이름
    :param item: 딕셔너리 형태의 저장할 속성
    """
    try:
        dynamodb = boto3.resource('dynamodb', region_name=region_name)
        table = dynamodb.Table(table_name)

        table.put_item(Item=item)
        print(f"Success : {table_name} table에 item이 저장되었습니다.")

    except Exception as e:
        err_msg = traceback.format_exc()
        print(f"Error : {table_name} table에 item을 저장하지 못했습니다.\n{err_msg}")
        raise e
        
        
def check_record_exists(table_name, pk_key, pk_value, sk_key=None, sk_value=None):
    # DynamoDB 리소스 생성
    dynamodb = boto3.resource('dynamodb', region_name=region_name)
    table = dynamodb.Table(table_name)
    
    try:
        key = {
            pk_key: pk_value
        }
        if sk_key is not None and sk_key != '':
            key[sk_key] = sk_key
        # 특정 pk, sk 조합의 항목을 가져옴
        response = table.get_item(
            Key=key
        )
        
        # 항목 존재 여부 확인
        return 'Item' in response

    except ClientError as e:
        print(f"오류 발생: {e.response['Error']['Message']}")
        return False
    
    
def conv_ts_to_dt_str(ts):
    
    utc_time = datetime.utcfromtimestamp(ts)

    # Asia/Seoul 타임존 정보를 가져옴
    seoul_tz = pytz.timezone('Asia/Seoul')

    # UTC 시간을 Asia/Seoul 타임존으로 변환
    seoul_time = utc_time.replace(tzinfo=pytz.utc).astimezone(seoul_tz)

    # yyyy mm dd hh:mm 형식으로 출력
    formatted_time = seoul_time.strftime('%Y-%m-%d %H:%M:%S')      
    return formatted_time


def download_s3_files_to_directory(bucket_name: str, s3_prefix: str, local_dir: str = '.'):
    """
    S3 버킷의 prefix 하위에 있는 모든 파일을 로컬 디렉토리로 다운로드합니다.

    Parameters:
    - bucket_name (str): S3 버킷 이름
    - s3_prefix (str): 다운로드할 S3 키(prefix), 예: 'folder1/subfolder2/'
    - local_dir (str): 다운로드한 파일을 저장할 로컬 디렉토리
    """
    s3 = boto3.client('s3')
    
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix):
        for obj in page.get('Contents', []):
            s3_key = obj['Key']
            if s3_key.endswith('/'):
                continue  # 폴더는 스킵 (파일만 다운로드)

            relative_path = os.path.relpath(s3_key, s3_prefix)
            local_path = os.path.join(local_dir, relative_path)
            local_folder = os.path.dirname(local_path)

            os.makedirs(local_folder, exist_ok=True)
            print(f"Downloading s3://{bucket_name}/{s3_key} → {local_path}")
            s3.download_file(bucket_name, s3_key, local_path)
      
        
def download_s3_file_to_directory(bucket, key, directory=None):
    
    # 파일명 추출
    filename = os.path.basename(key)
    
    # 디렉토리 설정: 인자가 없으면 현재 디렉토리, 있으면 지정된 디렉토리 생성
    if directory:
        os.makedirs(directory, exist_ok=True)  # 디렉토리가 없으면 생성
        filepath = os.path.join(directory, filename)
    else:
        filepath = filename  # 현재 디렉토리로 설정
    
    # 파일 다운로드
    try:
        s3.download_file(bucket, key, filepath)
        print(f"{key} 로부터 {filename} 파일이 '{filepath}'에 다운로드되었습니다.")
        with open(filepath, "r", encoding="utf-8") as file:
            temp = file.read()
            print(temp)
    except Exception as e:
        print(f"S3 파일 다운로드 오류: {str(e)}")
        
        
def move_file_to_directory(source_path, destination_dir):
    """
    지정된 파일을 대상 디렉토리로 이동하는 함수.
    
    Parameters:
    source_path (str): 이동할 파일의 경로
    destination_dir (str): 파일을 이동할 대상 디렉토리
    
    Returns:
    str: 이동된 파일의 최종 경로
    """
    # 대상 디렉토리가 없으면 생성
    os.makedirs(destination_dir, exist_ok=True)
    
    # 이동할 파일의 최종 경로 설정
    destination_path = os.path.join(destination_dir, os.path.basename(source_path))
    
    # 파일 이동
    shutil.move(source_path, destination_path)
    print(f"{source_path} 파일이 {destination_path}로 이동되었습니다.")
    
    return destination_path


def upload_directory_to_s3(local_directory, bucket, s3_prefix):
    """
    로컬 디렉토리의 모든 파일을 S3의 지정된 버킷과 경로에 업로드하며, 업로드된 파일 목록을 계층적 구조로 반환합니다.
    
    Parameters:
    local_directory (str): 로컬 폴더 경로 (예: "artifacts")
    bucket (str): 대상 S3 버킷 이름
    s3_prefix (str): S3에서 파일을 저장할 시작 경로 (폴더 경로처럼 사용)

    Returns:
    dict: 업로드된 파일의 목록을 포함한 계층적 구조 (artifacts)
    """
    artifacts = {}

    for root, _, files in os.walk(local_directory):
        # S3에 저장될 폴더의 상대 경로
        relative_folder_path = os.path.relpath(root, local_directory)
        folder_key = os.path.join(s3_prefix, relative_folder_path).replace("\\", "/")

        # artifacts에 폴더 키가 없으면 초기화
        if folder_key not in artifacts:
            artifacts[folder_key] = []

        for file in files:
            # 로컬 파일 경로
            local_file_path = os.path.join(root, file)
            
            # S3에 저장할 파일 경로
            relative_file_path = os.path.relpath(local_file_path, local_directory)
            s3_file_path = os.path.join(s3_prefix, relative_file_path).replace("\\", "/")  # S3 경로에 슬래시 사용
            
            # 파일 업로드
            try:
                s3.upload_file(local_file_path, bucket, s3_file_path)
                print(f"Uploaded {local_file_path} to s3://{bucket}/{s3_file_path}")

                # artifacts 딕셔너리에 파일 추가
                artifacts[folder_key].append(file)
            except Exception as e:
                print(f"Failed to upload {local_file_path} to s3://{bucket}/{s3_file_path}: {e}")

    return artifacts


def print_tree(root_dir, indent=""):
    """
    현재 디렉토리 및 하위 구조를 tree처럼 출력합니다.

    Parameters:
        root_dir (str): 시작 디렉토리 경로
        indent (str): 내부 재귀에서 사용되는 들여쓰기 (초기 호출 시 비워둠)
    """
    items = sorted(os.listdir(root_dir))
    for i, item in enumerate(items):
        path = os.path.join(root_dir, item)
        is_last = (i == len(items) - 1)
        prefix = "└── " if is_last else "├── "
        print(indent + prefix + item)
        if os.path.isdir(path):
            next_indent = indent + ("    " if is_last else "│   ")
            print_tree(path, next_indent)