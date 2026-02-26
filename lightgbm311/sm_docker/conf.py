import pprint
pp = pprint.PrettyPrinter(indent=4)
import boto3
import os


log_table_name = 'automl-logs'
kernel_name = "conda_lightgbm311"


def get_info():
    # STS 클라이언트 생성
    sts_client = boto3.client('sts')

    # 현재 AWS 계정 ID 가져오기
    response = sts_client.get_caller_identity()
    target_account_id = response['Account']

    print(f"AWS Account ID: {target_account_id}")

    # Boto3 세션 생성
    session = boto3.Session()

    # 현재 세션의 리전 정보 가져오기
    target_region = session.region_name
    if target_region is None:
        target_region = 'us-east-1'

    print(f"AWS Region: {target_region}")
    
    data = {
        "account_id": target_account_id,
        "region_name": target_region,
    }
    return data