import boto3
from jinja2 import Environment, FileSystemLoader
import os


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

    print(f"AWS Region: {target_region}")
    
    data = {
        "account_id": target_account_id,
        "region_name": target_region,
    }
    return data


def apply(template_filename, output_filename, data):
    # Jinja2 템플릿 환경 설정
    template_dir = "."  # 템플릿 파일이 있는 디렉토리
    env = Environment(loader=FileSystemLoader(template_dir))

    # 템플릿 파일 로드
    template = env.get_template(template_filename)

    # 템플릿 렌더링
    output = template.render(data)

    # 렌더링된 결과를 새 파일로 저장
    with open(output_filename, "w", encoding="utf-8") as file:
        file.write(output)

    print(f"템플릿이 렌더링되어 {output_filename} 파일로 저장되었습니다.")


def apply_dockerfile(data):
    template_filename = "Dockerfile.template"  # 템플릿 파일 이름
    output_filename = "Dockerfile"
    apply(template_filename, output_filename, data)
    

def apply_task_definition(data):
    template_filename = 'task-definition.template.json'
    output_filename = 'task-definition.json'
    apply(template_filename, output_filename, data)    
    
    
if __name__ == '__main__':
    # tempalte 에 전달할 data
    data = get_info()
    # Dockerfile 생성
    apply_dockerfile(data)
    # task-definition
    # apply_task_definition(data)