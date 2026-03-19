#!/bin/bash

# 인자 체크
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 {create|update}"
  exit 1
fi

command_type=$1
echo $command_type

# 변수 설정
layer_name="python-requests-layer"
function_name="email-notify"
role_name="RoleStack-automlcdkprodAmazonSageMakerExecutionRole-wsLsYVkWFDmo"
# role_name="AmazonSageMaker-ExecutionRole-20260211T204344"

# AWS_REGION 환경 변수 체크 및 기본값 설정
if [ -z "$AWS_REGION" ]; then
  # AWS CLI 설정에서 region 가져오기 시도
  region=$(aws configure get region)
  if [ -z "$region" ]; then
    echo "error: AWS_REGION 환경 변수가 설정되지 않았고, AWS CLI 기본 region도 없습니다."
    echo "다음 중 하나를 실행하세요:"
    echo "  export AWS_REGION=us-west-2"
    echo "  또는 aws configure set region us-west-2"
    exit 1
  fi
else
  region=$AWS_REGION
fi

echo "region: $region"

# account_id 조회
account_id=$(aws sts get-caller-identity --query Account --output text --region "$region")
if [ -z "$account_id" ]; then
  echo "error: AWS account ID를 가져올 수 없습니다. AWS 자격증명을 확인하세요."
  exit 1
fi

role_arn="arn:aws:iam::$account_id:role/$role_name"
zip_file="function.zip"

# zip 파일 생성
cd ./src
zip function.zip main.py

# 최신 버전의 layer arn 조회
echo "fetching latest version of layer arn..."
layer_arn=$(aws lambda list-layer-versions --layer-name "$layer_name" \
  --query 'LayerVersions[0].LayerVersionArn' --output text --region "$region")

if [ -z "$layer_arn" ] || [ "$layer_arn" == "None" ]; then
  echo "error: unable to fetch layer arn."
  exit 1
fi
echo "latest layer arn: $layer_arn"

if [ "$command_type" == "create" ]; then
  echo "creating lambda function..."
  aws lambda create-function --function-name "$function_name" \
    --zip-file "fileb://$zip_file" --handler main.lambda_handler --runtime python3.12 \
    --role "$role_arn" --layers "$layer_arn" --region "$region" --timeout 120 \
    --environment "Variables={REGION_NAME=$region}"

  if [ $? -eq 0 ]; then
    echo "lambda function created successfully."
  else
    echo "error: failed to create lambda function."
    exit 1
  fi

elif [ "$command_type" == "update" ]; then
  echo "updating lambda function code..."
  aws lambda update-function-code --function-name "$function_name" \
    --zip-file "fileb://$zip_file" --region "$region"

  if [ $? -eq 0 ]; then
    echo "lambda function code updated successfully."
  else
    echo "error: failed to update lambda function code."
    exit 1
  fi

  sleep 20

  echo "updating lambda function configuration..."
  aws lambda update-function-configuration --function-name "$function_name" \
    --handler main.lambda_handler --layers "$layer_arn" --region "$region" --timeout 120 \
    --environment "Variables={REGION_NAME=$region}"

  if [ $? -eq 0 ]; then
    echo "lambda function configuration updated successfully."
  else
    echo "error: failed to update lambda function configuration."
    exit 1
  fi

else
  echo "Invalid command_type: $command_type. Use 'create' or 'update'."
  exit 1
fi