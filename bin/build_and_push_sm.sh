#!/bin/bash -e
export PYTHONWARNINGS="ignore"
# ============================================================
# SageMaker Studio Notebook - Conda 환경 설정 스크립트
# 환경 위치: ~/SageMaker/.myenv/miniconda/envs/<env_name>
# ============================================================
# 도움말 함수 정의
show_help() {
    echo "Usage: $0 <env_name> [version]"
    echo
    echo "Arguments:"
    echo "  env_name           Conda environment name"
    echo "  version            version (default: 1.0)"
    echo
    echo "Example:"
    echo "  $0 streamlit312"
    echo "  $0 streamlit312 2.0"
}
# 인자 검증
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_help
    exit 0
fi
if [ $# -lt 1 ] || [ $# -gt 2 ]; then
    echo "Error: env_name은 필수입니다." >&2
    show_help
    exit 1
fi
ENV_NAME="$1"
VERSION="${2:-1.0}"

# gse-dxlab account id
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region --output text)
REPO_NAME="gs-automl-base-containers/${ENV_NAME}_sm"

aws ecr get-login-password --region ${REGION} \
  | docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

cp delete_untagged_images.py gen_dockerfile.py "../$ENV_NAME/sm_docker/"

cd "../$ENV_NAME/sm_docker"

python gen_dockerfile.py

docker build -f Dockerfile -t $REPO_NAME .

docker tag $REPO_NAME $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME:$VERSION

$(aws ecr get-login --no-include-email --registry-ids $ACCOUNT_ID)

aws ecr describe-repositories --repository-names $REPO_NAME || aws ecr create-repository --repository-name $REPO_NAME

docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME:$VERSION

sleep 10
python delete_untagged_images.py --repository_name ${REPO_NAME} --region ${REGION}