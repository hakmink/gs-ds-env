#!/bin/bash -e
export PYTHONWARNINGS="ignore"
# ============================================================
# run_pm Docker 이미지 빌드 & ECR 푸시 스크립트
# ============================================================
show_help() {
    echo "Usage: $0 <env_name> [my_version]"
    echo
    echo "Arguments:"
    echo "  env_name       Docker 이미지 이름 (예: run-pm-titanic)"
    echo "  my_version     이미지 버전 (default: 1.0)"
    echo
    echo "Example:"
    echo "  $0 run-pm-titanic"
    echo "  $0 run-pm-titanic 2.0"
}
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
MY_VERSION="${2:-1.0}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region --output text)
REPO_NAME="gs-automl-base-containers/${ENV_NAME}"

# ↓ 스크립트 위치 기준으로 상위 디렉토리(project 루트) 경로 계산
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_CONTEXT="${SCRIPT_DIR}/.."

echo "============================================================"
echo "  ENV_NAME    : ${ENV_NAME}"
echo "  MY_VERSION  : ${MY_VERSION}"
echo "  ACCOUNT_ID  : ${ACCOUNT_ID}"
echo "  REGION      : ${REGION}"
echo "  REPO_NAME   : ${REPO_NAME}"
echo "  BUILD_CONTEXT: ${BUILD_CONTEXT}"
echo "============================================================"
# ECR 로그인
aws ecr get-login-password --region ${REGION} \
  | docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com
# 베이스 이미지 ECR도 로그인 (다른 계정 베이스 이미지 pull용)
aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin 155954279556.dkr.ecr.us-east-1.amazonaws.com
# Docker 빌드
echo "▶ Building Docker image..."
docker build -f "${SCRIPT_DIR}/Dockerfile" -t ${REPO_NAME} "${BUILD_CONTEXT}"
# 태그
docker tag ${REPO_NAME} ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:${MY_VERSION}
docker tag ${REPO_NAME} ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:latest
# ECR 레포 없으면 생성
aws ecr describe-repositories --repository-names ${REPO_NAME} \
  || aws ecr create-repository --repository-name ${REPO_NAME}
# 푸시
echo "▶ Pushing to ECR..."
docker push ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:${MY_VERSION}
docker push ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:latest
echo "============================================================"
echo "✅ 완료!"
echo "  IMAGE_URI: ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:${MY_VERSION}"
echo "============================================================"