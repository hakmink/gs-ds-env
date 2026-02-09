# gse-dxlab account id
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region --output text)
REPO_NAME=gs-automl-base-containers/streamlit312
VERSION=1.0

docker build -f Dockerfile -t $REPO_NAME .

docker tag $REPO_NAME $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME:$VERSION

$(aws ecr get-login --no-include-email --registry-ids $ACCOUNT_ID)

aws ecr describe-repositories --repository-names $REPO_NAME || aws ecr create-repository --repository-name $REPO_NAME

docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME:$VERSION