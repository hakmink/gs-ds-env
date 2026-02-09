from sagemaker.estimator import Estimator

# 설정
REGION = "us-east-1"
ACCOUNT_ID = "155954279556"
BUCKET = "cheonhj-mlops-edu-202602"
IMAGE_URI = f"{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/cheonhj-titanic-training:latest"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/service-role/AmazonSageMaker-ExecutionRole-20260123T111313"

# Estimator 생성
estimator = Estimator(
    image_uri=IMAGE_URI,
    role=ROLE_ARN,
    instance_count=1,
    instance_type="ml.m5.large",
    output_path=f"s3://{BUCKET}/output",
    base_job_name="cheonhj-titanic",
    hyperparameters={
        "n_estimators": "100",
        "max_depth": "5",
        "random_state": "42",
    },
    max_run=3600,
    use_spot_instances=True,
    max_wait=7200,
)

# 학습 실행 (채널 기반 입력)
print("Starting training job...")
estimator.fit({
    "train": f"s3://{BUCKET}/titanic/train.csv",
    "validation": f"s3://{BUCKET}/titanic/val.csv",
})

print("Training complete!")
print(f"Training job name: {estimator.latest_training_job.name}")
print(f"Model artifacts: {estimator.model_data}")
