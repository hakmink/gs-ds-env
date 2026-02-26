import argparse
import json
import os
import tarfile
import yaml
import joblib
import lightgbm as lgb
import pandas as pd
from sklearn.metrics import accuracy_score


def train_model(X_train, y_train, hyperparameters):
    """
    모델 학습
    
    Args:
        X_train: 학습 특징 데이터
        y_train: 학습 타겟 데이터
        hyperparameters: global_params.yaml 에 정의된 모델 파라미터
    
    Returns:
        model: 학습된 모델
    """
    print("🎯 Training LightGBM tree model...")

    model = lgb.LGBMClassifier(**hyperparameters)
    model.fit(X_train, y_train)

    print("✅ Model training completed!")

    return model



def save_model(model, model_dir, model_name="model.joblib"):
    os.makedirs(model_dir, exist_ok=True)
    model_path = os.path.join(model_dir, model_name)
    joblib.dump(model, model_path)
    print(f"✅ Model saved: {model_path}")

    artifact_path = os.path.join(model_dir, "model.tar.gz")
    with tarfile.open(artifact_path, "w:gz") as tar:
        tar.add(model_dir, arcname=".")
    print(f"✅ Model artifact created: {artifact_path}")
    
    return artifact_path


def upload_to_s3(artifact_path, bucket, model_prefix):
    try:
        import boto3  # type: ignore
    except ImportError as exc:
        raise RuntimeError("boto3 is required for S3 upload") from exc

    key = model_prefix + "/" + os.path.basename(artifact_path)
    s3 = boto3.client("s3")
    s3.upload_file(artifact_path, bucket, key)
    uploaded_uri = f"s3://{bucket}/{key}"
    print(f"✅ Model artifact uploaded: {uploaded_uri}")
    return uploaded_uri




