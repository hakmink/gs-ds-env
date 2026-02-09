import argparse
import json
import os
from pathlib import Path

import joblib
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
from sklearn.preprocessing import LabelEncoder


def parse_args():
    parser = argparse.ArgumentParser()
    # SageMaker는 "train" 명령어를 인수로 전달
    parser.add_argument("command", nargs="?", default="train")
    # 하이퍼파라미터
    parser.add_argument("--n_estimators", type=int, default=100)
    parser.add_argument("--max_depth", type=int, default=5)
    parser.add_argument("--random_state", type=int, default=42)
    # SageMaker 환경 변수
    parser.add_argument("--model_dir", type=str, default=os.environ.get("SM_MODEL_DIR", "/opt/ml/model"))
    parser.add_argument("--train", type=str, default=os.environ.get("SM_CHANNEL_TRAIN", "/opt/ml/input/data/train"))
    parser.add_argument("--validation", type=str, default=os.environ.get("SM_CHANNEL_VALIDATION", "/opt/ml/input/data/validation"))
    return parser.parse_args()


def load_data(train_path, val_path):
    train_file = Path(train_path) / "train.csv"
    val_file = Path(val_path) / "val.csv"

    train_df = pd.read_csv(train_file)
    val_df = pd.read_csv(val_file)

    return train_df, val_df


def preprocess(df, label_encoders=None, fit=False):
    df = df.copy()

    # 사용할 피처
    features = ["Pclass", "Sex", "Age", "SibSp", "Parch", "Fare", "Embarked"]

    # 결측치 처리
    df["Age"] = df["Age"].fillna(df["Age"].median())
    df["Fare"] = df["Fare"].fillna(df["Fare"].median())
    df["Embarked"] = df["Embarked"].fillna(df["Embarked"].mode()[0])

    # 범주형 인코딩
    categorical_cols = ["Sex", "Embarked"]

    if label_encoders is None:
        label_encoders = {}

    for col in categorical_cols:
        if fit:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col].astype(str))
            label_encoders[col] = le
        else:
            df[col] = label_encoders[col].transform(df[col].astype(str))

    X = df[features]
    y = df["Survived"] if "Survived" in df.columns else None

    return X, y, label_encoders


def train():
    args = parse_args()

    print("=" * 50)
    print("Titanic Survival Prediction Training")
    print("=" * 50)
    print(f"Hyperparameters:")
    print(f"  n_estimators: {args.n_estimators}")
    print(f"  max_depth: {args.max_depth}")
    print(f"  random_state: {args.random_state}")
    print(f"Train path: {args.train}")
    print(f"Validation path: {args.validation}")
    print(f"Model dir: {args.model_dir}")
    print("=" * 50)

    # 데이터 로드
    print("Loading data...")
    train_df, val_df = load_data(args.train, args.validation)
    print(f"Train samples: {len(train_df)}")
    print(f"Validation samples: {len(val_df)}")

    # 전처리
    print("Preprocessing...")
    X_train, y_train, label_encoders = preprocess(train_df, fit=True)
    X_val, y_val, _ = preprocess(val_df, label_encoders=label_encoders, fit=False)

    # 모델 학습
    print("Training model...")
    model = lgb.LGBMClassifier(
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        random_state=args.random_state,
        verbose=-1
    )
    model.fit(X_train, y_train)

    # 평가
    print("Evaluating...")
    train_pred = model.predict(X_train)
    val_pred = model.predict(X_val)
    val_pred_proba = model.predict_proba(X_val)[:, 1]

    metrics = {
        "train_accuracy": accuracy_score(y_train, train_pred),
        "val_accuracy": accuracy_score(y_val, val_pred),
        "val_f1": f1_score(y_val, val_pred),
        "val_auc": roc_auc_score(y_val, val_pred_proba)
    }

    print("=" * 50)
    print("Metrics:")
    for k, v in metrics.items():
        print(f"  {k}: {v:.4f}")
    print("=" * 50)

    # 모델 저장
    model_dir = Path(args.model_dir)
    model_dir.mkdir(parents=True, exist_ok=True)

    model_path = model_dir / "model.joblib"
    encoders_path = model_dir / "label_encoders.joblib"
    metrics_path = model_dir / "metrics.json"

    joblib.dump(model, model_path)
    joblib.dump(label_encoders, encoders_path)
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"Model saved to {model_path}")
    print(f"Label encoders saved to {encoders_path}")
    print(f"Metrics saved to {metrics_path}")
    print("Training complete!")


if __name__ == "__main__":
    train()
