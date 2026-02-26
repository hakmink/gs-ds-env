import argparse
import json
import os
import yaml
import joblib
import pandas as pd
from sklearn.metrics import accuracy_score


def load_validation_df(val_path):
    if os.path.isdir(val_path):
        val_path = os.path.join(val_path, "validation.csv")
    return pd.read_csv(val_path)


def load_model(model_path, model_name):
    if os.path.isdir(model_path):
        model_path = os.path.join(model_path, model_name)
    return joblib.load(model_path)


def evaluate_model(model, X_val, y_val):
    preds = model.predict(X_val)
    acc = accuracy_score(y_val, preds)
    print(f"✅ Validation Accuracy: {acc:.4f}")
    return {"accuracy": acc}


def save_metrics(metrics, output_dir, filename="evaluation.json"):
    os.makedirs(output_dir, exist_ok=True)
    metrics_path = os.path.join(output_dir, filename)
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"✅ Metrics saved: {metrics_path}")
    return metrics_path



