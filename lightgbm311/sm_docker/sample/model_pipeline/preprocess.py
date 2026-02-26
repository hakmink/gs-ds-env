import argparse
import os
import pandas as pd
import yaml
import boto3
from io import BytesIO



def load_data(bucket,data_prefix):
    """
    데이터 로딩
    
    Args:
        data_path: 원본 데이터 폴더 경로

    Returns:
        df: 전체 데이터프레임
    """
    key = f"{data_prefix}/train.csv"
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(BytesIO(obj["Body"].read()))
        
    print(f"🔍 Data shape: {df.shape}")
    print(f"🔍 Columns: {list(df.columns)}")
    return df

def preprocess_data(df):
    """
    데이터 전처리
    
    Args:
        df: 원본 데이터프레임
    
    Returns:
        df: 전처리한 데이터프레임

    """
    
    df = df.copy()

    df = df.rename(columns={
        'PassengerId': 'passenger_id',
        'Survived': 'target',
        'Pclass': 'pclass',
        'Name': 'name',
        'Sex': 'sex',
        'Age': 'age',
        'SibSp': 'sibsp',
        'Parch': 'parch',
        'Ticket': 'ticket',
        'Fare': 'fare',
        'Cabin': 'cabin',
        'Embarked': 'embarked',
    })

    
    # 기본 결측치 처리 + 타입 기준 단순 전처리
    numeric_cols = df.select_dtypes(include="number").columns
    object_cols = df.select_dtypes(exclude="number").columns

    for col in numeric_cols:
        if df[col].isnull().any():
            df[col] = df[col].fillna(0)
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    for col in object_cols:
        if df[col].isnull().any():
            if df[col].dropna().empty:
                df[col] = df[col].fillna("")
            else:
                df[col] = df[col].fillna(df[col].mode()[0])
        # 범주형 컬럼은 간단히 숫자 인코딩
        df[col] = df[col].astype(str)
        df[col] = pd.factorize(df[col])[0]
    
    
    print(f"🔍 Features shape: {df.shape}")
    print(f"🔍 Features: {list(df.columns)}")
    
    return df


def save_preprocessed(df, output_dir, filename):
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    df.to_csv(output_path, index=False)
    print(f"💾 Saved: {output_path}")



