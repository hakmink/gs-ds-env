import pandas as pd
from sklearn.model_selection import train_test_split
from pathlib import Path


def split_data():
    # 데이터 로드
    df = pd.read_csv("data/train.csv")

    # 80:20 split (stratify by Survived for balanced split)
    train_df, val_df = train_test_split(
        df, test_size=0.2, random_state=42, stratify=df["Survived"]
    )

    # output 폴더 생성
    output_dir = Path("data/output")
    output_dir.mkdir(exist_ok=True)

    # 저장
    train_df.to_csv(output_dir / "train.csv", index=False)
    val_df.to_csv(output_dir / "val.csv", index=False)

    print(f"Original: {len(df)} rows")
    print(f"Train: {len(train_df)} rows ({len(train_df)/len(df)*100:.1f}%)")
    print(f"Val: {len(val_df)} rows ({len(val_df)/len(df)*100:.1f}%)")
    print(f"Saved to {output_dir}/")


if __name__ == "__main__":
    split_data()
