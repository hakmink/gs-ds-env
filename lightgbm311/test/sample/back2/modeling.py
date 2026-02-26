import os
import lightgbm as lgb
import pandas as pd
import matplotlib.pyplot as plt
import wandb
from wandb.integration.lightgbm import wandb_callback, log_summary


# 1. 데이터셋 준비 함수
def prepare_datasets(train_df, valid_df, target_col, feature_cols):
    """
    LGBM 모델 훈련에 사용할 데이터셋을 준비하는 함수.

    :param train_df: 학습 데이터 데이터프레임 (특징 컬럼과 타겟 컬럼 포함)
    :param valid_df: 검증 데이터 데이터프레임 (특징 컬럼과 타겟 컬럼 포함)
    :param target_col: 타겟 변수 컬럼 이름
    :param feature_cols: 사용할 특징 컬럼 목록
    :return: (dtrain, dvalid) LGBM 데이터셋
    """
    # LGBM 데이터셋 생성
    dtrain = lgb.Dataset(data=train_df[feature_cols], label=train_df[target_col])
    dvalid = lgb.Dataset(data=valid_df[feature_cols], label=valid_df[target_col])
    
    return dtrain, dvalid


# 2. 모델 훈련 함수
def train_lgbm_model(train_df, valid_df, target_col, feature_cols, params, boosting):
    """
    LGBM 회귀 모델을 훈련하는 함수.

    :param train_df: 학습 데이터 데이터프레임 (특징 컬럼과 타겟 컬럼 포함)
    :param valid_df: 검증 데이터 데이터프레임 (특징 컬럼과 타겟 컬럼 포함)
    :param target_col: 타겟 변수 컬럼 이름
    :param feature_cols: 사용할 특징 컬럼 목록
    :param params: 모델 하이퍼파라미터
    :param boosting: 부스팅 훈련 제어 파라미터
    :return: 훈련된 LGBM 모델
    """
    # prepare dataset
    dtrain, dvalid = prepare_datasets(train_df, valid_df, target_col, feature_cols)
    
    
    # Wandb 설정 업데이트
    # wandb.config.update(params)
    
    # boosting default setting
    num_boost_round = boosting.get('num_boost_round', 1000)
    early_stopping_rounds = boosting.get('early_stopping_rounds', 50)

    # 모델 훈련
    # num_boost_round는 params에 포함하지 않고 
    # lgb.train() 함수의 인자로 별도로 지정해야 합니다. 
    # 이유는 num_boost_round가 LightGBM의 훈련 반복 횟수를 설정하는 매개변수로, 
    # 하이퍼파라미터가 아닌 훈련 제어 파라미터이기 때문입니다.
    model = lgb.train(
        params,
        dtrain,
        num_boost_round=num_boost_round,                 # 부스팅 반복 횟수
        valid_sets=dvalid,
        valid_names=('validation',),
        callbacks=[
            lgb.early_stopping(stopping_rounds=early_stopping_rounds), # 조기 종료 조건
            # wandb_callback(),
        ]
    )

    return model

# 3. 모델 성능 및 피처 중요도 기록 함수
def log_model_summary(model):
    """
    모델 성능 요약 및 피처 중요도를 wandb에 기록하는 함수.

    :param model: 훈련된 LGBM 모델
    """
    log_summary(model, save_model_checkpoint=True)

    
# 4. 피처 중요도 시각화 함수
def plot_feature_importance(final_importance_df, save_path="metric_images/feature_importance.png", max_features=20):
    
    # 폴더 생성
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    # Final_Importance 기준으로 내림차순 정렬
    df_sorted = final_importance_df[:max_features].sort_values(by='Final_Importance', ascending=True)

    # 스택형 막대 차트 생성
    plt.figure(figsize=(8,7))

    # Gain Importance 막대 생성
    plt.barh(df_sorted['Feature'], df_sorted['Gain_Importance'], color='skyblue', label='Gain Importance')

    # Split Importance를 Gain Importance 위에 스택으로 추가
    plt.barh(df_sorted['Feature'], df_sorted['Split_Importance'], left=df_sorted['Gain_Importance'], color='salmon', label='Split Importance')

    # 그래프 설정
    plt.xlabel('Importance')
    plt.ylabel('Feature')
    plt.title('Feature Importance Comparison (Stacked Gain and Split)')
    plt.legend(loc='upper right')
    plt.grid(axis='both', linestyle='--', color='gray', alpha=0.7)  # x축 그리드 표시
    plt.tight_layout()

    # 차트를 파일로 저장
    plt.savefig(save_path, dpi=100)
    plt.show()


# 5. 피처 중요도 데이터프레임 생성 함수
def create_feature_importance_df(model, feature_cols, importance_type='gain'):
    """
    모델의 피처 중요도를 데이터프레임으로 생성하는 함수.

    :param model: 훈련된 LGBM 모델
    :param feature_cols: 모델 훈련에 사용한 피처 이름 목록
    :param importance_type: 피처 중요도 유형 ('gain' 또는 'split')
    :return: 피처 중요도 데이터프레임
    """
    importance = model.feature_importance(importance_type=importance_type)
    importance_df = pd.DataFrame({
        'Feature': feature_cols,
        'Importance': importance
    }).sort_values(by='Importance', ascending=False)
    return importance_df


def combine_feature_importance(importance_gain_df, importance_split_df):
    """
    Gain과 Split 피처 중요도를 결합하여 최종 피처 중요도를 계산하는 함수.
    
    :param importance_gain_df: Gain 기준 피처 중요도 데이터프레임
    :param importance_split_df: Split 기준 피처 중요도 데이터프레임
    :return: 결합된 최종 피처 중요도 데이터프레임
    """
    # Gain과 Split 중요도 결합을 위해 Feature 기준으로 데이터프레임 병합
    combined_df = importance_gain_df.merge(
        importance_split_df, on='Feature', suffixes=('_gain', '_split')
    )

    # Gain과 Split 각각 정규화 (합이 1이 되도록)
    combined_df['Gain_Importance'] = combined_df['Importance_gain'] / combined_df['Importance_gain'].sum()
    combined_df['Split_Importance'] = combined_df['Importance_split'] / combined_df['Importance_split'].sum()

    # Gain과 Split 중요도의 평균을 최종 중요도로 계산
    combined_df['Final_Importance'] = (combined_df['Gain_Importance'] + combined_df['Split_Importance']) / 2

    # 최종 중요도를 기준으로 내림차순 정렬
    combined_df = combined_df.sort_values(by='Final_Importance', ascending=False)
    
    return combined_df[['Feature', 'Final_Importance', 'Gain_Importance', 'Split_Importance']]

