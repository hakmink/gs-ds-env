import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import (
    mean_squared_error, 
    mean_absolute_error, 
    r2_score, 
    mean_squared_log_error, 
    explained_variance_score, 
    median_absolute_error
)
import wandb

# metric.py
import logging

# metric.py 모듈 로거 설정
logger = logging.getLogger(__name__)  # 모듈 로거 생성
logger.setLevel(logging.INFO)

# StreamHandler로 콘솔에 기본 출력 설정
if not logger.handlers:
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s\n%(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    
# 데이터 준비 함수
def predict(df, model, feature_cols, target_col, prefix=""):
    """
    모델 예측 결과를 포함한 데이터프레임 준비
    """
    try:
        df[f'pred_{prefix}'] = model.predict(df[feature_cols], num_iteration=model.best_iteration)
        df[f'diff_{prefix}'] = abs(df[target_col] - df[f'pred_{prefix}'])
        return df
    except Exception as e:
        logger.error(e)
        raise e

        
# 성능 지표 계산 함수
def calculate_metrics(y_true, y_pred):
    """
    성능 지표 계산
    """
    try:
        mse = mean_squared_error(y_true, y_pred)
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(y_true, y_pred)
        r2 = r2_score(y_true, y_pred)
        msle = mean_squared_log_error(y_true, y_pred)
        explained_variance = explained_variance_score(y_true, y_pred)
        median_ae = median_absolute_error(y_true, y_pred)

        return {
            "MSE": mse,
            "RMSE": rmse,
            "MAE": mae,
            "R2 Score": r2,
            "MSLE": msle,
            "Explained Variance": explained_variance,
            "Median AE": median_ae
        }
    except Exception as e:
        logger.error(e)
        raise e

        
# 성능 지표 로깅 함수
def log_metrics(metrics, prefix=""):
    """
    wandb에 성능 지표 로깅
    """
    try:
        metrics_prefixed = {f"{prefix}_{k}": v for k, v in metrics.items()}
        metrics_df = pd.DataFrame([metrics_prefixed])
        wandb.log({f"{prefix}_metrics": wandb.Table(dataframe=metrics_df)})
        return metrics_prefixed
    except Exception as e:
        logger.error(e)
        raise e

        
# 성능 지표 출력 함수
def print_metrics(metrics, prefix=""):
    """
    성능 지표 출력
    """
    a_msg = []
    try:
        a_msg.append(f"{prefix.upper()} Metrics:")
        for k, v in metrics.items():
            a_msg.append(f"{k}: {v:.4f}")
        msg = '\n'.join(a_msg)
        logger.info(msg)
        return msg
    except Exception as e:
        logger.error(e)
        raise e


# 성능 지표 차트 생성 함수
def plot_metrics_comparison(train_metrics, valid_metrics, test_metrics, folder_path):
    """
    MSE 및 나머지 성능 지표에 대한 비교 차트를 생성
    """
    try:
        metrics_names = list(train_metrics.keys())
        mse_values = [train_metrics["MSE"], valid_metrics["MSE"], test_metrics["MSE"]]

        # MSE 차트
        fig, ax = plt.subplots(1, 2, figsize=(8, 8))
        x_mse = ['Train', 'Validation', 'Test']
        ax[0].bar(x_mse, mse_values, color=['skyblue', 'lightgreen', 'salmon'])
        ax[0].set_title('Mean Squared Error (MSE) Comparison', fontsize=16)

        # 나머지 지표 차트
        x = np.arange(len(metrics_names) - 1)
        train_values = list(train_metrics.values())[1:]
        valid_values = list(valid_metrics.values())[1:]
        test_values = list(test_metrics.values())[1:]
        width = 0.25

        ax[1].bar(x - width, train_values, width, label='Train', color='skyblue')
        ax[1].bar(x, valid_values, width, label='Validation', color='lightgreen')
        ax[1].bar(x + width, test_values, width, label='Test', color='salmon')
        ax[1].set_xticks(x)
        ax[1].set_xticklabels(metrics_names[1:], rotation=45, ha='right', fontsize=12)
        ax[1].legend()

        plt.tight_layout()
        plt.savefig(f"{folder_path}/metric.png", dpi=100, bbox_inches='tight')
        plt.show()
        plt.close(fig)
    except Exception as e:
        logger.error(e)
        raise e


# 예측 결과와 실제값 비교 차트
def plot_actual_vs_predicted(test_df, index_col, target_col, prefix, folder_path):
    """
    실제값과 예측값을 비교하는 라인 차트 생성
    """
    try:
        plt.figure(figsize=(8, 8))
        plt.plot(test_df[index_col], test_df[target_col], label='Actual', color='blue')
        plt.plot(test_df[index_col], test_df[f'pred_{prefix}'], label='Predicted', color='red')
        plt.title('Actual vs Predicted SMP Value')
        plt.xlabel(f'{index_col}')
        plt.ylabel(f'{target_col}')
        plt.legend()
        plt.savefig(f"{folder_path}/forecasting_test.png", dpi=100, bbox_inches='tight')
        plt.show()
        plt.close()
    except Exception as e:
        logger.error(e)
        raise e    
    

# 전체 실행 예시 함수
def run_evaluation(train_df, valid_df, test_df, model, index_col, target_col, feature_cols, folder_path):
    """
    전체 평가 프로세스 실행
    """
    try:
        msg = {}
        train_df = predict(train_df, model, feature_cols, target_col, "train")
        valid_df = predict(valid_df, model, feature_cols, target_col, "valid")
        test_df = predict(test_df, model, feature_cols, target_col, "test")

        # 성능 지표 계산 및 로깅
        train_metrics = calculate_metrics(train_df[target_col], train_df['pred_train'])
        valid_metrics = calculate_metrics(valid_df[target_col], valid_df['pred_valid'])
        test_metrics = calculate_metrics(test_df[target_col], test_df['pred_test'])

        train_msg = print_metrics(train_metrics, "train")
        valid_msg = print_metrics(valid_metrics, "valid")
        test_msg = print_metrics(test_metrics, "test")

        # log_metrics(train_metrics, "train")
        # log_metrics(valid_metrics, "valid")
        # log_metrics(test_metrics, "test")

        # MSE 및 나머지 지표 비교 차트
        plot_metrics_comparison(train_metrics, valid_metrics, test_metrics, folder_path)

        # 실제값 vs 예측값 비교 차트
        prefix = 'test'
        plot_actual_vs_predicted(test_df, index_col, target_col, prefix, folder_path)
        
        msg = {
            'train_metrics': train_metrics,
            'valid_metric': valid_metrics,
            'test_metric': test_metrics,
        }
        return msg
    except Exception as e:
        logger.error(e)
        raise e
