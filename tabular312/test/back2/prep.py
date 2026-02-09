import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split


def split_train_test_chronologically(df, datetime_column, valid_size=0.1, test_size=0.1):
    """
    주어진 데이터프레임을 datetime 컬럼을 기준으로 시간 순서에 따라 train/test로 분할하는 함수.
    
    :param df: 분할할 데이터프레임
    :param datetime_column: datetime 기준 컬럼 이름
    :param valid_size: 학습 평가 데이터의 비율 (기본값은 10%)
    :param test_size: 테스트 데이터의 비율 (기본값은 10%)
    :return: (train_df, valid_df, test_df)로 분할된 데이터프레임 튜플
    """
    # datetime_column이 datetime 형식이 아니면 변환
    if not pd.api.types.is_datetime64_any_dtype(df[datetime_column]):
        df[datetime_column] = pd.to_datetime(df[datetime_column], errors='coerce')

    # 데이터프레임을 datetime_column 기준으로 정렬
    df = df.sort_values(by=datetime_column)

    # 전체 데이터의 길이
    total_len = len(df)

    # 학습 데이터의 끝 인덱스 계산
    test_start_idx = int(total_len * (1 - test_size))
    valid_end_idx = int(test_start_idx * (1-valid_size))

    # 데이터 분할
    train_df = df.iloc[:valid_end_idx]
    valid_df = df.iloc[valid_end_idx:test_start_idx]
    test_df = df.iloc[test_start_idx:]

    return train_df, valid_df, test_df


def split_train_test_randomly(df, target_col, seed, valid_size=0.1, test_size=0.1):
    """
    데이터를 랜덤하게 train/validation 데이터로 분할하는 함수.

    :param df: 분할할 데이터프레임
    :param target_col: 타겟 변수 컬럼 이름
    :param seed: 랜덤 시드 값 (random_state)
    :param valid_size: 학습 평가 데이터의 비율 (기본값은 10%)
    :param test_size: 테스트 데이터의 비율 (기본값은 10%)
    :return: (train_df, valid_df, test_df)로 분할된 데이터프레임 튜플
    """
    # 타겟 변수 분리
    label_df = df[target_col]
    
    # 타겟과 data_type을 제외한 특징 변수들 분리
    feature_df = df.drop([target_col], axis=1)

    # train_test_split으로 데이터를 랜덤하게 train/validation으로 분할
    X_train_valid, X_test, y_train_valid, y_test = train_test_split(
        feature_df, 
        label_df, 
        test_size=test_size, 
        random_state=seed
    )
    
    # train_df 생성
    X_train_valid_df = pd.concat([X_train_valid, y_train_valid.rename(target_col)], axis=1)

    # valid_df 생성
    test_df = pd.concat([X_test, y_test.rename(target_col)], axis=1)
    
    # train_test_split으로 데이터를 랜덤하게 train/validation으로 분할
    X_train, X_valid, y_train, y_valid = train_test_split(
        X_train_valid, 
        y_train_valid.rename(target_col), 
        test_size=valid_size, 
        random_state=seed
    )    
    
    # train_df 생성
    train_df = pd.concat([X_train, y_train.rename(target_col)], axis=1)

    # valid_df 생성
    valid_df = pd.concat([X_valid, y_valid.rename(target_col)], axis=1)
    
    return train_df, valid_df, test_df


def split_train_test_manually(df, type_col='data_type', train='train', valid='valid', test='test'):
    
    train_df = df[df[type_col] == train]
    valid_df = df[df[type_col] == valid]
    test_df = df[df[type_col] == test]
    
    return train_df, valid_df, test_df


def convert_column_type(df, column_name, data_type):
    """
    데이터프레임의 특정 컬럼을 지정된 타입으로 변환하는 함수.

    :param df: 변환할 데이터프레임
    :param column_name: 변환할 컬럼의 이름
    :param data_type: 변환할 데이터 타입 (int, float, datetime, string)
    :return: 변환된 데이터프레임
    """
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' does not exist in the DataFrame.")
    
    if data_type == 'int':
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce').astype('Int64')  # NaN 처리 가능
    elif data_type == 'float':
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce').astype(float)
    elif data_type == 'datetime':
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
    elif data_type == 'string':
        df[column_name] = df[column_name].astype(str)
    else:
        raise ValueError(f"Unsupported data type: {data_type}. Choose from 'int', 'float', 'datetime', 'string'.")
    
    return df


def filter_missing_values(df, target_col):
    """
    주어진 데이터프레임에서 특정 컬럼에 있는 결측값(NA, NaN, null, 빈 문자열 등)을 필터링하는 함수.
    
    :param df: pandas 데이터프레임
    :param target_col: 필터링할 컬럼 이름
    :return: 결측값이 제거된 데이터프레임
    """
    # 결측값으로 간주할 값들
    missing_values = ['NA', 'N/A', '', ' ', 'null', 'None']

    # target_col에 있는 결측값들을 필터링
    df_cleaned = df[~df[target_col].isin(missing_values)]

    # NaN 값도 함께 필터링
    df_cleaned = df_cleaned.dropna(subset=[target_col])

    return df_cleaned



# import pandas as pd

# def chronological_split(train_valid_df, target_col, datetime_col, test_size=0.2):
#     """
#     데이터를 datetime_column을 기준으로 시간 순서대로 split하여 train/validation 데이터로 나누는 함수.

#     :param train_valid_df: 입력 데이터프레임 (특징과 타겟 컬럼 포함)
#     :param target_col: 타겟 변수 컬럼 이름
#     :param datetime_col: 시간 순으로 split할 datetime 컬럼 이름
#     :param test_size: validation 데이터의 비율 (기본값 0.2)
#     :return: (tr_x, tr_y, va_x, va_y)
#     """
#     # datetime_col이 datetime 형식인지 확인 후 변환
#     if not pd.api.types.is_datetime64_any_dtype(train_valid_df[datetime_col]):
#         train_valid_df[datetime_col] = pd.to_datetime(train_valid_df[datetime_col], errors='coerce')
    
#     # datetime_col을 기준으로 데이터프레임을 정렬
#     train_valid_df = train_valid_df.sort_values(by=datetime_col)

#     # 전체 데이터 길이
#     total_len = len(train_valid_df)
    
#     # validation 데이터의 시작 인덱스 계산
#     split_idx = int(total_len * (1 - test_size))

#     # feature 데이터와 label 데이터를 분리
#     label_df = train_valid_df[target_col]
#     train_df = train_valid_df.drop([target_col, 'data_type'], axis=1)
    
#     # 시간 순서대로 train/validation 분할
#     tr_x = train_df.iloc[:split_idx]
#     tr_y = label_df.iloc[:split_idx]
#     va_x = train_df.iloc[split_idx:]
#     va_y = label_df.iloc[split_idx:]

#     return tr_x, tr_y, va_x, va_y

# # 사용 예시
# # conf['DATA']['train_valid_split']에 해당하는 test_size 비율을 사용
# tr_x, tr_y, va_x, va_y = chronological_split(train_valid_df, target_col='target', datetime_col='datetime', test_size=conf['DATA']['train_valid_split'])




# from sklearn.model_selection import train_test_split

# def split_train_valid_test(df, target_col, test_size, seed):
#     """
#     df의 data_type 컬럼을 기준으로 train_valid와 test를 나누고,
#     train_valid를 다시 train/valid로 나누는 함수.

#     :param df: 입력 데이터프레임 (data_type 컬럼 포함)
#     :param target_col: 타겟 변수 컬럼 이름
#     :param test_size: train_valid를 다시 train/valid로 나눌 때 validation 데이터 비율
#     :param seed: 랜덤 시드 값 (random_state)
#     :return: (tr_x, tr_y, va_x, va_y, test_x, test_y)로 분할된 데이터프레임
#     """
#     # test 데이터 분리
#     test_df = df[df['data_type'] == 'test']
#     test_x = test_df.drop([target_col, 'data_type'], axis=1)
#     test_y = test_df[target_col]

#     # train_valid 데이터 분리
#     train_valid_df = df[df['data_type'].isin(['train', 'valid'])]
#     label_df = train_valid_df[target_col]
#     train_df = train_valid_df.drop([target_col, 'data_type'], axis=1)

#     # train_valid 데이터를 train과 valid로 분할
#     X_train, X_valid, y_train, y_valid = train_test_split(
#         train_df, 
#         label_df, 
#         test_size=test_size, 
#         random_state=seed
#     )

#     # 결과 반환
#     tr_x = X_train
#     tr_y = y_train
#     va_x = X_valid
#     va_y = y_valid

#     return tr_x, tr_y, va_x, va_y, test_x, test_y

# # 사용 예시
# tr_x, tr_y, va_x, va_y, test_x, test_y = split_train_valid_test(
#     df=train_valid_df, 
#     target_col='target', 
#     test_size=conf['DATA']['train_valid_split'], 
#     seed=seed
# )



# from sklearn.model_selection import train_test_split

# def random_split(train_valid_df, target_col, test_size, seed):
#     """
#     데이터를 랜덤하게 train/validation 데이터로 분할하는 함수.

#     :param train_valid_df: 입력 데이터프레임 (특징과 타겟 컬럼 포함)
#     :param target_col: 타겟 변수 컬럼 이름
#     :param test_size: validation 데이터의 비율
#     :param seed: 랜덤 시드 값 (random_state)
#     :return: (tr_x, tr_y, va_x, va_y)로 분할된 데이터프레임과 타겟 값들
#     """
#     # 타겟 변수 분리
#     label_df = train_valid_df[target_col]
    
#     # 타겟과 data_type을 제외한 특징 변수들 분리
#     train_df = train_valid_df.drop([target_col, 'data_type'], axis=1)

#     # train_test_split으로 데이터를 랜덤하게 train/validation으로 분할
#     X_train, X_valid, y_train, y_valid = train_test_split(
#         train_df, 
#         label_df, 
#         test_size=test_size, 
#         random_state=seed
#     )

#     # 결과 반환
#     tr_x = X_train
#     tr_y = y_train
#     va_x = X_valid
#     va_y = y_valid

#     return tr_x, tr_y, va_x, va_y

# # 사용 예시
# # conf['DATA']['train_valid_split']에 해당하는 test_size 비율과 seed를 사용
# tr_x, tr_y, va_x, va_y = random_split(
#     train_valid_df=train_valid_df, 
#     target_col='target', 
#     test_size=conf['DATA']['train_valid_split'], 
#     seed=seed
# )
