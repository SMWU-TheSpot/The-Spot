import pandas as pd
import numpy as np
import uuid
# Normalize를 위해 Scikit-learn을 사용
from sklearn.preprocessing import MinMaxScaler 

def azureml_main(dataframe1 = None, dataframe2 = None):
    df = dataframe1.copy()

    # 1. 컬럼명 정규화 및 특성 공학
    df.rename(columns={'시군구명': 'city', '행정동명': 'town', 
                       '상권업종대분류명': 'category_L1', '상권업종중분류명': 'category_L2',
                       '경도': 'lon', '위도': 'lat'}, inplace=True)
    df['store_count'] = df.groupby(['city', 'town', 'category_L1'])['lon'].transform('count')
    df_final = df.groupby(['city', 'town', 'category_L1']).agg(
        lat=('lat', 'mean'), lon=('lon', 'mean'), 
        total_store_count=('store_count', 'sum') 
    ).reset_index()

    # 2. Python 내에서 One-Hot Encoding 및 정규화 수행 (Select Columns 과정에서 생기는 오류 수정)
    
    # One-Hot Encoding (Indicator Values 생성)
    # city, town, category_L1를 제외한 나머지 특성만 인코딩에 사용
    df_encoded = pd.get_dummies(df_final, columns=['category_L1'], prefix='category_L1')
    
    # 3. 정규화 대상 특성 분리
    # ID/주소 필드는 정규화에서 제외
    features_to_normalize = ['lat', 'lon', 'total_store_count'] + [col for col in df_encoded.columns if col.startswith('category_L1_')]
    
    # 4. MinMaxScaler를 사용하여 정규화
    scaler = MinMaxScaler()
    df_encoded[features_to_normalize] = scaler.fit_transform(df_encoded[features_to_normalize])

    # 5. Cosmos DB 연동을 위한 메타데이터 및 ID 추가 (유일값 보장)
    df_encoded['id'] = [str(uuid.uuid4()) for _ in range(len(df_encoded))]
    df_encoded['cluster_id'] = -1
    df_encoded['cluster_feature'] = "분석 전"
    
    # 6. 군집화에 사용될 정규화된 특성만 최종 출력
    # 학습에 사용할 컬럼만 남기기
    output_cols_1 = ['id'] + features_to_normalize 
    output_cols_2 = ['id', 'city', 'town', 'lat', 'lon', 'total_store_count']

    # 첫 번째 포트: K-Means 학습에 사용할 정규화된 데이터
    # 두 번째 포트: Assign Data to Clusters에 필요한 원본 ID/주소 데이터
    return df_encoded[output_cols_1], df_encoded[output_cols_2]