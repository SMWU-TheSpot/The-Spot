import pandas as pd
import os
import uuid
import numpy as np
from azure.cosmos import CosmosClient
from dotenv import load_dotenv

# 1. 환경 변수 로드
load_dotenv()
ENDPOINT = os.environ.get("COSMOS_DB_ENDPOINT")
KEY = os.environ.get("COSMOS_DB_KEY")
DATABASE_ID = "thespotDB"

# 2. 파일별 설정 (컨테이너명, 파티션키 필드명, ID로 쓸 필드 조합)
FILES_CONFIG = {
    # 1. 군집별 요약
    "cluster_summary.csv": {
        "container": "ClusterSummary", 
        "pk_col": "cluster_id", 
        "id_cols": ["cluster_id"] 
    },
    # 2. 도시별 군집
    "cluster_city_summary.csv": {
        "container": "ClusterCitySummary", 
        "pk_col": "city", 
        "id_cols": ["city", "cluster_id"] 
    },
    # 3. 동 단위 요약
    "town_summary.csv": {
        "container": "TownSummary", 
        "pk_col": "city", 
        "id_cols": ["city", "town"] 
    }
}

def clean_row(row):
    cleaned = {}
    for k, v in row.items():
        # NaN이나 무한대 값 체크
        if pd.isna(v) or v == np.inf or v == -np.inf:
            cleaned[k] = None
        else:
            cleaned[k] = v
    return cleaned

def upload_file(client, filename, config):
    container_name = config["container"]
    pk_col = config["pk_col"]
    id_cols = config["id_cols"]
    
    print(f"\n처리 중: {filename} -> {container_name}")

    try:
        # CSV 읽기
        df = pd.read_csv(filename)
        print(f"   - CSV 로드 성공: {len(df)}행")
    except Exception as e:
        print(f"   *** 파일 읽기 실패: {e}")
        return

    try:
        database = client.get_database_client(DATABASE_ID)
        container = database.get_container_client(container_name)
    except Exception as e:
        print(f"   *** 컨테이너 접속 실패: {e}")
        return

    success = 0
    # DataFrame을 순회하며 업로드
    for idx, row_raw in df.iterrows():
        try:
            # 1. 데이터 클렌징 (NaN 제거)
            item = clean_row(row_raw)

            # 2. 파티션 키 검증 (데이터에 파티션 키가 없으면 건너뜀)
            if pk_col not in item or not item[pk_col]:
                # None만 체크
                if item[pk_col] is None: 
                    print(f"     *** 파티션 키({pk_col}) 누락. 건너뜀.")
                    continue

            # 3. ID 생성
            # 설정된 컬럼들을 조합해서 고유 ID를 만듦 (예: "서울시_강남구")
            # 만약 컬럼 값이 없으면 랜덤 UUID 사용
            try:
                unique_parts = [str(item[col]) for col in id_cols]
                generated_id = "_".join(unique_parts)
            except:
                generated_id = str(uuid.uuid4())
            
            item['id'] = generated_id

            # 4. 타입 강제 변환
            # cluster_id나 수치형 데이터 정수 처리
            if 'cluster_id' in item and item['cluster_id'] is not None:
                try:
                    item['cluster_id'] = int(float(item['cluster_id']))
                except:
                    pass

            # 5. 업로드 (body만 전달)
            container.upsert_item(item)
            success += 1
            
            if success % 100 == 0:
                print(f"     ... {success}건 완료")

        except Exception as e:
            print(f"     *** 행 업로드 실패: {e}")

    print(f"   --- {container_name} 최종 완료 ({success}건) ---")

def main():
    if not ENDPOINT or not KEY:
        print("*** .env 파일을 확인해주세요.")
        return

    print("----- Cosmos DB 업로드 시작 -----")
    client = CosmosClient(ENDPOINT, KEY)

    for filename, config in FILES_CONFIG.items():
        if os.path.exists(filename):
            upload_file(client, filename, config)
        else:
            print(f"\n*** 파일 없음: {filename} (경로 확인 필요)")

    print("\n--- 모든 작업 종료 ---")

if __name__ == "__main__":
    main()
