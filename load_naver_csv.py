# load_naver_csv.py
import pandas as pd
from utils import get_db_engine
from collect_naver_listing import save_to_db, ensure_naver_schema

def load_csv_to_db():
    engine = get_db_engine()
    
    # 1. 스키마 초기화 (테이블이 없을 경우 CREATE TABLE IF NOT EXISTS 실행)
    ensure_naver_schema(engine)
    
    # 2. CSV 파일 읽기 (경로는 맞게 수정)
    print("CSV 파일 읽는 중...")
    df_complex = pd.read_csv("datas/naver_complex_20260303.csv")
    df_listing = pd.read_csv("datas/naver_listing_20260303.csv")
    
    # 3. DB에 데이터 보냄
    print("DB 적재 시작...")
    results = save_to_db(engine, df_complex, df_listing)
    
    print(f"완료: 단지 {results.get('complex', 0)}건, 매물 {results.get('listing', 0)}건 적재")

if __name__ == "__main__":
    load_csv_to_db()
