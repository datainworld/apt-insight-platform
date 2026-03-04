"""
공통 유틸리티 모듈
- API 요청 (fetch_data)
- CSV 저장 (save_to_csv)
- DB 연결 (get_db_engine)
- 파일 관리 (get_latest_file, get_today_str)
"""

import os
import glob
import requests
import xmltodict
import json
import pandas as pd
from dotenv import load_dotenv
import time
import sys
from datetime import datetime
from urllib.parse import unquote

# .env 파일 로드
load_dotenv()

DATA_API_KEY = os.getenv("DATA_API_KEY")
KAKAO_API_KEY = os.getenv("KAKAO_API_KEY")
DATA_DIR = "datas"

# PostgreSQL 설정
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)


# ==============================================================================
# API 유틸리티
# ==============================================================================

def get_api_key_decoded():
    """
    API 키가 URL 인코딩되어 있다면 디코딩하여 반환합니다.
    requests 모듈의 params 파라미터로 전달 시 requests가 자동으로 인코딩하므로
    디코딩된 키를 사용하는 것이 좋습니다.
    """
    if not DATA_API_KEY:
        raise ValueError("DATA_API_KEY가 .env 파일에 설정되지 않았습니다.")
    return unquote(DATA_API_KEY)


def fetch_data(url, params, method="GET", retries=5):
    """
    API 요청을 보내고 결과를 반환합니다.
    429 오류(Rate Limit) 발생 시 지수 백오프를 적용합니다.
    """
    params['serviceKey'] = get_api_key_decoded()
    
    for attempt in range(retries):
        try:
            response = requests.request(method, url, params=params, timeout=30)
            
            if response.status_code == 429:
                wait_time = (2 ** attempt) * 5
                print(f"Rate limit hit (429). Attempt {attempt+1}/{retries}. Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            
            response.raise_for_status()
            
            content_type = response.headers.get('Content-Type', '')
            text = response.text.strip()
            
            if 'xml' in content_type or text.startswith('<'):
                try:
                    return xmltodict.parse(text)
                except Exception as e:
                    print(f"XML Parsing Error: {e}")
                    return text
            elif 'json' in content_type:
                return response.json()
            else:
                return text
                
        except requests.exceptions.RequestException as e:
            print(f"Request failed (Attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2)
            else:
                print(f"Final failure for URL: {url}")
                return None


# ==============================================================================
# 파일 관리 유틸리티
# ==============================================================================

def save_to_csv(data_list, filename):
    """딕셔너리 리스트를 CSV 파일로 저장합니다."""
    if not data_list:
        print(f"No data to save for {filename}")
        return

    filepath = os.path.join(DATA_DIR, filename)
    df = pd.DataFrame(data_list)
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    print(f"Saved {len(df)} records to {filepath}")


def get_today_str():
    """오늘 날짜를 YYYYMMDD 형식으로 반환합니다."""
    return datetime.now().strftime("%Y%m%d")


def get_latest_file(pattern, exclude_today=False):
    """
    DATA_DIR에서 패턴에 맞는 최신 파일을 반환합니다.
    
    Args:
        pattern: glob 패턴 (예: "apt_code_*.csv")
        exclude_today: True이면 오늘 날짜 파일 제외
    """
    files = glob.glob(os.path.join(DATA_DIR, pattern))
    if not files:
        return None
    
    files.sort(key=os.path.getctime, reverse=True)
    
    if exclude_today:
        today_str = get_today_str()
        files = [f for f in files if today_str not in f]
    
    return files[0] if files else None


# ==============================================================================
# DB 유틸리티
# ==============================================================================

def get_db_engine():
    """PostgreSQL SQLAlchemy 엔진을 반환합니다."""
    try:
        from sqlalchemy import create_engine
        conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(conn_str)
        return engine
    except Exception as e:
        print(f"DB 연결 오류: {e}")
        sys.exit(1)
