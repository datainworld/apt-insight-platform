"""
PostgreSQL DB 엔진 팩토리
"""

import sys
from sqlalchemy import create_engine
from shared.config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME


def get_db_engine():
    """PostgreSQL SQLAlchemy 엔진을 반환합니다."""
    try:
        conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(conn_str)
        return engine
    except Exception as e:
        print(f"DB 연결 오류: {e}")
        sys.exit(1)
