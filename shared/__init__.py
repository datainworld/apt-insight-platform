"""
전 모듈 공통 코드 패키지
- DB 연결, 환경변수 로드 등
"""

from shared.config import (
    DATA_API_KEY, KAKAO_API_KEY,
    DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME,
    DATA_DIR,
)
from shared.db_engine import get_db_engine
