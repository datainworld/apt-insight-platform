"""
에이전트 설정 (모델, 파라미터)
"""

import os
from shared.config import OPENAI_API_KEY, GOOGLE_API_KEY

# LLM 모델 설정
DEFAULT_MODEL = os.getenv("AGENT_MODEL", "gemini-2.0-flash")
TEMPERATURE = float(os.getenv("AGENT_TEMPERATURE", "0"))

# SQL 에이전트 설정
SQL_MAX_ROWS = int(os.getenv("SQL_MAX_ROWS", "100"))  # 쿼리 결과 최대 행 수
SQL_READONLY = True  # SELECT 쿼리만 허용

# NotebookLM MCP 설정
NOTEBOOKLM_MCP_COMMAND = os.getenv("NOTEBOOKLM_MCP_COMMAND", "notebooklm-mcp")
