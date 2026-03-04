"""
FastAPI 웹 애플리케이션 메인 서버
"""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI(
    title="APT Insight Platform",
    description="아파트 실거래가·매물 AI 분석 플랫폼",
    version="0.1.0",
)

# 정적 파일 서빙
app.mount("/static", StaticFiles(directory="webapp/static"), name="static")

# 템플릿 엔진
templates = Jinja2Templates(directory="webapp/templates")


@app.get("/health")
async def health_check():
    """헬스 체크 엔드포인트"""
    return {"status": "ok"}
