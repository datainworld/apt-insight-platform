"""
멀티 에이전트 공유 상태(State) 스키마 정의
LangGraph v1 기반 — TypedDict로 그래프의 공유 상태를 관리합니다.
"""

from typing import Annotated, Literal
from typing_extensions import TypedDict
from langgraph.graph.message import add_messages


class AgentState(TypedDict):
    """Supervisor 그래프의 공유 상태"""
    # 대화 메시지 히스토리 (LangGraph add_messages reducer 사용)
    messages: Annotated[list, add_messages]
    # 현재 작업을 위임받은 에이전트
    next_agent: str
    # SQL 쿼리 결과 (SQL Agent → Analysis Agent 전달용)
    query_result: str | None
    # 최종 응답 완료 여부
    is_complete: bool
