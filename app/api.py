"""FastAPI 查询接口。"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from . import config
from .db import get_session
from .models import NewsFlash
from .schemas import NewsFlashOut, NewsListResponse

router = APIRouter()


@router.get("/api/news/latest", response_model=NewsListResponse)
def get_latest_news(
    limit: int = Query(default=config.API_DEFAULT_LIMIT, ge=1, le=100),
    session: Session = Depends(get_session),
) -> NewsListResponse:
    """按 published_at 倒序返回两个平台合计最新的 limit 条快讯。"""
    rows = session.execute(
        select(NewsFlash).order_by(NewsFlash.published_at.desc()).limit(limit)
    ).scalars().all()
    items = [NewsFlashOut.model_validate(row) for row in rows]
    return NewsListResponse(total=len(items), items=items)
