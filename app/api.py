"""FastAPI 查询接口。"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from . import config
from .collectors import BinanceCollector, UpbitCollector
from .db import get_session
from .models import NewsFlash
from .schemas import NewsFlashOut, NewsListResponse

router = APIRouter()


@router.get("/api/news/latest", response_model=NewsListResponse)
def get_latest_news(
    limit: int = Query(default=config.API_DEFAULT_LIMIT, ge=1, le=100),
    session: Session = Depends(get_session),
) -> NewsListResponse:
    """按 published_at 倒序返回最新 limit 条快讯。"""
    rows = session.execute(
        select(NewsFlash).order_by(NewsFlash.published_at.desc()).limit(limit)
    ).scalars().all()
    items = [NewsFlashOut.model_validate(row) for row in rows]
    return NewsListResponse(total=len(items), items=items)


@router.get("/api/collect/exchanges")
def collect_exchanges():
    """手动触发采集 Binance + Upbit 公告并返回结果（测试用，不落库）。"""
    results = []
    for collector in (BinanceCollector(), UpbitCollector()):
        try:
            flashes = collector.fetch()
            results.append(
                {
                    "source": collector.source,
                    "ok": True,
                    "count": len(flashes),
                    "items": [f.to_dict() for f in flashes],
                }
            )
        except Exception as e:  # noqa: BLE001
            results.append(
                {
                    "source": collector.source,
                    "ok": False,
                    "count": 0,
                    "error": f"{type(e).__name__}: {e}",
                    "items": [],
                }
            )
    return {"results": results}
