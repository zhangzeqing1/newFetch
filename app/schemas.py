"""Pydantic 响应模型。"""
from datetime import datetime

from pydantic import BaseModel, ConfigDict


class NewsFlashOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    source: str
    title: str
    content: str
    published_at: datetime
    url: str
    collected_at: datetime


class NewsListResponse(BaseModel):
    total: int
    items: list[NewsFlashOut]
