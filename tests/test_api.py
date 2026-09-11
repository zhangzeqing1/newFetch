"""FastAPI 查询接口测试。"""
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import get_session
from app.models import Base, NewsFlash
from main import app


@pytest.fixture()
def session():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    s = Session()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for i in range(5):
        s.add(
            NewsFlash(
                source="techflowpost" if i % 2 == 0 else "odaily",
                title=f"快讯 {i}",
                content=f"内容 {i}",
                published_at=now - timedelta(minutes=i),
                url=f"https://example.com/{i}",
                collected_at=now,
            )
        )
    s.commit()
    yield s
    s.close()


@pytest.fixture()
def client(session):
    def override():
        yield session

    app.dependency_overrides[get_session] = override
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def test_latest_default_limit(client):
    resp = client.get("/api/news/latest")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 5
    assert len(data["items"]) == 5
    # published_at 倒序：第 0 条最新
    assert data["items"][0]["title"] == "快讯 0"


def test_latest_limit(client):
    resp = client.get("/api/news/latest?limit=2")
    data = resp.json()
    assert data["total"] == 2
    assert len(data["items"]) == 2
    assert data["items"][0]["title"] == "快讯 0"
    assert data["items"][1]["title"] == "快讯 1"


def test_item_fields(client):
    resp = client.get("/api/news/latest?limit=1")
    item = resp.json()["items"][0]
    for key in ["source", "title", "content", "published_at", "url", "collected_at"]:
        assert key in item


def test_collect_exchanges(monkeypatch):
    from datetime import datetime, timezone

    from app.collectors.base import NewsFlash

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    def fake_binance_fetch(self):
        return [
            NewsFlash(
                source="binance",
                title="币安公告",
                content="",
                published_at=now,
                url="https://binance.example/1",
                collected_at=now,
            )
        ]

    def fake_upbit_fetch(self):
        return [
            NewsFlash(
                source="upbit",
                title="업비트 공지",
                content="",
                published_at=now,
                url="https://upbit.example/1",
                collected_at=now,
            )
        ]

    monkeypatch.setattr("app.api.BinanceCollector.fetch", fake_binance_fetch)
    monkeypatch.setattr("app.api.UpbitCollector.fetch", fake_upbit_fetch)

    with TestClient(app) as c:
        resp = c.get("/api/collect/exchanges")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data["results"]) == 2
    assert data["results"][0]["source"] == "binance"
    assert data["results"][0]["ok"] is True
    assert data["results"][0]["count"] == 1
    assert data["results"][0]["items"][0]["title"] == "币安公告"
    assert data["results"][1]["source"] == "upbit"
    assert data["results"][1]["items"][0]["title"] == "업비트 공지"
