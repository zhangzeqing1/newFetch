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
