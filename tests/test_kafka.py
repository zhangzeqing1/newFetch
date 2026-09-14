"""Kafka 消息序列化 / 反序列化测试。"""
import json
from datetime import datetime, timezone

from app.collectors.base import NewsFlash
from app.kafka_consumer import parse_message
from app.kafka_producer import serialize


def test_serialize_preserves_chinese():
    flash = NewsFlash(
        source="odaily",
        title="中文标题：测试",
        content="正文内容",
        published_at=datetime(2026, 9, 11, 1, 7, 9, 333000),
        url="https://www.odaily.news/newsflash/1",
        collected_at=datetime(2026, 9, 11, 1, 7, 10),
    )
    raw = serialize(flash)
    assert isinstance(raw, bytes)
    data = json.loads(raw.decode("utf-8"))
    assert data["title"] == "中文标题：测试"
    assert data["published_at"] == "2026-09-11T01:07:09.333000"


def test_parse_message_roundtrip():
    flash = NewsFlash(
        source="techflowpost",
        title="Title",
        content="Content",
        published_at=datetime(2026, 9, 11, 1, 7, 9, 333000),
        url="https://www.techflowpost.com/newsletter/1",
        collected_at=datetime(2026, 9, 11, 1, 7, 10),
    )
    parsed = parse_message(json.loads(serialize(flash).decode("utf-8")))
    assert parsed.source == flash.source
    assert parsed.title == flash.title
    assert parsed.published_at == flash.published_at
    assert parsed.url == flash.url
    assert parsed.collected_at == flash.collected_at


def test_flush_batch_idempotent():
    from sqlalchemy import create_engine, func, select
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import StaticPool

    from app.kafka_consumer import flush_batch
    from app.models import Base, NewsFlash

    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    flashes = [
        NewsFlash(source="s", title=f"t{i}", content="", published_at=now, url=f"https://x/{i}", collected_at=now)
        for i in range(3)
    ]

    # 批量写入
    assert flush_batch(session, flashes) == 3
    assert session.scalar(select(func.count()).select_from(NewsFlash)) == 3

    # 重复批次（新实例但 source/url 相同）：全部跳过（幂等）
    dup = [
        NewsFlash(source="s", title=f"t{i}", content="", published_at=now, url=f"https://x/{i}", collected_at=now)
        for i in range(3)
    ]
    assert flush_batch(session, dup) == 0
    assert session.scalar(select(func.count()).select_from(NewsFlash)) == 3

    # 混合批次：只有新的入库（走逐条降级路径）
    mixed = [
        NewsFlash(source="s", title="t0", content="", published_at=now, url="https://x/0", collected_at=now),
        NewsFlash(source="s", title="new", content="", published_at=now, url="https://x/new", collected_at=now),
    ]
    assert flush_batch(session, mixed) == 1
    assert session.scalar(select(func.count()).select_from(NewsFlash)) == 4

    session.close()
