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
