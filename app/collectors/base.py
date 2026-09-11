"""采集器公共数据结构与工具。"""
from dataclasses import dataclass, field
from datetime import datetime, timezone


def utcnow_naive() -> datetime:
    """返回 naive 的 UTC 当前时间（MySQL DATETIME 不存时区）。"""
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass
class NewsFlash:
    """两平台采集后统一的快讯数据结构。"""

    source: str
    title: str
    content: str
    published_at: datetime
    url: str
    collected_at: datetime = field(default_factory=utcnow_naive)

    def to_dict(self) -> dict:
        """转为可 JSON 序列化的字典（用于 Kafka 投递）。"""
        return {
            "source": self.source,
            "title": self.title,
            "content": self.content,
            "published_at": self.published_at.isoformat(),
            "url": self.url,
            "collected_at": self.collected_at.isoformat(),
        }
