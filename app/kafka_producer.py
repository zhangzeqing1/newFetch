"""Kafka 生产者：将新快讯以 JSON 投递到 newsflash topic。"""
import json

from kafka import KafkaProducer

from . import config
from .collectors.base import NewsFlash
from .kafka_topic import ensure_topic


def serialize(flash: NewsFlash) -> bytes:
    """将快讯序列化为 JSON 字节（保留中文，不转义）。"""
    return json.dumps(flash.to_dict(), ensure_ascii=False).encode("utf-8")


class NewsFlashProducer:
    def __init__(self, bootstrap_servers: str | None = None):
        servers = bootstrap_servers or config.KAFKA_BOOTSTRAP_SERVERS
        ensure_topic()
        self.producer = KafkaProducer(
            bootstrap_servers=servers.split(","),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )

    def send(self, flash: NewsFlash) -> None:
        self.producer.send(
            config.KAFKA_TOPIC,
            key=flash.source.encode("utf-8"),
            value=flash.to_dict(),
        )

    def flush(self) -> None:
        self.producer.flush()

    def close(self) -> None:
        self.producer.close()
