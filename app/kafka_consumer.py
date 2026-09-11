"""Kafka 消费者：消费 newsflash topic 并写入 MySQL（幂等，唯一约束冲突跳过）。"""
import json
import logging
from datetime import datetime

from kafka import KafkaConsumer
from sqlalchemy.exc import IntegrityError

from . import config
from .db import SessionLocal
from .kafka_topic import ensure_topic
from .models import NewsFlash

logger = logging.getLogger(__name__)


def parse_message(value: dict) -> NewsFlash:
    """将 Kafka JSON 消息解析为 NewsFlash 模型（无 ORM 主键，仅作数据载体）。"""
    return NewsFlash(
        source=value["source"],
        title=value["title"],
        content=value["content"],
        published_at=datetime.fromisoformat(value["published_at"]),
        url=value["url"],
        collected_at=datetime.fromisoformat(value["collected_at"]),
    )


def save_flash(session, flash: NewsFlash) -> None:
    """写入 MySQL；重复 (source, url) 时抛出 IntegrityError。"""
    session.add(flash)
    session.commit()


def consume() -> None:
    ensure_topic()
    consumer = KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=config.KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    logger.info("consumer started, topic=%s group=%s", config.KAFKA_TOPIC, config.KAFKA_CONSUMER_GROUP)

    for message in consumer:
        value = message.value
        session = SessionLocal()
        try:
            flash = parse_message(value)
            save_flash(session, flash)
            logger.info("saved [%s] %s", flash.source, flash.title[:40])
        except IntegrityError:
            session.rollback()
            logger.info("duplicate skipped: %s", value.get("url"))
        except Exception:
            session.rollback()
            logger.exception("save failed: %s", value)
        finally:
            session.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    consume()
