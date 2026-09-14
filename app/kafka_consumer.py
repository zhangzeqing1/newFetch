"""Kafka 消费者：多线程批量消费 newsflash topic 并写入 MySQL（幂等，唯一约束冲突跳过）。"""
import json
import logging
import threading
import time
from datetime import datetime

from kafka import KafkaConsumer
from sqlalchemy.exc import IntegrityError

from . import config
from .db import SessionLocal
from .kafka_topic import ensure_topic
from .models import NewsFlash

logger = logging.getLogger(__name__)


def parse_message(value: dict) -> NewsFlash:
    """将 Kafka JSON 消息解析为 NewsFlash 模型。"""
    return NewsFlash(
        source=value["source"],
        title=value["title"],
        content=value["content"],
        published_at=datetime.fromisoformat(value["published_at"]),
        url=value["url"],
        collected_at=datetime.fromisoformat(value["collected_at"]),
    )


def flush_batch(session, flashes: list[NewsFlash]) -> int:
    """批量写入 MySQL；整批遇到重复时降级为逐条写入并跳过重复，返回成功条数。"""
    if not flashes:
        return 0
    try:
        session.add_all(flashes)
        session.commit()
        return len(flashes)
    except IntegrityError:
        # 批次里有重复（唯一约束冲突），降级为逐条，跳过重复
        session.rollback()
        saved = 0
        for flash in flashes:
            try:
                session.add(flash)
                session.commit()
                saved += 1
            except IntegrityError:
                session.rollback()
            except Exception:  # noqa: BLE001
                session.rollback()
                logger.exception("row insert failed: %s", flash.url)
        return saved
    except Exception:  # noqa: BLE001
        session.rollback()
        logger.exception("batch flush failed")
        return 0


def _consume_worker(worker_id: int) -> None:
    """单个消费者线程：同组多实例，Kafka 按分区自动分配。"""
    consumer = KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=config.KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    session = SessionLocal()
    batch: list[NewsFlash] = []
    last_flush = time.time()
    logger.info("worker-%d started", worker_id)
    try:
        while True:
            try:
                records = consumer.poll(timeout_ms=1000)
            except Exception:  # noqa: BLE001
                logger.exception("worker-%d poll failed", worker_id)
                time.sleep(1)
                continue

            for msgs in records.values():
                for msg in msgs:
                    try:
                        batch.append(parse_message(msg.value))
                    except Exception:  # noqa: BLE001
                        logger.exception("worker-%d parse failed", worker_id)

            # 达到「条数阈值」或「时间阈值」就刷一批
            if batch and (
                len(batch) >= config.CONSUMER_BATCH_SIZE
                or time.time() - last_flush >= config.CONSUMER_BATCH_TIMEOUT
            ):
                saved = flush_batch(session, batch)
                logger.info("worker-%d batch flush: %d/%d", worker_id, saved, len(batch))
                batch = []
                last_flush = time.time()
    finally:
        if batch:
            flush_batch(session, batch)
        session.close()
        consumer.close()


def consume() -> None:
    ensure_topic()
    workers = config.KAFKA_TOPIC_PARTITIONS  # 并发度 = 分区数
    threads = []
    for i in range(workers):
        t = threading.Thread(target=_consume_worker, args=(i,), name=f"consumer-{i}", daemon=True)
        t.start()
        threads.append(t)
    logger.info(
        "consumer started, topic=%s group=%s workers=%d batch_size=%d batch_timeout=%.1fs",
        config.KAFKA_TOPIC,
        config.KAFKA_CONSUMER_GROUP,
        workers,
        config.CONSUMER_BATCH_SIZE,
        config.CONSUMER_BATCH_TIMEOUT,
    )
    for t in threads:
        t.join()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    consume()
