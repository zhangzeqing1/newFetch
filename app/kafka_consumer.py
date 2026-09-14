"""Kafka 消费者：多线程批量消费并写入 MySQL（手动提交 offset，幂等）。

可靠性策略：
- 手动提交：写库成功后才 commit offset，失败不提交（崩溃后重试）→ 不丢消息
- 幂等：MySQL 唯一约束 + IntegrityError 降级跳过 → 不重复入库
"""
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


def flush_batch(session, flashes: list[NewsFlash]) -> tuple[int, int]:
    """批量写入 MySQL，返回 (成功处理条数, 硬失败条数)。

    - 成功处理：写入成功 或 幂等跳过（唯一约束冲突）
    - 硬失败：非重复的异常（如连接丢失），需重试
    """
    if not flashes:
        return 0, 0
    try:
        session.add_all(flashes)
        session.commit()
        return len(flashes), 0
    except IntegrityError:
        # 批次里有重复，降级为逐条，跳过重复
        session.rollback()
        saved = 0
        failed = 0
        for flash in flashes:
            try:
                session.add(flash)
                session.commit()
                saved += 1
            except IntegrityError:
                session.rollback()  # 重复，算已处理
            except Exception:  # noqa: BLE001
                session.rollback()
                logger.exception("row insert failed: %s", flash.url)
                failed += 1
        return saved, failed
    except Exception:  # noqa: BLE001
        session.rollback()
        logger.exception("batch flush failed")
        return 0, len(flashes)  # 整批硬失败


def _consume_worker(worker_id: int) -> None:
    """单个消费者线程：同组多实例，Kafka 按分区自动分配。"""
    consumer = KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=config.KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # 手动提交
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
                saved, failed = flush_batch(session, batch)
                if failed == 0:
                    try:
                        consumer.commit()  # 全部成功才提交 offset
                    except Exception:  # noqa: BLE001
                        logger.exception("worker-%d commit failed", worker_id)
                else:
                    logger.warning("worker-%d %d 条写库失败，不提交 offset（崩溃后重试）", worker_id, failed)
                logger.info("worker-%d batch flush: saved=%d failed=%d total=%d", worker_id, saved, failed, len(batch))
                batch = []
                last_flush = time.time()
    finally:
        if batch:
            saved, failed = flush_batch(session, batch)
            if failed == 0:
                try:
                    consumer.commit()
                except Exception:  # noqa: BLE001
                    logger.exception("worker-%d final commit failed", worker_id)
            logger.info("worker-%d final flush: saved=%d failed=%d", worker_id, saved, failed)
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
