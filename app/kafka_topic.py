"""确保 newsflash topic 存在且分区数达标（幂等，broker 自动建主题之外的显式兜底）。"""
import logging
import time

from kafka import KafkaAdminClient
from kafka.admin import NewPartitions, NewTopic
from kafka.errors import InvalidPartitionsError, TopicAlreadyExistsError

from . import config

logger = logging.getLogger(__name__)


def ensure_topic(topic: str | None = None, num_partitions: int | None = None,
                 retries: int = 30, delay: float = 2.0) -> None:
    """创建主题（幂等）；已存在但分区不足时扩容到 num_partitions。"""
    name = topic or config.KAFKA_TOPIC
    num_partitions = num_partitions or config.KAFKA_TOPIC_PARTITIONS
    servers = config.KAFKA_BOOTSTRAP_SERVERS.split(",")

    last_err: Exception | None = None
    for _ in range(retries):
        try:
            admin = KafkaAdminClient(bootstrap_servers=servers)
            try:
                try:
                    admin.create_topics(
                        [NewTopic(name=name, num_partitions=num_partitions, replication_factor=1)]
                    )
                    logger.info("created topic %s (partitions=%d)", name, num_partitions)
                except TopicAlreadyExistsError:
                    try:
                        admin.create_partitions({name: NewPartitions(num_partitions)})
                        logger.info("increased topic %s partitions -> %d", name, num_partitions)
                    except InvalidPartitionsError:
                        logger.info("topic %s partitions already >= %d", name, num_partitions)
            finally:
                admin.close()
            return
        except Exception as e:  # noqa: BLE001
            last_err = e
            logger.warning("ensure_topic retry: %s", e)
            time.sleep(delay)
    raise RuntimeError(f"failed to ensure topic {name}: {last_err}")
