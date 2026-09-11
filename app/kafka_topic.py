"""确保 newsflash topic 存在（对 broker 自动建主题之外的显式兜底，幂等）。"""
import logging
import time

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from . import config

logger = logging.getLogger(__name__)


def ensure_topic(topic: str | None = None, retries: int = 30, delay: float = 2.0) -> None:
    """创建主题（幂等），broker 未就绪时重试。"""
    name = topic or config.KAFKA_TOPIC
    servers = config.KAFKA_BOOTSTRAP_SERVERS.split(",")

    last_err: Exception | None = None
    for _ in range(retries):
        try:
            admin = KafkaAdminClient(bootstrap_servers=servers)
            try:
                admin.create_topics([NewTopic(name=name, num_partitions=1, replication_factor=1)])
                logger.info("created topic %s", name)
            except TopicAlreadyExistsError:
                logger.info("topic %s already exists", name)
            finally:
                admin.close()
            return
        except Exception as e:  # noqa: BLE001
            last_err = e
            logger.warning("ensure_topic retry: %s", e)
            time.sleep(delay)
    raise RuntimeError(f"failed to ensure topic {name}: {last_err}")
