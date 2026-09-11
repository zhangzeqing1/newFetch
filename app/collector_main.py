"""采集服务入口：每 3 秒轮询两平台，经去重后投递 Kafka。"""
import logging
import time

from . import config
from .collectors import BinanceCollector, OdailyCollector, TechFlowCollector, UpbitCollector
from .dedup import Dedup
from .kafka_producer import NewsFlashProducer

logger = logging.getLogger(__name__)


def main() -> None:
    collectors = [TechFlowCollector(), OdailyCollector(), BinanceCollector(), UpbitCollector()]
    dedup = Dedup()
    producer = NewsFlashProducer()

    logger.info("collector started, interval=%.1fs", config.COLLECT_INTERVAL)
    try:
        while True:
            for collector in collectors:
                try:
                    flashes = collector.fetch()
                except Exception:
                    logger.exception("fetch failed: %s", collector.source)
                    continue

                new_count = 0
                for flash in flashes:
                    if dedup.should_publish(flash):
                        producer.send(flash)
                        new_count += 1
                logger.info("[%s] fetched=%d new=%d", collector.source, len(flashes), new_count)

            producer.flush()
            time.sleep(config.COLLECT_INTERVAL)
    finally:
        producer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
