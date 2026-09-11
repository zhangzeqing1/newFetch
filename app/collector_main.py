"""采集服务入口：并发轮询 4 个平台，经去重后投递 Kafka。"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from . import config
from .collectors import BinanceCollector, OdailyCollector, TechFlowCollector, UpbitCollector
from .dedup import Dedup
from .kafka_producer import NewsFlashProducer

logger = logging.getLogger(__name__)


def _fetch_one(collector):
    """采集单个平台，返回 (source, flashes, error)。"""
    try:
        return collector.source, collector.fetch(), None
    except Exception as e:  # noqa: BLE001
        logger.exception("fetch failed: %s", collector.source)
        return collector.source, [], e


def main() -> None:
    collectors = [TechFlowCollector(), OdailyCollector(), BinanceCollector(), UpbitCollector()]
    dedup = Dedup()
    producer = NewsFlashProducer()

    logger.info("collector started, interval=%.1fs (concurrent)", config.COLLECT_INTERVAL)
    try:
        with ThreadPoolExecutor(max_workers=len(collectors)) as pool:
            while True:
                # 并发采集 4 个平台（一轮耗时 ≈ 最慢的那个站，而非 4 站之和）
                for source, flashes, err in pool.map(_fetch_one, collectors):
                    if err is not None:
                        continue
                    new_count = 0
                    for flash in flashes:
                        if dedup.should_publish(flash):
                            producer.send(flash)
                            new_count += 1
                    logger.info("[%s] fetched=%d new=%d", source, len(flashes), new_count)

                producer.flush()
                time.sleep(config.COLLECT_INTERVAL)
    finally:
        producer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
