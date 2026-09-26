"""采集服务入口：并发轮询 4 个平台，按站点独立轮询间隔，经去重后投递 Kafka。"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from prometheus_client import start_http_server

from . import config
from .collectors import BinanceCollector, OdailyCollector, TechFlowCollector, UpbitCollector
from .collectors.base import utcnow_naive
from .dedup import Dedup
from .kafka_producer import NewsFlashProducer
from .metrics import (
    COLLECTOR_FETCHED_TOTAL,
    COLLECTOR_FETCH_DURATION,
    COLLECTOR_FETCH_TOTAL,
    COLLECTOR_NEW_TOTAL,
    KAFKA_PUBLISHED_TOTAL,
    LATEST_ITEM_AGE,
)

logger = logging.getLogger(__name__)


def _fetch_one(collector):
    """采集单个平台，返回 (collector, flashes, error)。"""
    start = time.perf_counter()
    try:
        flashes = collector.fetch()
        COLLECTOR_FETCH_TOTAL.labels(collector.source, "success").inc()
        COLLECTOR_FETCHED_TOTAL.labels(collector.source).inc(len(flashes))
        return collector, flashes, None
    except Exception as e:  # noqa: BLE001
        COLLECTOR_FETCH_TOTAL.labels(collector.source, "fail").inc()
        logger.exception("fetch failed: %s", collector.source)
        return collector, [], e
    finally:
        COLLECTOR_FETCH_DURATION.labels(collector.source).observe(time.perf_counter() - start)


def main() -> None:
    start_http_server(config.COLLECTOR_METRICS_PORT)

    collectors = [TechFlowCollector(), OdailyCollector(), BinanceCollector(), UpbitCollector()]
    dedup = Dedup()
    producer = NewsFlashProducer()

    intervals = {c: getattr(c, "poll_interval", config.COLLECT_INTERVAL) for c in collectors}
    last_fetch = {c: 0.0 for c in collectors}

    logger.info(
        "collector started, base interval=%.1fs, per-source=%s, metrics=:%d",
        config.COLLECT_INTERVAL,
        {c.source: intervals[c] for c in collectors},
        config.COLLECTOR_METRICS_PORT,
    )
    try:
        with ThreadPoolExecutor(max_workers=len(collectors)) as pool:
            while True:
                now = time.monotonic()
                due = [c for c in collectors if now - last_fetch[c] >= intervals[c]]
                for c in due:
                    last_fetch[c] = now  # 标记本次已调度

                if due:
                    # 并发采集本轮到期的平台
                    for collector, flashes, err in pool.map(_fetch_one, due):
                        if err is not None:
                            continue

                        # 新鲜度：最新一条数据的发布时间距今多久
                        if flashes:
                            latest_published = max(f.published_at for f in flashes)
                            age = (utcnow_naive() - latest_published).total_seconds()
                            LATEST_ITEM_AGE.labels(collector.source).set(max(age, 0))

                        fetch_detail = getattr(collector, "fetch_detail", None)
                        new_count = 0
                        for flash in flashes:
                            if dedup.should_publish(flash):
                                if fetch_detail is not None:
                                    try:
                                        flash.content = fetch_detail(flash)
                                    except Exception:  # noqa: BLE001
                                        logger.warning("fetch_detail failed: %s", flash.url)
                                producer.send(flash)
                                new_count += 1

                        COLLECTOR_NEW_TOTAL.labels(collector.source).inc(new_count)
                        KAFKA_PUBLISHED_TOTAL.inc(new_count)
                        logger.info("[%s] fetched=%d new=%d", collector.source, len(flashes), new_count)
                    producer.flush()

                time.sleep(config.COLLECT_INTERVAL)
    finally:
        producer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
