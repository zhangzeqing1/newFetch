"""Prometheus 指标定义（collector / consumer 共用）。"""
from prometheus_client import Counter, Gauge, Histogram

# ---- 采集器 ----
COLLECTOR_FETCH_TOTAL = Counter(
    "collector_fetch_total",
    "采集请求次数（按来源与成功/失败）",
    ["source", "status"],
)
COLLECTOR_FETCHED_TOTAL = Counter(
    "collector_fetched_total",
    "采集到的快讯条数（按来源）",
    ["source"],
)
COLLECTOR_NEW_TOTAL = Counter(
    "collector_new_total",
    "判定为新并投递 Kafka 的条数（按来源）",
    ["source"],
)
COLLECTOR_FETCH_DURATION = Histogram(
    "collector_fetch_duration_seconds",
    "单次采集耗时（按来源）",
    ["source"],
)
LATEST_ITEM_AGE = Gauge(
    "latest_item_age_seconds",
    "最新一条数据的发布时间距今秒数（越小越新）",
    ["source"],
)

# ---- Kafka 生产 ----
KAFKA_PUBLISHED_TOTAL = Counter(
    "kafka_published_total",
    "投递到 Kafka 的消息总数",
)

# ---- 消费者 ----
CONSUMER_WRITE_TOTAL = Counter(
    "consumer_write_total",
    "写库条数（按成功/失败）",
    ["status"],
)
CONSUMER_SAVED_TOTAL = Counter(
    "consumer_saved_total",
    "成功入库条数",
)
KAFKA_CONSUMER_LAG = Gauge(
    "kafka_consumer_lag",
    "Kafka 消费积压（生产 offset - 已消费 offset）",
    ["partition"],
)
