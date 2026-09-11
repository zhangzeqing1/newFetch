"""应用配置：统一从环境变量读取 MySQL / Redis / Kafka 连接信息。

Docker Compose 下使用服务名（mysql / redis / kafka）；本地开发可通过环境变量覆盖。
"""
import os


def _env(key: str, default: str) -> str:
    return os.getenv(key, default)


# ---- MySQL ----
MYSQL_HOST = _env("MYSQL_HOST", "mysql")
MYSQL_PORT = int(_env("MYSQL_PORT", "3306"))
MYSQL_USER = _env("MYSQL_USER", "news")
MYSQL_PASSWORD = _env("MYSQL_PASSWORD", "news123")
MYSQL_DB = _env("MYSQL_DB", "newsflash")

DATABASE_URL = _env(
    "DATABASE_URL",
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4",
)

# ---- Redis ----
REDIS_HOST = _env("REDIS_HOST", "redis")
REDIS_PORT = int(_env("REDIS_PORT", "6379"))
REDIS_URL = _env("REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/0")

# ---- Kafka ----
KAFKA_BOOTSTRAP_SERVERS = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = _env("KAFKA_TOPIC", "newsflash")
KAFKA_CONSUMER_GROUP = _env("KAFKA_CONSUMER_GROUP", "newsflash-consumer")

# ---- 采集器 ----
COLLECT_INTERVAL = float(_env("COLLECT_INTERVAL", "1"))  # 默认每 1 秒采集一次
TECHFLOW_PAGE_SIZE = int(_env("TECHFLOW_PAGE_SIZE", "9"))
ODAILY_PAGE_SIZE = int(_env("ODAILY_PAGE_SIZE", "16"))
BINANCE_PAGE_SIZE = int(_env("BINANCE_PAGE_SIZE", "20"))
BINANCE_CATALOG_ID = int(_env("BINANCE_CATALOG_ID", "161"))
UPBIT_PAGE_SIZE = int(_env("UPBIT_PAGE_SIZE", "20"))

# ---- API ----
API_DEFAULT_LIMIT = int(_env("API_DEFAULT_LIMIT", "20"))
