"""去重逻辑。

1. 初次去重（Redis）：按平台 + 原文链接 SET NX，带 7 天 TTL。
2. 二次去重：标题精确匹配（ZSet 7 天滑动窗口）+ 字符 bigram Jaccard（最近 N 条窗口）。
"""
import re
import time

import redis

from . import config
from .collectors.base import NewsFlash

# 匹配所有非单词字符（含中文标点、空白、英文标点、下划线），归一化后删除
_NORMALIZE_RE = re.compile(r"[\W_]+", re.UNICODE)

_URL_KEY_PREFIX = "newsflash:url"
_TITLE_ZSET_KEY = "newsflash:titles_window"  # ZSet：7 天窗口，精确去重
_TITLE_LIST_KEY = "newsflash:recent_titles"  # List：500 条窗口，模糊去重


def _ngrams(text: str, n: int = 2) -> set:
    """字符级 n-gram（中文无需分词）。"""
    return {text[i : i + n] for i in range(len(text) - n + 1)}


def _jaccard(a: str, b: str) -> float:
    """基于字符 bigram 的 Jaccard 相似度。"""
    sa, sb = _ngrams(a), _ngrams(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


class Dedup:
    def __init__(self, client: redis.Redis | None = None):
        self.client = client or redis.Redis.from_url(config.REDIS_URL, decode_responses=True)

    def url_key(self, source: str, url: str) -> str:
        return f"{_URL_KEY_PREFIX}:{source}:{url}"

    def is_new_by_url(self, source: str, url: str) -> bool:
        """按平台 + 原文链接去重（7 天 TTL）。SET NX 成功返回 True。"""
        return bool(
            self.client.set(self.url_key(source, url), "1", nx=True, ex=config.DEDUP_TTL)
        )

    @staticmethod
    def normalize_title(title: str) -> str:
        """归一化标题：去除空白与标点，英文转小写。"""
        return _NORMALIZE_RE.sub("", title or "").lower()

    def is_new_by_title(self, title: str) -> bool:
        """标题二次去重：ZSet 精确匹配（7 天窗口）+ 最近窗口 bigram Jaccard 相似度。"""
        normalized = self.normalize_title(title)
        if not normalized:
            return True

        now = time.time()
        # 1. 精确匹配（ZSet 7 天滑动窗口）
        if self.client.zscore(_TITLE_ZSET_KEY, normalized) is not None:
            return False

        # 2. 模糊匹配（对最近 N 条标题窗口）
        for old in self.client.lrange(_TITLE_LIST_KEY, 0, -1):
            if _jaccard(normalized, old) > config.TITLE_SIM_THRESHOLD:
                return False

        # 3. 确认为新标题：写入 ZSet + 清理 7 天前成员 + 加入最近窗口
        self.client.zadd(_TITLE_ZSET_KEY, {normalized: now})
        self.client.zremrangebyscore(_TITLE_ZSET_KEY, 0, now - config.DEDUP_TTL)
        self.client.lpush(_TITLE_LIST_KEY, normalized)
        self.client.ltrim(_TITLE_LIST_KEY, 0, config.TITLE_WINDOW - 1)
        return True

    def should_publish(self, flash: NewsFlash) -> bool:
        """先按链接去重，再按标题去重；均为新则返回 True。"""
        if not self.is_new_by_url(flash.source, flash.url):
            return False
        if not self.is_new_by_title(flash.title):
            return False
        return True
