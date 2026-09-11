"""去重逻辑。

1. 初次去重（Redis）：按平台 + 原文链接 SET NX，已存在则跳过。
2. 二次去重：聚合两平台数据后，标题归一化（去空白/标点、英文转小写）精确匹配。
"""
import re

import redis

from . import config
from .collectors.base import NewsFlash

# 匹配所有非单词字符（含中文标点、空白、英文标点、下划线），归一化后删除
_NORMALIZE_RE = re.compile(r"[\W_]+", re.UNICODE)

_URL_KEY_PREFIX = "newsflash:url"
_TITLE_SET_KEY = "newsflash:titles"


class Dedup:
    def __init__(self, client: redis.Redis | None = None):
        self.client = client or redis.Redis.from_url(config.REDIS_URL, decode_responses=True)

    def url_key(self, source: str, url: str) -> str:
        return f"{_URL_KEY_PREFIX}:{source}:{url}"

    def is_new_by_url(self, source: str, url: str) -> bool:
        """按平台 + 原文链接去重。SET NX 成功（key 不存在）返回 True。"""
        return bool(self.client.set(self.url_key(source, url), "1", nx=True))

    @staticmethod
    def normalize_title(title: str) -> str:
        """归一化标题：去除空白与标点，英文转小写。"""
        return _NORMALIZE_RE.sub("", title or "").lower()

    def is_new_by_title(self, title: str) -> bool:
        """按归一化标题去重。SADD 返回 1 表示新加入。"""
        return self.client.sadd(_TITLE_SET_KEY, self.normalize_title(title)) == 1

    def should_publish(self, flash: NewsFlash) -> bool:
        """先按链接去重，再按标题去重；均为新则返回 True。"""
        if not self.is_new_by_url(flash.source, flash.url):
            return False
        if not self.is_new_by_title(flash.title):
            return False
        return True
