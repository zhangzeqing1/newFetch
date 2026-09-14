"""Upbit 公告采集器（공지사항/通知）。

接口：GET https://api-manager.upbit.com/api/v1/announcements?os=web&page={n}&per_page={n}&category=notice
列表接口不含正文，正文通过 fetch_detail 调详情接口获取。
"""
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import requests

from .. import config
from .base import NewsFlash, utcnow_naive

API_URL = "https://api-manager.upbit.com/api/v1/announcements"
DETAIL_URL_TEMPLATE = "https://api-manager.upbit.com/api/v1/announcements/{id}"
PAGE_URL_TEMPLATE = "https://upbit.com/service_center/notice?id={id}"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# 复用连接：常驻 Session 避免每轮轮询重复 TCP/TLS 握手
session = requests.Session()
session.headers.update(HEADERS)


class UpbitCollector:
    source = "upbit"
    poll_interval = config.UPBIT_POLL_INTERVAL  # Upbit 有风控，单独放慢轮询

    def fetch(self, page: int = 1, per_page: int | None = None) -> list[NewsFlash]:
        per_page = per_page or config.UPBIT_PAGE_SIZE
        resp = session.get(
            API_URL,
            params={"os": "web", "page": page, "per_page": per_page, "category": "notice"},
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json()

        collected_at = utcnow_naive()
        flashes: list[NewsFlash] = []
        for item in payload.get("data", {}).get("notices", []):
            flashes.append(
                NewsFlash(
                    source=self.source,
                    title=item.get("title", ""),
                    content=item.get("content") or "",  # 列表接口不含正文
                    published_at=self._parse_time(item.get("listed_at")),
                    url=PAGE_URL_TEMPLATE.format(id=item.get("id")),
                    collected_at=collected_at,
                )
            )
        return flashes

    def fetch_detail(self, flash: NewsFlash) -> str:
        """请求详情接口获取正文（列表接口不含 body）。"""
        nid = parse_qs(urlparse(flash.url).query).get("id", [None])[0]
        if not nid:
            return ""
        resp = session.get(
            DETAIL_URL_TEMPLATE.format(id=nid),
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("data", {}).get("body", "") or ""

    @staticmethod
    def _parse_time(value) -> datetime:
        """解析时间：兼容秒/毫秒时间戳与 ISO 字符串。"""
        if not value:
            return utcnow_naive()
        if isinstance(value, (int, float)):
            ts = value / 1000 if value > 10_000_000_000 else value
            return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
        try:
            return (
                datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                .astimezone(timezone.utc)
                .replace(tzinfo=None)
            )
        except ValueError:
            return utcnow_naive()
