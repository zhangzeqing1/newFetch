"""Upbit 公告采集器（공지사항/通知）。

接口：GET https://api-manager.upbit.com/api/v1/notices?page={n}&per_page={n}&thread_name=general
"""
from datetime import datetime, timezone

import requests

from .. import config
from .base import NewsFlash, utcnow_naive

API_URL = "https://api-manager.upbit.com/api/v1/notices"
PAGE_URL_TEMPLATE = "https://upbit.com/service_center/notice?id={id}"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


class UpbitCollector:
    source = "upbit"

    def fetch(self, page: int = 1, per_page: int | None = None) -> list[NewsFlash]:
        per_page = per_page or config.UPBIT_PAGE_SIZE
        resp = requests.get(
            API_URL,
            params={"page": page, "per_page": per_page, "thread_name": "general"},
            headers=HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json()

        collected_at = utcnow_naive()
        flashes: list[NewsFlash] = []
        for item in payload.get("data", {}).get("list", []):
            flashes.append(
                NewsFlash(
                    source=self.source,
                    title=item.get("title", ""),
                    content=item.get("content") or item.get("body", ""),
                    published_at=self._parse_time(item.get("created_at")),
                    url=PAGE_URL_TEMPLATE.format(id=item.get("id")),
                    collected_at=collected_at,
                )
            )
        return flashes

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
