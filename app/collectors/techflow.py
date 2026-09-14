"""深潮 TechFlow 采集器。

接口：GET https://api.techflowpost.com/api/client/newsflashes?page={n}&page_size={n}
"""
from datetime import datetime, timezone

import requests

from .. import config
from .base import NewsFlash, utcnow_naive

API_URL = "https://api.techflowpost.com/api/client/newsflashes"
PAGE_URL_TEMPLATE = "https://www.techflowpost.com/newsletter/{id}"
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


class TechFlowCollector:
    source = "techflowpost"

    def fetch(self, page: int = 1, page_size: int | None = None) -> list[NewsFlash]:
        page_size = page_size or config.TECHFLOW_PAGE_SIZE
        resp = session.get(
            API_URL,
            params={"page": page, "page_size": page_size},
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json()

        collected_at = utcnow_naive()
        flashes: list[NewsFlash] = []
        for item in payload.get("data", []):
            url = item.get("url") or PAGE_URL_TEMPLATE.format(id=item.get("id"))
            flashes.append(
                NewsFlash(
                    source=self.source,
                    title=item.get("title", ""),
                    content=item.get("abstract", ""),
                    published_at=self._parse_time(item.get("created_at")),
                    url=url,
                    collected_at=collected_at,
                )
            )
        return flashes

    @staticmethod
    def _parse_time(value: str | None) -> datetime:
        """解析 ISO 8601 UTC 时间，如 2026-09-11T01:07:09.333Z。"""
        if not value:
            return utcnow_naive()
        return (
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            .astimezone(timezone.utc)
            .replace(tzinfo=None)
        )
