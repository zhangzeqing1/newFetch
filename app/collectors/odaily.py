"""Odaily 采集器。

接口：GET https://web-api.odaily.news/newsflash/page?page={n}&size={n}&groupId=0&isImport=false
"""
import re
from datetime import datetime, timezone

import requests

from .. import config
from .base import NewsFlash, utcnow_naive

API_URL = "https://web-api.odaily.news/newsflash/page"
PAGE_URL_TEMPLATE = "https://www.odaily.news/newsflash/{id}"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json",
}
_TAG_RE = re.compile(r"<[^>]+>")

# 复用连接：常驻 Session 避免每轮轮询重复 TCP/TLS 握手
session = requests.Session()
session.headers.update(HEADERS)


class OdailyCollector:
    source = "odaily"

    def fetch(self, page: int = 1, size: int | None = None) -> list[NewsFlash]:
        size = size or config.ODAILY_PAGE_SIZE
        resp = session.get(
            API_URL,
            params={"page": page, "size": size, "groupId": 0, "isImport": "false"},
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json()

        collected_at = utcnow_naive()
        flashes: list[NewsFlash] = []
        for item in payload.get("data", {}).get("list", []):
            url = item.get("newsUrl") or PAGE_URL_TEMPLATE.format(id=item.get("id"))
            flashes.append(
                NewsFlash(
                    source=self.source,
                    title=item.get("title", ""),
                    content=self._strip_html(item.get("description", "")),
                    published_at=self._parse_time(item.get("publishTimestamp")),
                    url=url,
                    collected_at=collected_at,
                )
            )
        return flashes

    @staticmethod
    def _strip_html(html: str) -> str:
        """去除 <p> 等 HTML 标签，仅保留文本。"""
        return _TAG_RE.sub("", html or "").strip()

    @staticmethod
    def _parse_time(ms: int | None) -> datetime:
        """解析毫秒时间戳为 naive UTC。"""
        if not ms:
            return utcnow_naive()
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).replace(tzinfo=None)
