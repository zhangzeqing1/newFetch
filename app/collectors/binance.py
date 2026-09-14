"""Binance 公告采集器（下架/退市公告 catalogId=161）。

接口：GET https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&catalogId={n}&pageNo={n}&pageSize={n}
列表接口不含正文，正文通过 fetch_detail 调 detail 接口获取。
"""
import re
from datetime import datetime, timezone

import requests

from .. import config
from .base import NewsFlash, utcnow_naive

API_URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
DETAIL_URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query"
PAGE_URL_TEMPLATE = "https://www.binance.com/zh-CN/support/announcement/{code}"
_TAG_RE = re.compile(r"<[^>]+>")
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "clienttype": "web",
    "lang": "zh-CN",
    "Origin": "https://www.binance.com",
}

# 复用连接：常驻 Session 避免每轮轮询重复 TCP/TLS 握手
session = requests.Session()
session.headers.update(HEADERS)


class BinanceCollector:
    source = "binance"
    poll_interval = config.BINANCE_POLL_INTERVAL  # Binance 有风控，单独放慢轮询

    def fetch(
        self,
        page: int = 1,
        page_size: int | None = None,
        catalog_id: int | None = None,
    ) -> list[NewsFlash]:
        page_size = page_size or config.BINANCE_PAGE_SIZE
        catalog_id = catalog_id or config.BINANCE_CATALOG_ID
        resp = session.get(
            API_URL,
            params={"type": 1, "catalogId": catalog_id, "pageNo": page, "pageSize": page_size},
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json()

        collected_at = utcnow_naive()
        flashes: list[NewsFlash] = []
        for catalog in payload.get("data", {}).get("catalogs", []):
            for item in catalog.get("articles", []):
                code = item.get("code") or str(item.get("id"))
                flashes.append(
                    NewsFlash(
                        source=self.source,
                        title=item.get("title", ""),
                        content=item.get("body", ""),  # 列表接口通常不含正文
                        published_at=self._parse_time(item.get("releaseDate")),
                        url=PAGE_URL_TEMPLATE.format(code=code),
                        collected_at=collected_at,
                    )
                )
        return flashes

    def fetch_detail(self, flash: NewsFlash) -> str:
        """请求详情接口获取正文（列表接口不含 body）。"""
        code = flash.url.rstrip("/").split("/")[-1]
        resp = session.get(
            DETAIL_URL,
            params={"articleCode": code},
            timeout=10,
        )
        resp.raise_for_status()
        body = resp.json().get("data", {}).get("body", "")
        return _TAG_RE.sub("", body or "").strip()

    @staticmethod
    def _parse_time(ms: int | None) -> datetime:
        """解析毫秒时间戳为 naive UTC。"""
        if not ms:
            return utcnow_naive()
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).replace(tzinfo=None)
