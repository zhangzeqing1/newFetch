"""IP 池（demo 版）：基于 Clash API 切换节点实现代理 IP 轮换。

原理：Clash 只有一个代理端口（7897），切换「GLOBAL 组」的选中节点 = 切换出口 IP。
本模块把 N 个节点抽象成一个池，提供轮换切换能力。
"""
import logging

import requests

from . import config

logger = logging.getLogger(__name__)


class ProxyPool:
    def __init__(self, nodes: list[str] | None = None):
        self.base = config.CLASH_API_BASE
        self.secret = config.CLASH_API_SECRET
        self.nodes = nodes if nodes is not None else config.PROXY_NODES
        self.index = 0

    def _api(self, method: str, path: str, body: dict | None = None):
        headers = {"Authorization": f"Bearer {self.secret}"}
        resp = requests.request(
            method,
            self.base + path,
            json=body,
            headers=headers,
            timeout=5,
            proxies={"http": None, "https": None},  # Clash API 走本地直连，不走代理
        )
        resp.raise_for_status()
        return resp

    def switch_to(self, node: str) -> None:
        """把 GLOBAL 组切到指定节点。"""
        self._api("PUT", "/proxies/GLOBAL", {"name": node})
        logger.info("proxy switch -> %s", node)

    def rotate(self) -> str | None:
        """轮换到下一个节点，返回节点名。"""
        if not self.nodes:
            return None
        node = self.nodes[self.index % len(self.nodes)]
        self.index += 1
        self.switch_to(node)
        return node

    def current(self) -> str:
        """返回当前选中的节点名。"""
        data = self._api("GET", "/proxies").json()
        return data["proxies"]["GLOBAL"]["now"]
