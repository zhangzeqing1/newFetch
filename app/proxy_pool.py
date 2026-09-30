"""IP 池（demo 版）：基于 Clash API 切换节点实现代理 IP 轮换 + 健康检测。

原理：Clash 只有一个代理端口（7897），切换「GLOBAL 组」的选中节点 = 切换出口 IP。
rotate() 在切换前先检测节点是否可用（能否通过它访问健康检测目标），不可用则跳过。
"""
import logging
import time

import requests

from . import config
from .metrics import IP_POOL_HEALTH_CHECK_TOTAL

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

    def check_health(self, node: str, timeout: int = 8) -> tuple[bool, float | None]:
        """切到该节点并测试能否访问健康检测目标，返回 (是否可用, 延迟ms)。"""
        self.switch_to(node)
        start = time.time()
        try:
            r = requests.get(
                config.PROXY_HEALTH_URL,
                proxies={"http": config.PROXY_URL, "https": config.PROXY_URL},
                timeout=timeout,
            )
            latency = (time.time() - start) * 1000
            ok = r.status_code < 500
            IP_POOL_HEALTH_CHECK_TOTAL.labels("success" if ok else "fail").inc()
            return ok, latency
        except Exception:  # noqa: BLE001
            IP_POOL_HEALTH_CHECK_TOTAL.labels("fail").inc()
            return False, None

    def rotate(self) -> str | None:
        """轮换到下一个健康节点，跳过不可用节点。"""
        for _ in range(len(self.nodes)):
            node = self.nodes[self.index % len(self.nodes)]
            self.index += 1
            ok, latency = self.check_health(node)
            if ok:
                logger.info("proxy switch -> %s (%.0fms)", node, latency or 0)
                return node
            logger.warning("proxy node unhealthy, skip: %s", node)
        logger.error("all proxy nodes unhealthy")
        return None

    def current(self) -> str:
        """返回当前选中的节点名。"""
        data = self._api("GET", "/proxies").json()
        return data["proxies"]["GLOBAL"]["now"]
