"""IP 池（demo 版）：基于 Clash API 切换节点实现代理 IP 轮换 + 健康检测。

原理：Clash 只有一个代理端口（7897），切换「GLOBAL 组」的选中节点 = 切换出口 IP。
- rotate()：切换前检测节点健康，不可用跳过
- record_request()：记录一次请求归到当前节点（用于「单 IP 请求占比」）
- healthy 集合：维护可用节点，暴露「可用数」
"""
import logging
import re
import time

import requests

from . import config
from .metrics import IP_POOL_AVAILABLE, IP_POOL_HEALTH_CHECK_TOTAL, IP_POOL_REQUEST_TOTAL

logger = logging.getLogger(__name__)


class ProxyPool:
    def __init__(self, nodes: list[str] | None = None):
        self.base = config.CLASH_API_BASE
        self.secret = config.CLASH_API_SECRET
        self.nodes = nodes if nodes is not None else config.PROXY_NODES
        self.index = 0
        self.current_node: str | None = None
        self.healthy: set = set()

    @staticmethod
    def _label(node: str) -> str:
        """去掉 emoji 等非 ASCII 字符，得到可读的标签。"""
        return re.sub(r"[^\x00-\x7F]+", "", node).strip() or node

    def _api(self, method: str, path: str, body: dict | None = None):
        headers = {"Authorization": f"Bearer {self.secret}"}
        resp = requests.request(
            method,
            self.base + path,
            json=body,
            headers=headers,
            timeout=5,
            proxies={"http": None, "https": None},  # Clash API 走本地直连
        )
        resp.raise_for_status()
        return resp

    def switch_to(self, node: str) -> None:
        """把 GLOBAL 组切到指定节点。"""
        self._api("PUT", "/proxies/GLOBAL", {"name": node})
        self.current_node = node

    def record_request(self) -> None:
        """记录一次请求（归到当前节点）。"""
        if self.current_node:
            IP_POOL_REQUEST_TOTAL.labels(self._label(self.current_node)).inc()

    def check_health(self, node: str, timeout: int = 8) -> tuple[bool, float | None]:
        """切到该节点并测访问，返回 (是否可用, 延迟ms)。"""
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
        except Exception:  # noqa: BLE001
            ok = False
            latency = None

        if ok:
            self.healthy.add(node)
        else:
            self.healthy.discard(node)
        IP_POOL_HEALTH_CHECK_TOTAL.labels("success" if ok else "fail").inc()
        IP_POOL_AVAILABLE.set(len(self.healthy))
        return ok, latency

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
