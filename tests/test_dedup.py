"""Redis 去重 + 标题归一化去重测试。"""
from datetime import datetime, timezone

from app.collectors.base import NewsFlash
from app.dedup import Dedup


class FakeRedis:
    """内存版 Redis，仅实现 set / sadd。"""

    def __init__(self):
        self.kv = {}
        self.sets = {}

    def set(self, key, value, nx=False):
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True

    def sadd(self, key, member):
        s = self.sets.setdefault(key, set())
        if member in s:
            return 0
        s.add(member)
        return 1


def make_flash(title="某快讯标题", url="https://example.com/a", source="techflowpost"):
    return NewsFlash(
        source=source,
        title=title,
        content="c",
        published_at=datetime.now(timezone.utc).replace(tzinfo=None),
        url=url,
    )


def test_url_dedup():
    d = Dedup(client=FakeRedis())
    f = make_flash()
    assert d.is_new_by_url(f.source, f.url) is True
    assert d.is_new_by_url(f.source, f.url) is False  # 第二次重复


def test_normalize_title():
    assert (
        Dedup.normalize_title("Bitwise Announces  Liquidation, of DOGECOIN ETF!")
        == "bitwiseannouncesliquidationofdogecoinetf"
    )
    assert Dedup.normalize_title(" 标题：去重测试！！！ ") == "标题去重测试"


def test_title_dedup():
    d = Dedup(client=FakeRedis())
    assert d.is_new_by_title("快讯 A!") is True
    assert d.is_new_by_title("快讯 A") is False  # 归一化后相同


def test_should_publish():
    d = Dedup(client=FakeRedis())
    f = make_flash()
    assert d.should_publish(f) is True
    assert d.should_publish(f) is False  # url + title 均已存在


def test_cross_platform_same_title_dedup():
    """跨平台同标题应被二次去重拦截。"""
    d = Dedup(client=FakeRedis())
    f1 = make_flash(source="techflowpost", url="https://a.example/1")
    f2 = make_flash(source="odaily", url="https://b.example/2")
    assert d.should_publish(f1) is True
    assert d.should_publish(f2) is False  # 标题相同（不同 url）仍被拦截
