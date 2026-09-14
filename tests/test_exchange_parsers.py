"""交易所公告采集器解析测试（Binance / Upbit）。"""
from datetime import datetime, timezone

from app.collectors.binance import BinanceCollector
from app.collectors.upbit import UpbitCollector


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def _binance_payload():
    return {
        "code": "000000",
        "message": "success",
        "data": {
            "catalogs": [
                {
                    "catalogId": 161,
                    "catalogName": "下架公告",
                    "articles": [
                        {
                            "id": 123456,
                            "code": "binance-will-delist-xxx",
                            "title": "币安将下架 XXX",
                            "type": 1,
                            "releaseDate": 1730000000000,
                        },
                        {
                            "id": 123455,
                            "code": "binance-will-delist-yyy",
                            "title": "币安将下架 YYY",
                            "type": 1,
                            "releaseDate": 1729999999000,
                        },
                    ],
                }
            ]
        },
    }


def test_binance_parse(monkeypatch):
    def fake_get(url, params, timeout):
        assert params["catalogId"] == 161
        return _FakeResp(_binance_payload())

    monkeypatch.setattr("app.collectors.binance.session.get", fake_get)
    flashes = BinanceCollector().fetch()

    assert len(flashes) == 2
    f0, f1 = flashes
    assert f0.source == "binance"
    assert f0.title == "币安将下架 XXX"
    assert f0.url == "https://www.binance.com/zh-CN/support/announcement/binance-will-delist-xxx"
    # 列表接口无正文
    assert f0.content == ""
    expected = datetime.fromtimestamp(1730000000000 / 1000, tz=timezone.utc).replace(tzinfo=None)
    assert f0.published_at == expected
    assert f1.title == "币安将下架 YYY"


def _upbit_payload():
    return {
        "success": True,
        "data": {
            "notices": [
                {
                    "id": 5098,
                    "title": "某币种入金延迟公告",
                    "listed_at": "2026-09-11T01:00:00+09:00",
                },
                {
                    "id": 5097,
                    "title": "系统维护公告",
                    "listed_at": "2026-09-10T23:00:00+09:00",
                },
            ]
        },
    }


def test_upbit_parse(monkeypatch):
    def fake_get(url, params, timeout):
        assert params["category"] == "notice"
        assert params["os"] == "web"
        return _FakeResp(_upbit_payload())

    monkeypatch.setattr("app.collectors.upbit.session.get", fake_get)
    flashes = UpbitCollector().fetch()

    assert len(flashes) == 2
    f0, f1 = flashes
    assert f0.source == "upbit"
    assert f0.title == "某币种入金延迟公告"
    assert f0.content == ""
    assert f0.url == "https://upbit.com/service_center/notice?id=5098"
    # +09:00 转 UTC：2026-09-11T01:00+09:00 => 2026-09-10T16:00 UTC
    assert f0.published_at == datetime(2026, 9, 10, 16, 0, 0)
    assert f1.url == "https://upbit.com/service_center/notice?id=5097"


def _make_flash(source, url):
    from app.collectors.base import NewsFlash

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return NewsFlash(source=source, title="t", content="", published_at=now, url=url, collected_at=now)


def test_binance_fetch_detail(monkeypatch):
    flash = _make_flash("binance", "https://www.binance.com/zh-CN/support/announcement/abc123")

    def fake_get(url, params, timeout):
        assert params["articleCode"] == "abc123"
        return _FakeResp({"data": {"body": "<p>正文</p><p>第二段</p>"}})

    monkeypatch.setattr("app.collectors.binance.session.get", fake_get)
    assert BinanceCollector().fetch_detail(flash) == "正文第二段"


def test_upbit_fetch_detail(monkeypatch):
    flash = _make_flash("upbit", "https://upbit.com/service_center/notice?id=6554")

    def fake_get(url, timeout):
        assert url == "https://api-manager.upbit.com/api/v1/announcements/6554"
        return _FakeResp({"data": {"body": "안녕하세요 업비트입니다."}})

    monkeypatch.setattr("app.collectors.upbit.session.get", fake_get)
    assert UpbitCollector().fetch_detail(flash) == "안녕하세요 업비트입니다."
