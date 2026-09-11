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
    def fake_get(url, params, headers, timeout):
        assert params["catalogId"] == 161
        assert headers["clienttype"] == "web"
        return _FakeResp(_binance_payload())

    monkeypatch.setattr("app.collectors.binance.requests.get", fake_get)
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
            "list": [
                {
                    "id": 5098,
                    "title": "某币种入金延迟公告",
                    "content": "正文内容……",
                    "created_at": "2026-09-11T01:00:00.000Z",
                },
                {
                    "id": 5097,
                    "title": "系统维护公告",
                    "content": "正文内容2……",
                    "created_at": "2026-09-10T23:00:00.000Z",
                },
            ]
        },
    }


def test_upbit_parse(monkeypatch):
    def fake_get(url, params, headers, timeout):
        assert params["thread_name"] == "general"
        return _FakeResp(_upbit_payload())

    monkeypatch.setattr("app.collectors.upbit.requests.get", fake_get)
    flashes = UpbitCollector().fetch()

    assert len(flashes) == 2
    f0, f1 = flashes
    assert f0.source == "upbit"
    assert f0.title == "某币种入金延迟公告"
    assert f0.content == "正文内容……"
    assert f0.url == "https://upbit.com/service_center/notice?id=5098"
    assert f0.published_at == datetime(2026, 9, 11, 1, 0, 0)
    assert f1.url == "https://upbit.com/service_center/notice?id=5097"
