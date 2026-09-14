"""两平台接口解析测试（字段映射）。"""
from datetime import datetime, timezone

from app.collectors.odaily import OdailyCollector
from app.collectors.techflow import TechFlowCollector


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def _techflow_payload():
    return {
        "data": [
            {
                "id": 135809,
                "title": "OpenAI CEO Open to Slowing Down Frontier AI Development",
                "abstract": "According to foreign media reports, OpenAI is considering...",
                "url": "",
                "created_at": "2026-09-11T01:07:09.333Z",
            },
            {
                "id": 135808,
                "title": "OpenAI Launches Financial Services Version of ChatGPT",
                "abstract": "According to CNBC, OpenAI launched...",
                "url": "https://www.cnbc.com/2026/09/10/example.html",
                "created_at": "2026-09-11T00:54:19.529Z",
            },
        ],
        "total": 116086,
        "page": 1,
        "page_size": 9,
    }


def test_techflow_parse(monkeypatch):
    def fake_get(url, params, timeout):
        assert params["page"] == 1
        return _FakeResp(_techflow_payload())

    monkeypatch.setattr("app.collectors.techflow.session.get", fake_get)
    flashes = TechFlowCollector().fetch()

    assert len(flashes) == 2
    f0, f1 = flashes
    assert f0.source == "techflowpost"
    assert f0.title.startswith("OpenAI CEO")
    assert f0.content.startswith("According to foreign media")
    # url 为空时回退到详情页链接
    assert f0.url == "https://www.techflowpost.com/newsletter/135809"
    assert f0.published_at == datetime(2026, 9, 11, 1, 7, 9, 333000)
    # url 非空时保留原文链接
    assert f1.url == "https://www.cnbc.com/2026/09/10/example.html"


def _odaily_payload():
    return {
        "code": 200,
        "data": {
            "list": [
                {
                    "id": 517076,
                    "title": "持有超224.7万美元UNI，Arthur Hayes 3小时内买入3.98万枚",
                    "description": "<p>Odaily星球日报讯 据链上分析师监测...</p>",
                    "publishTimestamp": 1789089096000,
                    "newsUrl": "https://x.com/ai_9684xtpa/status/123",
                },
                {
                    "id": 517073,
                    "title": "另一条快讯",
                    "description": "<p>正文</p><p>第二段</p>",
                    "publishTimestamp": 1789089030000,
                    "newsUrl": "",
                },
            ],
            "total": 373652,
        },
    }


def test_odaily_parse(monkeypatch):
    def fake_get(url, params, timeout):
        assert params["groupId"] == 0
        return _FakeResp(_odaily_payload())

    monkeypatch.setattr("app.collectors.odaily.session.get", fake_get)
    flashes = OdailyCollector().fetch()

    assert len(flashes) == 2
    f0, f1 = flashes
    assert f0.source == "odaily"
    # HTML 标签被去除
    assert "<p>" not in f0.content
    assert f0.content.startswith("Odaily星球日报讯")
    assert f0.url == "https://x.com/ai_9684xtpa/status/123"
    expected = datetime.fromtimestamp(1789089096000 / 1000, tz=timezone.utc).replace(tzinfo=None)
    assert f0.published_at == expected
    # 空 newsUrl 回退详情页
    assert f1.url == "https://www.odaily.news/newsflash/517073"
    assert f1.content == "正文第二段"
