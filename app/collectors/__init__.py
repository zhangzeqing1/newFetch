"""采集器：请求两平台接口并解析为统一字段。"""
from .odaily import OdailyCollector
from .techflow import TechFlowCollector

__all__ = ["TechFlowCollector", "OdailyCollector"]
