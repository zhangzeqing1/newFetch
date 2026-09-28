import time

from fastapi import FastAPI
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware

from app.api import router
from app.metrics import REQUEST_COUNT, REQUEST_DURATION

app = FastAPI(title="新闻快讯采集系统")

app.include_router(router)


class MetricsMiddleware(BaseHTTPMiddleware):
    """统计每个 HTTP 请求的次数与耗时（埋点）。"""

    async def dispatch(self, request, call_next):
        start = time.perf_counter()
        status = "500"
        try:
            response = await call_next(request)
            status = str(response.status_code)
            return response
        finally:
            duration = time.perf_counter() - start
            REQUEST_COUNT.labels(request.method, request.url.path, status).inc()
            REQUEST_DURATION.labels(request.method, request.url.path).observe(duration)


app.add_middleware(MetricsMiddleware)


@app.get("/metrics")
def metrics():
    """Prometheus 指标端点。"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}
