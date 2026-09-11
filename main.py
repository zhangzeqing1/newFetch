from fastapi import FastAPI

from app.api import router

app = FastAPI(title="新闻快讯采集系统")

app.include_router(router)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}
