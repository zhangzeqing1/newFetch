"""数据库引擎与会话管理。"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from . import config

engine = create_engine(
    config.DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
    echo=False,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def get_session():
    """FastAPI 依赖：请求作用域的会话。"""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
