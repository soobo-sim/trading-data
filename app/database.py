"""Database configuration for CoinMarket Data service — 기존 PostgreSQL 인스턴스 공유"""
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
import os

# load_dotenv()는 pydantic-settings가 이미 처리하므로 호출하지 않는다.
# 모듈 레벨 load_dotenv()는 OS 환경변수를 오염시켜 테스트 격리를 파괴한다.

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://trader:trader_password_123@localhost:5432/trader_db",
)

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)

Base = declarative_base()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db():
    """테이블 자동 생성 (개발용). 운영 환경은 Alembic 사용."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def close_db():
    await engine.dispose()
