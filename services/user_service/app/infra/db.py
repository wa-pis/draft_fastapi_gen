from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config.settings import get_settings


settings = get_settings()
engine = create_async_engine(str(settings.DATABASE_URL), pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


@asynccontextmanager
async def get_db_session() -> AsyncSession:
    async with SessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def db_is_healthy() -> bool:
    async with SessionLocal() as session:
        try:
            await session.execute("SELECT 1")  # type: ignore[arg-type]
            return True
        except Exception:
            return False

