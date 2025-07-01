from .async_session import async_engine
from .async_base import Base


async def create_all():
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("All tables created.")


async def drop_all():
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    print("All tables dropped.")
