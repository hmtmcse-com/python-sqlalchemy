import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from python_sqlalchemy.combination.model import BaseModel

engine = create_async_engine("sqlite+aiosqlite:///combination.sqlite3", echo=True)
async_session = async_sessionmaker(engine, expire_on_commit=False)


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(BaseModel.metadata.drop_all)
        await conn.run_sync(BaseModel.metadata.create_all)


if __name__ == '__main__':
    asyncio.run(main())
