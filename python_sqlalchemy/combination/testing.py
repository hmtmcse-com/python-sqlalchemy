import asyncio
from python_sqlalchemy.combination.model import BaseModel, User
from python_sqlalchemy.combination.orm import orm


async def main():
    # async with orm.engine.begin() as conn:
    #     await conn.run_sync(BaseModel.metadata.drop_all)
    #     await conn.run_sync(BaseModel.metadata.create_all)

    # async with orm.session() as session:
    #     session.add(User(username="name", email="email"))
    #     await session.commit()

    await User(username="name3", email="email3").save()

if __name__ == '__main__':
    asyncio.run(main())
