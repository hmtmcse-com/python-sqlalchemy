import asyncio
from python_sqlalchemy.combination.dto import UserDTO, User2DTO
from python_sqlalchemy.combination.model import BaseModel, User
from python_sqlalchemy.combination.orm import orm


async def main():
    async with orm.engine.begin() as conn:
        await conn.run_sync(BaseModel.metadata.drop_all)
        await conn.run_sync(BaseModel.metadata.create_all)

    for index in range(500):
        user = await User(username=f"name {index}", email=f"email{1}@email.local").save()
        print(UserDTO().to_dict(user))

    data_list = [
        {"email": "touhid@gmail.com", "username": "Name"},
        {"email": "touhid@gmail.com"},
        {},
    ]

    for data in data_list:
        model = UserDTO().to_model(data)
        await model.save()


if __name__ == '__main__':
    asyncio.run(main())
