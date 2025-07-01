import asyncio
from python_sqlalchemy.query_ex1.models import User, Post
from python_sqlalchemy.query_ex1.async_base import or_

async def main():

    # await drop_all()
    # await create_all()

    # Create user
    user = await User.create(name="John", email="john@example.com", age=30)

    # # Create a post
    await Post.create(title="First Post", user_id=user.id)

    # query full models
    users = await User.select().where(User.age >= 18).list()
    for u in users:
        print("Full model:", u.name, u.email)

    # query columns only
    rows = await User.select(User.id, User.name).where(User.age >= 18).list()
    for row in rows:
        print("Row:", row.id, row.name)

    # # joins
    # posts = await Post.select().join(User, Post.user_id == User.id).where(User.name == "John").list()
    # for post in posts:
    #     print("Post:", post.title, "by", post.author.name)

    # count
    count = await User.select().where(User.age >= 18).count()
    print("Count:", count)

    # and/or
    filtered = await User.select().where(
        or_(
            User.age > 20,
            User.name.like("%John%")
        )
    ).list()
    print("Filtered:", [u.name for u in filtered])

    # update
    await User.update(User.id == user.id, {"name": "Johnny"})

    # bulk update
    await User.bulk_update([
        {"id": user.id, "age": 31}
    ])

    # delete
    await User.delete(User.id == user.id)


if __name__ == "__main__":
    asyncio.run(main())
