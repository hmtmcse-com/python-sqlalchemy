import asyncio

from python_sqlalchemy.query_ex1.models import User, Post
from python_sqlalchemy.query_ex1.async_base import and_, or_


async def main():
    # Create user
    user = await User.create(name="John", email="john@example.com", age=30)

    # Create a post
    post = await Post.create(title="First post", user_id=user.id)

    # Select with where + paginate
    users = await User.select().where(User.age >= 18).order_by(User.name).paginate(page=1, page_size=10).list()
    print("Users >= 18:", users)

    # Count
    count = await User.select().where(User.age >= 18).count()
    print("Count:", count)

    # and_ / or_ example
    filtered = await User.select().where(
        or_(
            User.name.like("%John%"),
            User.age > 25
        )
    ).list()
    print("Filtered:", filtered)

    # Join posts with author
    posts = await Post.select().join(User, Post.user_id == User.id).where(User.name == "John").list()
    print("Posts by John:", posts)

    # Update user
    await User.update(User.id == user.id, {"name": "Johnny"})

    # Bulk update
    await User.bulk_update([
        {"id": user.id, "age": 31}
    ])

    # Delete user
    await User.delete(User.id == user.id)


if __name__ == "__main__":
    asyncio.run(main())
