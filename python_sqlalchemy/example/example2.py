import asyncio
from datetime import datetime
from typing import List, Optional

from sqlalchemy import (
    Column, Integer, String, Float, DateTime, ForeignKey, Boolean,
    func, select, and_, or_, between, text, case, literal
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import (
    declarative_base, relationship, joinedload, contains_eager,
    aliased, selectinload, with_loader_criteria
)
from sqlalchemy.sql.expression import cast

# Base model
Base = declarative_base()


# Models with relationships
class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # One-to-many relationship with posts
    posts = relationship("Post", back_populates="author",
                         order_by="Post.created_at.desc()",
                         cascade="all, delete-orphan")

    # One-to-one relationship with profile
    profile = relationship("Profile", back_populates="user", uselist=False,
                           cascade="all, delete-orphan")

    # Many-to-many relationship with groups
    groups = relationship("Group", secondary="user_groups", back_populates="users")


class Profile(Base):
    __tablename__ = 'profiles'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete="CASCADE"), unique=True)
    bio = Column(String(500))
    website = Column(String(100))

    user = relationship("User", back_populates="profile")


class Post(Base):
    __tablename__ = 'posts'

    id = Column(Integer, primary_key=True)
    title = Column(String(100), nullable=False)
    content = Column(String(1000))
    views = Column(Integer, default=0)
    rating = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    author_id = Column(Integer, ForeignKey('users.id'))

    author = relationship("User", back_populates="posts")
    comments = relationship("Comment", back_populates="post",
                            cascade="all, delete-orphan")

    # Conditional relationship
    popular_comments = relationship(
        "Comment",
        primaryjoin="and_(Post.id==Comment.post_id, Comment.likes>10)",
        viewonly=True
    )


class Comment(Base):
    __tablename__ = 'comments'

    id = Column(Integer, primary_key=True)
    content = Column(String(500), nullable=False)
    likes = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    post_id = Column(Integer, ForeignKey('posts.id'))
    user_id = Column(Integer, ForeignKey('users.id'))

    post = relationship("Post", back_populates="comments")
    user = relationship("User")


class Group(Base):
    __tablename__ = 'groups'

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(String(200))

    users = relationship("User", secondary="user_groups", back_populates="groups")


# Association table for many-to-many relationship
class UserGroup(Base):
    __tablename__ = 'user_groups'

    user_id = Column(Integer, ForeignKey('users.id'), primary_key=True)
    group_id = Column(Integer, ForeignKey('groups.id'), primary_key=True)
    joined_at = Column(DateTime, default=datetime.utcnow)
    is_admin = Column(Boolean, default=False)


# Database setup
DATABASE_URL = "sqlite+aiosqlite:///example2.db"
engine = create_async_engine(DATABASE_URL, echo=True)
async_session = async_sessionmaker(engine, expire_on_commit=False)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# CRUD Operations
class DatabaseManager:
    @staticmethod
    async def create_single_user(session: AsyncSession, user_data: dict):
        user = User(**user_data)
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user

    @staticmethod
    async def create_bulk_users(session: AsyncSession, users_data: List[dict]):
        users = [User(**data) for data in users_data]
        session.add_all(users)
        await session.commit()
        return users

    @staticmethod
    async def update_single_user(session: AsyncSession, user_id: int, update_data: dict):
        result = await session.execute(
            select(User).where(User.id == user_id)
        )
        user = result.scalar_one()

        for key, value in update_data.items():
            setattr(user, key, value)

        await session.commit()
        await session.refresh(user)
        return user

    @staticmethod
    async def update_bulk_users(session: AsyncSession, user_ids: List[int], update_data: dict):
        result = await session.execute(
            select(User).where(User.id.in_(user_ids))
        )
        users = result.scalars().all()

        for user in users:
            for key, value in update_data.items():
                setattr(user, key, value)

        await session.commit()
        return users

    @staticmethod
    async def list_all_users(session: AsyncSession):
        # No pagination
        result = await session.execute(select(User))
        return result.scalars().all()

    @staticmethod
    async def list_users_with_pagination(session: AsyncSession, page: int, per_page: int):
        offset = (page - 1) * per_page
        result = await session.execute(
            select(User)
            .offset(offset)
            .limit(per_page)
        )
        return result.scalars().all()

    @staticmethod
    async def get_user_with_profile(session: AsyncSession, user_id: int):
        result = await session.execute(
            select(User)
            .options(selectinload(User.profile))
            .where(User.id == user_id)
        )
        return result.scalar_one()

    @staticmethod
    async def get_users_with_posts(session: AsyncSession, min_posts: int = 0):
        stmt = (
            select(User)
            .join(User.posts)
            .group_by(User.id)
            .having(func.count(Post.id) >= min_posts)
        )
        result = await session.execute(stmt)
        return result.scalars().all()

    @staticmethod
    async def get_posts_with_comments(session: AsyncSession, min_likes: int = 0):
        stmt = (
            select(Post)
            .options(selectinload(Post.comments))
            .join(Post.comments)
            .where(Comment.likes >= min_likes)
            .distinct()
        )
        result = await session.execute(stmt)
        return result.scalars().all()

    @staticmethod
    async def get_post_stats(session: AsyncSession):
        # Aggregation examples
        stmt = select(
            func.count(Post.id).label("total_posts"),
            func.sum(Post.views).label("total_views"),
            func.avg(Post.rating).label("avg_rating"),
            func.max(Post.created_at).label("latest_post"),
            func.min(Post.created_at).label("oldest_post")
        )
        result = await session.execute(stmt)
        return result.one()

    @staticmethod
    async def get_user_post_counts(session: AsyncSession):
        # Using alias and label
        user_alias = aliased(User)
        stmt = (
            select(
                user_alias.name.label("user_name"),
                func.count(Post.id).label("post_count"),
                func.sum(Post.views).label("total_views"),
                (func.sum(Post.views) / func.nullif(func.count(Post.id), 0)).label("avg_views_per_post")
            )
            .join(Post, user_alias.id == Post.author_id)
            .group_by(user_alias.id)
            .order_by(func.count(Post.id).desc())
        )
        result = await session.execute(stmt)
        return result.all()

    @staticmethod
    async def complex_filter_examples(session: AsyncSession):
        # Various filtering examples
        stmt = select(User).where(
            or_(
                and_(
                    User.is_active.is_(True),
                    User.created_at >= datetime(2023, 1, 1)
                ),
                User.email.like("%@example.com"),
                between(User.id, 5, 10),
                User.name.in_(["Alice", "Bob", "Charlie"]),
                User.id > 5,
                User.created_at < datetime.now()
            )
        )
        result = await session.execute(stmt)
        return result.scalars().all()

    @staticmethod
    async def join_examples(session: AsyncSession):
        # Inner join (default)
        inner_join = select(User).join(User.posts)

        # Left outer join
        left_join = select(User).outerjoin(User.posts)

        # Right join (SQLite doesn't support RIGHT JOIN directly, we emulate it)
        right_join = select(Post).outerjoin(User).where(User.id.is_not(None))

        # Full outer join (SQLite doesn't support FULL OUTER JOIN directly)
        union1 = select(User).outerjoin(User.posts).where(Post.id.is_(None))
        union2 = select(Post).outerjoin(User).where(User.id.is_(None))
        full_outer_join = union1.union(union2)

        # Execute examples
        inner_result = await session.execute(inner_join)
        left_result = await session.execute(left_join)
        right_result = await session.execute(right_join)
        full_result = await session.execute(full_outer_join)

        return {
            "inner": inner_result.scalars().all(),
            "left": left_result.scalars().all(),
            "right": right_result.scalars().all(),
            "full": full_result.scalars().all()
        }

    @staticmethod
    async def subquery_examples(session: AsyncSession):
        # Subquery in FROM clause
        subq = (
            select(
                Post.author_id.label("user_id"),
                func.count(Post.id).label("post_count")
            )
            .group_by(Post.author_id)
            .subquery()
        )

        stmt = (
            select(User, subq.c.post_count)
            .join(subq, User.id == subq.c.user_id)
            .order_by(subq.c.post_count.desc())
        )

        # Subquery in WHERE clause
        subq2 = select(Post.author_id).where(Post.views > 100).subquery()
        stmt2 = select(User).where(User.id.in_(subq2))

        # Correlated subquery
        subq3 = (
            select(func.count(Post.id))
            .where(Post.author_id == User.id)
            .scalar_subquery()
        )
        stmt3 = select(User, subq3.label("post_count"))

        # Execute examples
        result1 = await session.execute(stmt)
        result2 = await session.execute(stmt2)
        result3 = await session.execute(stmt3)

        return {
            "from_clause": result1.all(),
            "where_clause": result2.scalars().all(),
            "correlated": result3.all()
        }

    @staticmethod
    async def text_sql_examples(session: AsyncSession):
        # Raw SQL with parameters
        stmt1 = text("""
                     SELECT *
                     FROM users
                     WHERE is_active = :active
                       AND created_at > :date
                     """).bindparams(active=True, date=datetime(2023, 1, 1))

        # Raw SQL with result mapping
        stmt2 = text("""
                     SELECT u.name as user_name, COUNT(p.id) as post_count
                     FROM users u
                              LEFT JOIN posts p ON u.id = p.author_id
                     GROUP BY u.id
                     ORDER BY post_count DESC
                     """).columns(user_name=String, post_count=Integer)

        # Execute examples
        result1 = await session.execute(stmt1)
        result2 = await session.execute(stmt2)

        return {
            "parameterized": result1.mappings().all(),
            "mapped": result2.mappings().all()
        }


async def main():
    await init_db()

    async with async_session() as session:
        # Create sample data
        user1 = await DatabaseManager.create_single_user(session, {
            "name": "Alice",
            "email": "alice@example.com"
        })

        users = await DatabaseManager.create_bulk_users(session, [
            {"name": "Bob", "email": "bob@example.com"},
            {"name": "Charlie", "email": "charlie@example.com"}
        ])

        # Update examples
        await DatabaseManager.update_single_user(session, user1.id, {"name": "Alice Updated"})
        await DatabaseManager.update_bulk_users(session, [u.id for u in users], {"is_active": False})

        # List examples
        all_users = await DatabaseManager.list_all_users(session)
        paginated_users = await DatabaseManager.list_users_with_pagination(session, page=1, per_page=2)

        # Relationship examples
        user_with_profile = await DatabaseManager.get_user_with_profile(session, user1.id)
        active_users_with_posts = await DatabaseManager.get_users_with_posts(session, min_posts=1)

        # Aggregation examples
        post_stats = await DatabaseManager.get_post_stats(session)
        user_post_counts = await DatabaseManager.get_user_post_counts(session)

        # Filter examples
        filtered_users = await DatabaseManager.complex_filter_examples(session)

        # Join examples
        join_results = await DatabaseManager.join_examples(session)

        # Subquery examples
        subquery_results = await DatabaseManager.subquery_examples(session)

        # Text SQL examples
        text_results = await DatabaseManager.text_sql_examples(session)

        # Print some results
        print(f"Total users: {len(all_users)}")
        print(f"Post stats: {post_stats}")
        print(f"User post counts: {user_post_counts}")


if __name__ == "__main__":
    asyncio.run(main())
