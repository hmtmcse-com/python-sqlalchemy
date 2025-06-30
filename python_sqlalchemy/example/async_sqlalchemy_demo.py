import asyncio
from sqlalchemy import (
    Column, Integer, String, ForeignKey, select, func, update, delete, text, between, and_, or_, literal_column
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import (
    DeclarativeBase, Mapped, mapped_column, relationship, aliased
)
from sqlalchemy.ext.asyncio import async_sessionmaker

# ----------------- Base setup -----------------
class Base(DeclarativeBase):
    pass

# ----------------- Models -----------------
class Author(Base):
    __tablename__ = "authors"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    books = relationship("Book", back_populates="author", order_by="Book.title")

class Book(Base):
    __tablename__ = "books"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String)
    pages: Mapped[int] = mapped_column(Integer)
    author_id: Mapped[int] = mapped_column(ForeignKey("authors.id"))
    author = relationship("Author", back_populates="books")

# ----------------- Async engine -----------------
engine = create_async_engine("sqlite+aiosqlite:///example1.db", echo=True)
async_session = async_sessionmaker(engine, expire_on_commit=False)

# ----------------- Core operations -----------------
async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        # Single create
        author = Author(name="J.K. Rowling")
        session.add(author)
        await session.commit()

        # Bulk create
        books = [
            Book(title="Harry Potter 1", pages=300, author=author),
            Book(title="Harry Potter 2", pages=350, author=author),
        ]
        session.add_all(books)
        await session.commit()

        # Single update
        stmt = update(Book).where(Book.title == "Harry Potter 1").values(pages=320)
        await session.execute(stmt)
        await session.commit()

        # Bulk update
        stmt = update(Book).values(pages=func.coalesce(Book.pages, 0) + 10)
        await session.execute(stmt)
        await session.commit()

        # List without pagination
        books_all = await session.execute(select(Book))
        for book in books_all.scalars():
            print("Book:", book.title)

        # List with pagination
        page = 1
        page_size = 1
        paged_books = await session.execute(
            select(Book).offset((page - 1) * page_size).limit(page_size)
        )
        for book in paged_books.scalars():
            print("Paged Book:", book.title)

        # Alias / Label
        book_alias = aliased(Book)
        stmt = select(book_alias.title.label("book_title"))
        result = await session.execute(stmt)
        for row in result:
            print("Alias title:", row.book_title)

        # Aggregations
        count_books = await session.execute(select(func.count(Book.id)))
        print("Total books:", count_books.scalar())

        # Filters with and_, or_, between, <, >, ==
        filtered = await session.execute(
            select(Book).where(
                and_(Book.pages > 300, or_(Book.title.like("%Harry%"), Book.pages.between(310, 400)))
            )
        )
        for book in filtered.scalars():
            print("Filtered Book:", book.title)

        # Joins
        stmt = select(Book, Author).join(Book.author)
        result = await session.execute(stmt)
        for book, author in result:
            print("Join book:", book.title, "by", author.name)

        # LEFT OUTER JOIN example (SQLite only supports left outer)
        stmt = select(Book, Author).join(Author, Book.author_id == Author.id, isouter=True)
        result = await session.execute(stmt)
        for book, author in result:
            print("LEFT JOIN book:", book.title, "by", author.name)

        # Subquery example
        subq = select(func.avg(Book.pages)).scalar_subquery()
        books_above_avg = await session.execute(
            select(Book).where(Book.pages > subq)
        )
        for book in books_above_avg.scalars():
            print("Book above avg pages:", book.title)

        # Raw text query
        result = await session.execute(text("SELECT title FROM books WHERE pages > :pages"), {"pages": 300})
        for row in result:
            print("Raw text book:", row.title)

asyncio.run(main())
