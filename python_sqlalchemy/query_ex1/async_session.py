from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

# Replace with your database URL (example: postgres+asyncpg://user:pass@localhost/db)
DATABASE_URL = "sqlite+aiosqlite:///./db.sqlite3"

async_engine = create_async_engine(DATABASE_URL, echo=True)

AsyncSessionFactory = async_sessionmaker(
    bind=async_engine,
    expire_on_commit=False,
)


def get_session():
    return AsyncSessionFactory()
