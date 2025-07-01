from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from python_sqlalchemy.combination.orm_definition import ORMDefinition


class ORM(ORMDefinition):
    _engine = create_async_engine("sqlite+aiosqlite:///combination.sqlite3", echo=True)
    _sync_engine = create_engine("sqlite+aiosqlite:///combination.sqlite3", echo=True)

    @property
    def session(self):
        return async_sessionmaker(self._engine, expire_on_commit=False)

    @property
    def engine(self):
        return self._engine

    @property
    def sync_session(self):
        return sessionmaker(self._sync_engine, expire_on_commit=False)

    @property
    def sync_engine(self):
        return self._sync_engine


orm = ORM()
