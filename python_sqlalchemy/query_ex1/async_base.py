# orm/async_base.py

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import select as sa_select, delete as sa_delete, update as sa_update, func, and_, or_

from .async_session import get_session


class Base(DeclarativeBase):
    pass


class AsyncQuerySet:
    def __init__(self, model, session: AsyncSession):
        self.model = model
        self.session = session
        self._query = sa_select(model)
        self._filters = []
        self._order = []
        self._group = []
        self._joins = []

    def where(self, *conditions):
        self._filters.extend(conditions)
        return self

    def order_by(self, *ordering):
        self._order.extend(ordering)
        return self

    def group_by(self, *grouping):
        self._group.extend(grouping)
        return self

    def join(self, target, onclause=None, isouter=False):
        self._joins.append((target, onclause, isouter))
        return self

    def fields(self, *columns):
        self._query = sa_select(*columns)
        return self

    def paginate(self, page=1, page_size=20):
        self._query = self._query.limit(page_size).offset((page - 1) * page_size)
        return self

    async def count(self):
        q = sa_select(func.count()).select_from(self.model)
        if self._filters:
            q = q.where(*self._filters)
        result = await self.session.scalar(q)
        return result

    async def list(self):
        q = self._query
        if self._filters:
            q = q.where(*self._filters)
        if self._order:
            q = q.order_by(*self._order)
        if self._group:
            q = q.group_by(*self._group)
        for target, onclause, isouter in self._joins:
            q = q.join(target, onclause=onclause, isouter=isouter)
        result = await self.session.execute(q)
        return result.unique().scalars().all()

    async def first(self):
        q = self._query
        if self._filters:
            q = q.where(*self._filters)
        if self._order:
            q = q.order_by(*self._order)
        for target, onclause, isouter in self._joins:
            q = q.join(target, onclause=onclause, isouter=isouter)
        result = await self.session.execute(q.limit(1))
        return result.scalars().first()


class AsyncBaseModel(Base):
    __abstract__ = True

    @classmethod
    def select(cls):
        session = get_session()
        return AsyncQuerySet(cls, session)

    @classmethod
    async def create(cls, **kwargs):
        session = get_session()
        async with session.begin():
            obj = cls(**kwargs)
            session.add(obj)
        return obj

    @classmethod
    async def update(cls, where_clause, values: dict):
        session = get_session()
        async with session.begin():
            stmt = sa_update(cls).where(where_clause).values(**values)
            await session.execute(stmt)

    @classmethod
    async def bulk_update(cls, list_of_dicts: list[dict], key_field: str = "id"):
        session = get_session()
        async with session.begin():
            for data in list_of_dicts:
                stmt = sa_update(cls).where(getattr(cls, key_field) == data[key_field]).values(**data)
                await session.execute(stmt)

    @classmethod
    async def delete(cls, where_clause):
        session = get_session()
        async with session.begin():
            stmt = sa_delete(cls).where(where_clause)
            await session.execute(stmt)


# expose AND / OR
__all__ = ["AsyncBaseModel", "and_", "or_"]
