from datetime import datetime
from sqlalchemy.orm import DeclarativeBase
from python_sqlalchemy.combination.orm import orm


class BaseModel(DeclarativeBase):
    pass


class User(BaseModel):
    __tablename__ = "user"
    id = orm.Column(orm.Integer(), primary_key=True)
    username = orm.Column(orm.String(), unique=True, index=True, nullable=False)
    email = orm.Column(orm.String(), unique=True, index=True, nullable=False)
    is_active = orm.Column(orm.Boolean(), default=True)
    created_at = orm.Column(orm.DateTime(), default=datetime.now)
