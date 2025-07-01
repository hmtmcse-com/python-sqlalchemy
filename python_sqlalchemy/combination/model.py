from dataclasses import dataclass
from datetime import datetime
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass
from python_sqlalchemy.combination.orm import orm


@dataclass(kw_only=True)
class BaseModel(DeclarativeBase, MappedAsDataclass):
    __abstract__ = True
    _model_list = []

    async def save(self):
        await self.bulk_save(self._model_list)
        self._model_list.clear()
        return self

    def delete(self):
        try:
            orm.session.delete(self)
            orm.session.commit()
        except Exception as e:
            raise Exception(e)

    def add(self, model):
        self._model_list.append(model)
        return self

    def add_all(self, models: list):
        self._model_list = models
        return self

    @staticmethod
    def save_all(models: list):
        if models:
            model = models.pop(0)
            if isinstance(model, BaseModel):
                model.bulk_save(models)
            models.append(model)

    async def bulk_save(self, models: list):
        try:
            async with orm.session() as session:
                self.before_save()
                session.add(self)
                if models:
                    for model in models:
                        model.before_save()
                        session.add(models)
                await session.flush()  # Need to study
                await session.commit() # Need to study
                await session.refresh(self)  # Should be configurable due to performance
                self.after_save()
                for model in models:
                    model.after_save()
        except Exception as e:
            raise Exception(e)

    def before_save(self):
        pass

    def after_save(self):
        pass


class User(BaseModel):
    __tablename__ = "user"
    id: int = orm.Column(orm.Integer(), primary_key=True, autoincrement=True, init=False)
    username: str = orm.Column(orm.String(), index=True, nullable=False)
    email: str = orm.Column(orm.String(), index=True, nullable=False)
    address: str = orm.Column(orm.String(), nullable=True)
    house: str = orm.Column(orm.String(), nullable=True)
    road: str = orm.Column(orm.String())
    is_active: bool = orm.Column(orm.Boolean(), default=True)
    created_at = orm.Column(orm.DateTime(), default=datetime.now)


