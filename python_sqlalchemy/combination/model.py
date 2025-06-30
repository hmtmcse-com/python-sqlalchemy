from datetime import datetime
from xmlrpc.client import DateTime

from sqlalchemy.orm import DeclarativeBase, Mapped

from python_sqlalchemy.combination.orm import orm


class BaseModel(DeclarativeBase):
    __abstract__ = True
    _model_list = []

    async def save(self):
        await self.bulk_save(self._model_list)
        self._model_list.clear()

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
                        session.add_all(models)
                await session.commit()
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
    id = orm.Column(orm.Integer(), primary_key=True)
    username = orm.Column(orm.String(), index=True, nullable=False)
    email = orm.Column(orm.String(), index=True, nullable=False)
    is_active = orm.Column(orm.Boolean(), default=True)
    created_at = orm.Column(orm.DateTime(), default=datetime.now)
