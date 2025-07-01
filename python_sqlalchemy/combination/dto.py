from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field, SQLAlchemyAutoSchema
from marshmallow import fields
from python_sqlalchemy.combination.model import User


class UserDTO(SQLAlchemySchema):
    class Meta:
        model = User
        load_instance = True

    email = fields.Email(required=True, error_messages={"required": "Please enter email."})
    username = auto_field()


class User2DTO(SQLAlchemyAutoSchema):
    class Meta:
        model = User

    name = fields.Email(required=True, error_messages={"required": "Please enter name."})
