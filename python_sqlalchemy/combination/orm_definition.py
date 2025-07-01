from typing import Optional, Any, Union, Callable
from sqlalchemy import Integer, String, Boolean, DateTime, Date
from sqlalchemy.orm import MappedColumn
from sqlalchemy.orm.interfaces import _AttributeOptions
from sqlalchemy.sql.base import SchemaEventTarget, _NoArg
from sqlalchemy.sql.schema import _ServerOnUpdateArgument, _ServerDefaultArgument, SchemaConst
from sqlalchemy.sql._typing import _InfoType, _AutoIncrementType
from sqlalchemy.util.typing import Literal


class ORMDefinition:

    def Column(
            self,
            __name_pos = None,
            __type_pos = None,
            *args: SchemaEventTarget,
            init: Union[_NoArg, bool] = _NoArg.NO_ARG,
            repr: Union[_NoArg, bool] = _NoArg.NO_ARG,  # noqa: A002
            default: Optional[Any] = _NoArg.NO_ARG,
            default_factory = _NoArg.NO_ARG,
            compare: Union[_NoArg, bool] = _NoArg.NO_ARG,
            kw_only: Union[_NoArg, bool] = _NoArg.NO_ARG,
            hash: Union[_NoArg, bool, None] = _NoArg.NO_ARG,  # noqa: A002

            nullable: Optional[
                Union[bool, Literal[SchemaConst.NULL_UNSPECIFIED]]
            ] = SchemaConst.NULL_UNSPECIFIED,

            primary_key: Optional[bool] = False,
            deferred: Union[_NoArg, bool] = _NoArg.NO_ARG,
            deferred_group: Optional[str] = None,
            deferred_raiseload: Optional[bool] = None,
            use_existing_column: bool = False,
            name: Optional[str] = None,
            type_ = None,
            autoincrement: _AutoIncrementType = "auto",
            doc: Optional[str] = None,
            key: Optional[str] = None,
            index: Optional[bool] = None,
            unique: Optional[bool] = None,
            info: Optional[_InfoType] = None,
            onupdate: Optional[Any] = None,
            insert_default: Optional[Any] = _NoArg.NO_ARG,
            server_default: Optional[_ServerDefaultArgument] = None,
            server_onupdate: Optional[_ServerOnUpdateArgument] = None,
            active_history: bool = False,
            quote: Optional[bool] = None,
            system: bool = False,
            comment: Optional[str] = None,
            sort_order: Union[_NoArg, int] = _NoArg.NO_ARG,
            **kw: Any,
    ) -> MappedColumn[Any]:

        print(nullable)
        if nullable is None or nullable is True or nullable == SchemaConst.NULL_UNSPECIFIED:
            init = False
            nullable = True

        return MappedColumn(
            __name_pos,
            __type_pos,
            *args,
            name=name,
            type_=type_,
            autoincrement=autoincrement,
            insert_default=insert_default,
            attribute_options=_AttributeOptions(
                init, repr, default, default_factory, compare, kw_only, hash
            ),
            doc=doc,
            key=key,
            index=index,
            unique=unique,
            info=info,
            active_history=active_history,
            nullable=nullable,
            onupdate=onupdate,
            primary_key=primary_key,
            server_default=server_default,
            server_onupdate=server_onupdate,
            use_existing_column=use_existing_column,
            quote=quote,
            comment=comment,
            system=system,
            deferred=deferred,
            deferred_group=deferred_group,
            deferred_raiseload=deferred_raiseload,
            sort_order=sort_order,
            **kw,
        )

    def Integer(self):
        return Integer()

    def String(self, length: Optional[int] = None, collation: Optional[str] = None):
        return String(length=length, collation=collation)

    def Boolean(self, create_constraint: bool = False, name: Optional[str] = None):
        return Boolean(create_constraint=create_constraint, name=name)

    def DateTime(self):
        return DateTime()

    def Date(self):
        return Date()
