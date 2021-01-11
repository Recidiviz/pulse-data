# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Provides a decorator for augmenting Entity classes with a deserialization constructor."""

from typing import Optional, Dict, Callable, Any, Type, Union, TypeVar

import attr

from recidiviz.common.attr_utils import is_forward_ref, is_list, is_str, is_date, is_enum, is_int, is_attr_decorated
from recidiviz.common.str_field_utils import normalize, parse_date, parse_int, parse_bool
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.common.constants.enum_parser import EnumParser

EntityFieldConverterType = Callable[[str], Any]

MaybeEntityT = TypeVar('MaybeEntityT', bound=Entity)
EntityT = TypeVar('EntityT', bound=Entity)


def add_deserialize_constructor(
        converter_overrides_opt: Optional[Dict[str, EntityFieldConverterType]] = None
) -> Callable:
    """Decorator that augments an Entity class with a `deserialize` class method that parses ingested versions of the
    Entity constructor args into database-ready, normalized values.

    Usage:
        @add_deserialize_constructor()
        @attr.s(eq=False)
        class MyEntityClass(Entity):
            some_field: int = attr.ib()

        m = MyEntityClass.deserialize(some_field='1')
        m.some_field == 1 => True

    Each field type is normalized in a standard way, but you can also pass in non-standard converters for any field via
    the |converter_overrides_opt| param.
    """

    converter_overrides: Dict[str, EntityFieldConverterType] = converter_overrides_opt or {}

    def decorator(maybe_cls: Type[MaybeEntityT]) -> Type[MaybeEntityT]:
        if not is_attr_decorated(maybe_cls):
            raise ValueError(
                f'Can only decorate attrs classes with @add_deserialize_constructor() - found class [{maybe_cls}].')

        if not issubclass(maybe_cls, Entity):
            raise ValueError(
                f'Can only decorate Entity classes with @add_deserialize_constructor() - found class [{maybe_cls}].')

        def convert_field_value(field: attr.Attribute, field_value: Any) -> Any:
            if field_value is None:
                return None

            if is_forward_ref(field) or is_list(field):
                return field_value

            if isinstance(field_value, EnumParser):
                if is_enum(field):
                    return field_value.parse()
                raise ValueError(f'Found field value [{field_value}] for field that is not an enum [{field}].')

            if isinstance(field_value, str):
                if not field_value or not field_value.strip():
                    return None

                if field.name in converter_overrides:
                    return converter_overrides[field.name](field_value)
                if is_str(field):
                    return normalize(field_value)
                if is_date(field):
                    return parse_date(field_value)
                if is_int(field):
                    return parse_int(field_value)
                if field.type in {bool, Union[bool, None]}:
                    return parse_bool(field_value)

            raise ValueError(f'Unsupported field {field.name}')

        def deserialize(cls: Type[EntityT], **kwargs: Union[str, EnumParser]) -> EntityT:
            converted_args = {}
            for field_name, field in attr.fields_dict(cls).items():
                if field_name in kwargs:
                    converted_args[field_name] = convert_field_value(field, kwargs[field_name])

            return cls(**converted_args)  # type: ignore[call-arg]

        if hasattr(maybe_cls, "deserialize"):
            raise ValueError(
                f'Method |deserialize| already present for cls [{maybe_cls}].')

        setattr(maybe_cls, "deserialize", classmethod(deserialize))
        return maybe_cls
    return decorator
