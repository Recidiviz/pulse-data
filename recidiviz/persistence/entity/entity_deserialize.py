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

from typing import Dict, Callable, Any, Type, Union, TypeVar, Generic

import attr

from recidiviz.common.attr_utils import is_forward_ref, is_list, is_str, is_date, is_enum, is_int, is_attr_decorated
from recidiviz.common.str_field_utils import normalize, parse_date, parse_int, parse_bool
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.common.constants.enum_parser import EnumParser
from recidiviz.utils.types import T

EntityT = TypeVar('EntityT', bound=Entity)


@attr.s
class EntityFieldConverter(Generic[T]):
    field_type: Type[T] = attr.ib(validator=attr.validators.in_({str, EnumParser}))
    conversion_function: Callable[[T], Any] = attr.ib()

    def convert(self, field_value: T) -> Any:
        return self.conversion_function(field_value)


def entity_deserialize(cls: Type[EntityT],
                       converter_overrides: Dict[str, EntityFieldConverter],
                       **kwargs: Union[str, EnumParser]) -> EntityT:
    """Factory function that parses ingested versions of the Entity constructor args into database-ready, normalized
    values and uses the normalized values to construct an instance of the object.

    Each field type is normalized in a standard way, but you can also pass in non-standard converters for any field via
    the |converter_overrides_opt| param.
    """

    if not is_attr_decorated(cls):
        raise ValueError(
            f'Can only deserialize attrs classes with entity_deserialize() - found class [{cls}].')

    if not issubclass(cls, Entity):
        raise ValueError(
            f'Can only deserialize Entity classes with entity_deserialize() - found class [{cls}].')

    def convert_field_value(field: attr.Attribute, field_value: Union[str, EnumParser]) -> Any:
        if field_value is None:
            return None

        if is_forward_ref(field) or is_list(field):
            return field_value

        if isinstance(field_value, str):
            if not field_value or not field_value.strip():
                return None

        if field.name in converter_overrides:
            converter = converter_overrides[field.name]
            if not isinstance(field_value, converter.field_type):
                raise ValueError(f'Found converter for field [{field.name}] in the converter_overrides, but expected '
                                 f'field type [{converter.field_type}] does not match actual field type '
                                 f'[{type(field_value)}]')
            return converter.convert(field_value)

        if isinstance(field_value, EnumParser):
            if is_enum(field):
                return field_value.parse()
            raise ValueError(f'Found field value [{field_value}] for field that is not an enum [{field}].')

        if isinstance(field_value, str):
            if is_str(field):
                return normalize(field_value)
            if is_date(field):
                return parse_date(field_value)
            if is_int(field):
                return parse_int(field_value)
            if field.type in {bool, Union[bool, None]}:
                return parse_bool(field_value)

        raise ValueError(f'Unsupported field {field.name}')

    converted_args = {}
    for field_name, field_ in attr.fields_dict(cls).items():
        if field_name in kwargs:
            converted_args[field_name] = convert_field_value(field_, kwargs[field_name])

    return cls(**converted_args)  # type: ignore[call-arg]
