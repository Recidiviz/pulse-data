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
import datetime
from abc import abstractmethod
from enum import Enum
from typing import Any, Callable, Dict, Generic, Optional, Type, Union

import attr

from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attribute_field_type_reference_for_class,
)
from recidiviz.common.attr_utils import is_attr_decorated
from recidiviz.common.str_field_utils import (
    SerializableJSON,
    normalize,
    parse_bool,
    parse_date,
    parse_datetime,
    parse_int,
)
from recidiviz.persistence.entity.base_entity import Entity, EntityT
from recidiviz.utils.types import T

DeserializableEntityFieldValue = Optional[
    Union[
        str,
        Enum,
        bool,
        int,
        datetime.date,
        datetime.datetime,
        SerializableJSON,
    ]
]


@attr.s
class EntityFieldConverter(Generic[T]):
    field_type: Type[T] = attr.ib(
        validator=attr.validators.in_({str, Enum, SerializableJSON})
    )
    conversion_function: Callable[[T], Any] = attr.ib()

    def convert(self, field_value: T) -> Any:
        return self.conversion_function(field_value)


def entity_deserialize(
    cls: Type[EntityT],
    converter_overrides: Dict[str, EntityFieldConverter],
    defaults: Dict[str, Any],
    **kwargs: DeserializableEntityFieldValue,
) -> EntityT:
    """Factory function that parses ingested versions of the Entity constructor args
    into database-ready, normalized values and uses the normalized values to construct
    an instance of the object.

    Each field type is normalized in a standard way, but you can also pass in
    non-standard converters for any field via the |converter_overrides_opt| param.

    Null values will never be passed to an EntityFieldConverter. If you want to add a
    default value that will override any null field value, pass in the default via the
    |defaults| map.
    """

    if not is_attr_decorated(cls):
        raise ValueError(
            f"Can only deserialize attrs classes with entity_deserialize() - found class [{cls}]."
        )

    if not issubclass(cls, Entity):
        raise ValueError(
            f"Can only deserialize Entity classes with entity_deserialize() - found class [{cls}]."
        )

    def convert_field_value(
        field_name: str,
        field_type: BuildableAttrFieldType,
        field_value: DeserializableEntityFieldValue,
    ) -> Any:
        if field_value is None:
            return None

        if isinstance(field_value, str):
            if not field_value or not field_value.strip():
                return None

        if field_name in converter_overrides:
            converter = converter_overrides[field_name]
            if not isinstance(field_value, converter.field_type):
                raise ValueError(
                    f"Found converter for field [{field_name}] in the converter_overrides, but expected "
                    f"field type [{converter.field_type}] does not match actual field type "
                    f"[{type(field_value)}]"
                )
            return converter.convert(field_value)

        if isinstance(field_value, SerializableJSON):
            if field_type == BuildableAttrFieldType.STRING:
                return field_value.serialize()

        if isinstance(field_value, str):
            if field_type == BuildableAttrFieldType.STRING:
                return normalize(field_value)
            if field_type == BuildableAttrFieldType.DATETIME:
                # Pick an arbitrary from_dt so parsing is deterministic (used for
                # parsing strings like 10Y 1D into dates).
                return parse_datetime(
                    field_value, from_dt=datetime.datetime(2020, 1, 1)
                )
            if field_type == BuildableAttrFieldType.DATE:
                # Pick an arbitrary from_dt so parsing is deterministic (used for
                # parsing strings like 10Y 1D into dates).
                return parse_date(field_value, from_dt=datetime.datetime(2020, 1, 1))
            if field_type == BuildableAttrFieldType.INTEGER:
                return parse_int(field_value)
            if field_type == BuildableAttrFieldType.BOOLEAN:
                return parse_bool(field_value)

        if isinstance(field_value, Enum):
            if field_type == BuildableAttrFieldType.ENUM:
                return field_value

        if isinstance(field_value, datetime.datetime):
            if field_type == BuildableAttrFieldType.DATETIME:
                return field_value

        if isinstance(field_value, datetime.date):
            if field_type == BuildableAttrFieldType.DATE:
                return field_value

        if isinstance(field_value, int):
            if field_type == BuildableAttrFieldType.INTEGER:
                return field_value

        if isinstance(field_value, bool):
            if field_type == BuildableAttrFieldType.BOOLEAN:
                return field_value

        if field_type in (
            BuildableAttrFieldType.FORWARD_REF,
            BuildableAttrFieldType.LIST,
        ):
            return field_value

        raise ValueError(
            f"Unsupported field {field_name} with value: "
            f"{field_value} ({type(field_value)})."
        )

    converted_args = {}
    fields = set()

    class_reference = attribute_field_type_reference_for_class(cls)

    for field_name_ in class_reference.fields:
        if field_name_ in kwargs:
            converted_args[field_name_] = convert_field_value(
                field_name=field_name_,
                field_type=class_reference.get_field_info(field_name_).field_type,
                field_value=kwargs[field_name_],
            )
        if field_name_ in defaults:
            if converted_args.get(field_name_, None) is None:
                converted_args[field_name_] = defaults[field_name_]
        fields.add(field_name_)

    unexpected_kwargs = set(kwargs.keys()).difference(fields)
    if unexpected_kwargs:
        # Throw if there are unexpected args. NOTE: if there are missing required args,
        # that will be caught by the object construction itself.
        raise ValueError(
            f"Unexpected kwargs for class [{cls.__name__}]: {unexpected_kwargs}"
        )

    return cls(**converted_args)  # type: ignore[call-arg]


class EntityFactory(Generic[EntityT]):
    @staticmethod
    @abstractmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> Union[EntityT]:
        """Instantiates an entity from the provided list of arguments."""
