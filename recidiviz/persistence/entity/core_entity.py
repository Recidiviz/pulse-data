# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Base class for all functionality that pertains to objects that model
our database schema, whether or not they are actual SQLAlchemy objects."""

from enum import Enum
from functools import cache
from typing import Any, List, Optional, Type

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.common.attr_utils import is_attr_decorated
from recidiviz.common.constants.state import enum_canonical_strings
from recidiviz.common.str_field_utils import to_snake_case


class CoreEntity:
    """Base class for all functionality that pertains to objects that model
    our database schema, whether or not they are actual SQLAlchemy objects."""

    # Consider CoreEntity abstract and only allow instantiating subclasses
    def __new__(cls: Any, *_: Any, **__: Any) -> Any:
        if cls is CoreEntity:
            raise NotImplementedError("Abstract class cannot be instantiated")
        return super().__new__(cls)

    @classmethod
    @cache
    def get_primary_key_column_name(cls) -> str:
        """Returns string name of primary key column of the table

        NOTE: This name is the *column* name on the table, which is not
        guaranteed to be the same as the *attribute* name on the ORM object.
        """

        return primary_key_name_from_cls(cls)

    @classmethod
    @cache
    def get_table_id(cls) -> str:
        """Returns the BQ table id for this entity.
        Example:
            StatePerson -> "state_person"
            NormalizedStateAssessment -> "state_assessment"
        """
        entity_name = cls.get_entity_name()
        if entity_name.startswith("normalized_"):
            return entity_name.removeprefix("normalized_")
        return entity_name

    @classmethod
    @cache
    def get_class_id_name(cls) -> str:
        table_name = cls.get_table_id()
        return table_name.removeprefix("state_") + "_id"

    def get_id(self) -> int:
        return getattr(self, self.get_class_id_name())

    @classmethod
    def has_field(cls, field: str) -> bool:
        if is_attr_decorated(cls):
            return field in attribute_field_type_reference_for_class(cls)
        return hasattr(cls, field)

    @classmethod
    @cache
    def get_entity_name(cls) -> str:
        return to_snake_case(cls.__name__)

    def clear_id(self) -> None:
        setattr(self, self.get_class_id_name(), None)

    def set_id(self, entity_id: int) -> None:
        return setattr(self, self.get_class_id_name(), entity_id)

    def get_external_id(self) -> Optional[str]:
        if not hasattr(self, "external_id"):
            return None
        return self.get_field("external_id")

    # TODO(#2163): Use get/set_field_from_list when possible to clean up code.
    def get_field_as_list(self, child_field_name: str) -> List[Any]:
        field = self.get_field(child_field_name)
        if field is None:
            return []
        if isinstance(field, list):
            return field
        return [field]

    def get_field(self, field_name: str) -> Any:
        if not hasattr(self, field_name):
            raise ValueError(
                f"Expected entity {type(self)} to have field {field_name}, "
                f"but it did not."
            )
        return getattr(self, field_name)

    def set_field(self, field_name: str, value: Any) -> None:
        if not hasattr(self, field_name):
            raise ValueError(
                f"Expected entity {type(self)} to have field {field_name}, "
                f"but it did not."
            )
        return setattr(self, field_name, value)

    def clear_field(self, field_name: str) -> None:
        """Clears the provided |field_name| off of the CoreEntity."""
        field = self.get_field(field_name)
        if isinstance(field, list):
            self.set_field(field_name, [])
        else:
            self.set_field(field_name, None)

    def set_field_from_list(self, field_name: str, value: List) -> None:
        """Given the provided |value|, sets the value onto the provided |entity|
        based on the given |field_name|.
        """
        field = self.get_field(field_name)
        if isinstance(field, list):
            self.set_field(field_name, value)
        else:
            if not value:
                self.set_field(field_name, None)
            elif len(value) == 1:
                self.set_field(field_name, value[0])
            else:
                raise ValueError(
                    f"Attempting to set singular field: {field_name} on "
                    f"entity: {self.get_entity_name()}, but got multiple "
                    f"values: {value}.",
                    self.get_entity_name(),
                )

    def is_default_enum(
        self,
        field_name: str,
        default_str_value: str = enum_canonical_strings.present_without_info,
    ) -> bool:
        value = self.get_field(field_name)
        raw_text_field_name = f"{field_name}_raw_text"
        if not hasattr(self, raw_text_field_name):
            # This is not an enum field
            return False
        raw_value = self.get_field(raw_text_field_name)
        if raw_value:
            # This is a mapped enum
            return False

        if isinstance(value, str):
            enum_str = value
        elif isinstance(value, Enum):
            enum_str = value.value
        else:
            raise ValueError(
                f"Unexpected type [{type(value)}] for enum field [{field_name}]: {value}."
            )

        return enum_str == default_str_value

    def limited_pii_repr(self) -> str:
        """String representation of a Core object that prints DB IDs and external ids
        for better debugging but does not print other information."""
        flat_properties = {self.get_primary_key_column_name(), "external_id", "id_type"}
        property_strs = sorted(
            [
                f"{key}={repr(self.get_field(key))}"
                for key in flat_properties
                if self.has_field(key)
            ]
        )

        if self.has_field("external_ids"):
            external_ids = ",".join(
                [e.limited_pii_repr() for e in self.get_field_as_list("external_ids")]
            )
            property_strs.append(f"external_ids=[{external_ids}]")
        properties_str = ", ".join(property_strs)
        return f"{self.__class__.__name__}({properties_str})"


def primary_key_name_from_cls(schema_cls: Type[CoreEntity]) -> str:
    return schema_cls.get_class_id_name()


def primary_key_name_from_obj(schema_object: CoreEntity) -> str:
    return schema_object.get_class_id_name()


def primary_key_value_from_obj(schema_object: CoreEntity) -> Optional[int]:
    return schema_object.get_id()
