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
# ============================================================================
"""Logic for Attr objects that can be built with a Builder."""
from enum import Enum
from typing import Any, Dict, Optional, Type, Set, TypeVar
import datetime
import attr

from recidiviz.common.attr_utils import is_enum, is_forward_ref, get_enum_cls, is_date
from recidiviz.common.str_field_utils import is_yyyymmdd_date, parse_yyyymmdd_date
from recidiviz.utils.types import ClsType

DefaultableAttrType = TypeVar('DefaultableAttrType', bound='DefaultableAttr')


class DefaultableAttr:
    """Mixin to add method to attr class that creates default object"""

    # DefaultableAttr can only be mixed in with an attr class
    def __new__(cls: Any, *_args: Any, **_kwargs: Any) -> Any:
        if not attr.has(cls):
            raise Exception('Parent class must be an attr class')
        return super().__new__(cls)

    @classmethod
    def new_with_defaults(cls: Type[DefaultableAttrType], **kwargs: Any) -> DefaultableAttrType:
        """Create a new object with default values if set, otherwise None.

        Note: This method should only be used in tests. In prod you should
        always use the Attr's __init__ or builder which will verify that all
        fields on the Attr are set.

        Arguments:
            kwargs: The kwargs to pass to Attr object's __init__, the rest of
            the attributes are set to their default or None if a default is
            unspecified.
        """
        for field, attribute in attr.fields_dict(cls).items():
            default = attribute.default

            # Don't set a default if the field is already set
            if field in kwargs:
                continue

            # Ignore Factories to allow them to render into a default value
            if isinstance(default, attr.Factory):  # type: ignore
                continue

            kwargs[field] = None if default is attr.NOTHING else default

        return cls(**kwargs)


class BuildableAttr:
    """Mixin used to make attr object buildable"""

    # BuildableAttr can only be mixed in with an attr class
    def __new__(cls: Any, *_args: Any, **_kwargs: Any) -> Any:
        if not attr.has(cls):
            raise Exception('Parent class must be an attr class')
        return super().__new__(cls)

    class Builder:
        """Builder used to build the specified |cls| Attr object."""

        def __init__(self, cls: Type) -> None:
            # Directly set self.__dict__ to avoid invoking __setattr__
            self.__dict__['cls'] = cls
            self.__dict__['fields'] = {}

        def __setattr__(self, key: str, value: Any) -> None:
            self.fields[key] = value

        def __getattr__(self, key: str) -> Any:
            if key in self.fields:
                return self.fields[key]
            raise AttributeError("{} object has no attribute {}".format(
                self.__class__.__name__, key))

        def build(self) -> Any:
            """Builds the given Attr class after verifying that all fields
            without a default value are set and that no extra fields are set."""
            self._verify_has_all_and_only_required_fields()
            return self.cls(**self.fields)

        def _verify_has_all_and_only_required_fields(self) -> None:
            """Throws a |BuilderException| if:
                1. Any field without a default/factory value is left unset
                2. Any field is set that doesn't exist on the Attr
            """
            required_fields = set(attr.fields_dict(self.cls).keys())

            fields_provided = set(self.fields.keys())
            fields_with_defaults = {field for field, attribute in
                                    attr.fields_dict(self.cls).items() if
                                    attribute.default is not attr.NOTHING}
            fields_with_value = fields_provided | fields_with_defaults

            if not required_fields == fields_with_value:
                raise BuilderException(
                    self.cls, required_fields, fields_with_value)

    @classmethod
    def builder(cls) -> Builder:
        return cls.Builder(cls)

    @classmethod
    def build_from_dictionary(cls, build_dict: Dict[str, Any]) -> \
            Optional['BuildableAttr']:
        """Builds a BuildableAttr with values from the given build_dict.

        Given build_dict must contain all required fields, and cannot contain
        any fields with ForwardRef attribute types. Any date values
        must be in the format 'YYYY-MM-DD' if they are present.
        """
        if not attr.has(cls):
            raise Exception("Parent class must be an attr class")

        if not build_dict:
            raise ValueError("build_dict cannot be empty")

        cls_builder = cls.builder()

        for field, attribute in attr.fields_dict(cls).items():
            if field in build_dict:
                if is_forward_ref(attribute):
                    # TODO(#1886): Implement detection of non-ForwardRefs
                    # ForwardRef fields are expected to be references to other
                    # BuildableAttrs
                    raise ValueError("build_dict should be a dictionary of "
                                     "flat values. Should not contain any "
                                     f"ForwardRef fields: {build_dict}")

                if is_enum(attribute):
                    value: Optional[Any] = cls.extract_enum_value(build_dict, field, attribute)
                elif is_date(attribute):
                    value = cls.extract_date_value(build_dict, field)
                else:
                    value = build_dict.get(field)

                setattr(cls_builder, field, value)

        return cls_builder.build()

    @classmethod
    def extract_enum_value(cls, build_dict: Dict[str, str], field: str, attribute: attr.Attribute) -> Optional[Enum]:
        enum_cls = get_enum_cls(attribute)

        if not enum_cls:
            raise ValueError(f'Did not find enum class for attribute [{attribute}]')

        value = build_dict.get(field)

        if value is None:
            return None

        if not isinstance(value, str) and not isinstance(value, enum_cls):
            raise ValueError(f'Unexpected type [{type(value)}] for value [{value}]')

        return enum_cls(value)

    @classmethod
    def extract_date_value(cls, build_dict: Dict[str, Any], field: str) -> Optional[datetime.date]:
        value = build_dict.get(field)

        if value is None:
            return None

        if isinstance(value, str):
            if is_yyyymmdd_date(value):
                return parse_yyyymmdd_date(value)
            return datetime.datetime.strptime(value, '%Y-%m-%d').date()

        if isinstance(value, datetime.date):
            return value

        raise ValueError(f'Unexpected type [{type(value)}] for value [{value}].')


class BuilderException(Exception):
    """Exception raised if the Attr object cannot be built."""

    def __init__(self,
                 cls: ClsType,
                 required_fields: Set[str],
                 fields_with_value: Set[str]) -> None:
        message = _error_message(cls, required_fields, fields_with_value)
        super().__init__(message)


def _error_message(cls: ClsType,
                   required_fields: Set[str],
                   fields_with_value: Set[str]) -> str:
    return \
        """Failed to build {cls}.
        Expected Fields: {expected_fields}
        Fields Provided or with Defaults: {fields_with_value}
        Missing Fields: {missing_fields}
        Extra Fields: {extra_fields}""".format(
            cls=cls,
            expected_fields=required_fields,
            fields_with_value=fields_with_value,
            missing_fields=required_fields - fields_with_value,
            extra_fields=fields_with_value - required_fields)
