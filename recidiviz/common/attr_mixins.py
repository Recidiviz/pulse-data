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
import datetime
from enum import Enum
from typing import Any, Callable, Dict, Optional, Set, Type, TypeVar

import attr
from more_itertools import one

from recidiviz.common.attr_utils import (
    get_enum_cls,
    get_non_flat_attribute_class_name,
    is_bool,
    is_date,
    is_enum,
    is_forward_ref,
    is_int,
    is_list,
    is_str,
)
from recidiviz.common.str_field_utils import is_yyyymmdd_date, parse_yyyymmdd_date
from recidiviz.utils import environment
from recidiviz.utils.types import ClsT

DefaultableAttrType = TypeVar("DefaultableAttrType", bound="DefaultableAttr")


class BuildableAttrFieldType(Enum):
    """Defines categories of attribute types on BuildableAttrs, which assist in setting
    fields when building a BuildableAttr."""

    FORWARD_REF = "FORWARD_REF"
    LIST = "LIST"
    BOOLEAN = "BOOLEAN"
    ENUM = "ENUM"
    DATE = "DATE"
    STRING = "STRING"
    INTEGER = "INTEGER"
    OTHER = "OTHER"


@attr.s(kw_only=True, frozen=True)
class CachedAttributeInfo:
    attribute: attr.Attribute = attr.ib()
    field_type: BuildableAttrFieldType = attr.ib()
    enum_cls: Optional[Type[Enum]] = attr.ib()
    referenced_cls_name: Optional[str] = attr.ib()


# Cached _class_structure_reference value
_class_structure_reference: Optional[Dict[Type, Dict[str, CachedAttributeInfo]]] = None


def _get_class_structure_reference() -> Dict[Type, Dict[str, CachedAttributeInfo]]:
    """Returns the cached _class_structure_reference object, if it exists. If the
    _class_structure_reference is None, instantiates it as an empty dict."""
    global _class_structure_reference
    if not _class_structure_reference:
        _class_structure_reference = {}
    return _class_structure_reference


@environment.test_only
def _clear_class_structure_reference() -> None:
    global _class_structure_reference
    _class_structure_reference = None


def _attribute_field_type_reference_for_class(
    cls: Type,
) -> Dict[str, CachedAttributeInfo]:
    """Returns a dictionary mapping the attributes of the given class to the type of
    field the attribute is and, if the field is of type ENUM, the class of Enum stored
    in the field.

    Adds the attribute field type reference to the cached class_structure_reference if
    this class's attribute field types have not yet been indexed.
    """
    class_structure_reference = _get_class_structure_reference()
    attr_field_types = class_structure_reference.get(cls)

    if attr_field_types:
        return attr_field_types

    attr_field_types = _map_attr_to_type_for_class(cls)

    # Add the field type ref for this class to the cached reference
    class_structure_reference[cls] = attr_field_types
    return attr_field_types


def attr_field_type_for_field_name(
    cls: Type, field_name: str
) -> BuildableAttrFieldType:
    """Returns the BuildableAttrFieldType of the Attribute on the |cls| with the name
    matching the given |field_name|."""
    return _attribute_field_type_reference_for_class(cls)[field_name].field_type


def attr_field_attribute_for_field_name(cls: Type, field_name: str) -> attr.Attribute:
    """Returns the Attribute on the |cls| for the given |field_name|."""
    return _attribute_field_type_reference_for_class(cls)[field_name].attribute


def attr_field_enum_cls_for_field_name(
    cls: Type, field_name: str
) -> Optional[Type[Enum]]:
    """Returns the enum class of the Attribute on the |cls| with the name
    matching the given |field_name|. Returns None if the field is not an enum.
    """
    return _attribute_field_type_reference_for_class(cls)[field_name].enum_cls


def attr_field_name_storing_referenced_cls_name(
    base_cls: Type, referenced_cls_name: str
) -> Optional[str]:
    """Returns the name of the field on the |base_cls| that stores a reference to a
    class with the name |referenced_cls_name|, if one exists.

    Returns None if there are no fields on the |base_cls| storing a reference to a
    |referenced_cls_name| class. Raises an error if there exists more than one field
    storing the class.
    """
    fields_storing_referenced_cls = [
        field
        for field, info in _attribute_field_type_reference_for_class(base_cls).items()
        if info.referenced_cls_name and info.referenced_cls_name == referenced_cls_name
    ]

    if not fields_storing_referenced_cls:
        # There are no fields on the base_cls that store a type with the name matching
        # the |referenced_cls_name|
        return None

    if len(fields_storing_referenced_cls) > 1:
        raise ValueError(
            f"Class {base_cls} stores more than one reference to class "
            f"{referenced_cls_name}."
        )

    return one(fields_storing_referenced_cls)


def attr_field_referenced_cls_name_for_field_name(
    cls: Type, field_name: str
) -> Optional[str]:
    """Returns the name of the class referenced by the Attribute on the |cls| with the
    name matching the given |field_name|. Returns None if the field is not a FORWARD_REF
    or LIST type field.
    """
    return _attribute_field_type_reference_for_class(cls)[
        field_name
    ].referenced_cls_name


def _map_attr_to_type_for_class(
    cls: Type,
) -> Dict[str, CachedAttributeInfo]:
    """Helper function for _attribute_field_type_reference_for_class to map attributes
    to their BuildableAttrFieldType for a class if the attributes of the class aren't
    yet in the cached _class_structure_reference.
    """
    attr_field_types: Dict[str, CachedAttributeInfo] = {}

    for attribute in attr.fields_dict(cls).values():
        field_name = attribute.name
        if is_forward_ref(attribute):
            referenced_cls_name = get_non_flat_attribute_class_name(attribute)

            attr_field_types[field_name] = CachedAttributeInfo(
                attribute=attribute,
                field_type=BuildableAttrFieldType.FORWARD_REF,
                enum_cls=None,
                referenced_cls_name=referenced_cls_name,
            )
        elif is_list(attribute):
            # If the list stores forward references, get the name of the class stored
            # in the list
            referenced_cls_name = get_non_flat_attribute_class_name(attribute)

            attr_field_types[field_name] = CachedAttributeInfo(
                attribute=attribute,
                field_type=BuildableAttrFieldType.LIST,
                enum_cls=None,
                referenced_cls_name=referenced_cls_name,
            )
        elif is_enum(attribute):
            enum_cls = get_enum_cls(attribute)

            if not enum_cls:
                raise ValueError(
                    f"Did not find enum class for enum attribute [{attribute}]"
                )

            attr_field_types[field_name] = CachedAttributeInfo(
                attribute=attribute,
                field_type=BuildableAttrFieldType.ENUM,
                enum_cls=enum_cls,
                referenced_cls_name=None,
            )
        elif is_date(attribute):
            attr_field_types[field_name] = CachedAttributeInfo(
                attribute=attribute,
                field_type=BuildableAttrFieldType.DATE,
                enum_cls=None,
                referenced_cls_name=None,
            )
        elif is_str(attribute):
            attr_field_types[field_name] = CachedAttributeInfo(
                attribute=attribute,
                field_type=BuildableAttrFieldType.STRING,
                enum_cls=None,
                referenced_cls_name=None,
            )
        elif is_int(attribute):
            attr_field_types[field_name] = CachedAttributeInfo(
                attribute=attribute,
                field_type=BuildableAttrFieldType.INTEGER,
                enum_cls=None,
                referenced_cls_name=None,
            )
        elif is_bool(attribute):
            attr_field_types[field_name] = CachedAttributeInfo(
                attribute=attribute,
                field_type=BuildableAttrFieldType.BOOLEAN,
                enum_cls=None,
                referenced_cls_name=None,
            )
        else:
            attr_field_types[field_name] = CachedAttributeInfo(
                attribute=attribute,
                field_type=BuildableAttrFieldType.OTHER,
                enum_cls=None,
                referenced_cls_name=None,
            )

    return attr_field_types


class DefaultableAttr:
    """Mixin to add method to attr class that creates default object"""

    # DefaultableAttr can only be mixed in with an attr class
    def __new__(cls: Any, *_args: Any, **_kwargs: Any) -> Any:
        if not attr.has(cls):
            raise Exception("Parent class must be an attr class")
        return super().__new__(cls)

    @classmethod
    def new_with_defaults(
        cls: Type[DefaultableAttrType], **kwargs: Any
    ) -> DefaultableAttrType:
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


BuildableAttrType = TypeVar("BuildableAttrType", bound="BuildableAttr")


class BuildableAttr:
    """Mixin used to make attr object buildable"""

    # BuildableAttr can only be mixed in with an attr class
    def __new__(cls: Any, *_args: Any, **_kwargs: Any) -> Any:
        if not attr.has(cls):
            raise Exception("Parent class must be an attr class")
        return super().__new__(cls)

    class Builder:
        """Builder used to build the specified |cls| Attr object."""

        def __init__(self, cls: Type) -> None:
            # Directly set self.__dict__ to avoid invoking __setattr__
            self.__dict__["cls"] = cls
            self.__dict__["fields"] = {}

        def __setattr__(self, key: str, value: Any) -> None:
            self.fields[key] = value

        def __getattr__(self, key: str) -> Any:
            if key in self.fields:
                return self.fields[key]
            raise AttributeError(
                f"{self.__class__.__name__} object has no attribute {key}"
            )

        def build(self, constructor_fn_override: Optional[Callable] = None) -> Any:
            """Builds the given Attr class after verifying that all fields
            without a default value are set and that no extra fields are set.

            If a constructor_fn_override is set, uses that function as a constructor
            for the Attr class.
            """
            self._verify_has_all_and_only_required_fields()

            if constructor_fn_override:
                obj = constructor_fn_override(**self.fields)
                if not isinstance(obj, self.cls):
                    raise ValueError(
                        f"Overridden constructor function returned value of type [{type(obj)}], "
                        f"expected type [{type(self.cls)}]."
                    )
                return obj
            return self.cls(**self.fields)

        def _verify_has_all_and_only_required_fields(self) -> None:
            """Throws a |BuilderException| if:
            1. Any field without a default/factory value is left unset
            2. Any field is set that doesn't exist on the Attr
            """
            all_fields = attr.fields_dict(self.cls)

            required_fields = set(all_fields.keys())

            fields_provided = set(self.fields.keys())
            fields_with_defaults = {
                field
                for field, attribute in all_fields.items()
                if attribute.default is not attr.NOTHING
            }
            fields_with_value = fields_provided | fields_with_defaults

            if not required_fields == fields_with_value:
                raise BuilderException(self.cls, required_fields, fields_with_value)

    @classmethod
    def builder(cls) -> Builder:
        return cls.Builder(cls)

    @classmethod
    def build_from_dictionary(
        cls: Type[BuildableAttrType], build_dict: Dict[str, Any]
    ) -> Optional[BuildableAttrType]:
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

        attributes_and_types = _attribute_field_type_reference_for_class(cls)
        for field, cached_attribute_info in attributes_and_types.items():
            field_type = cached_attribute_info.field_type
            if field in build_dict:
                if field_type == BuildableAttrFieldType.FORWARD_REF:
                    # TODO(#1886): Implement detection of non-ForwardRefs
                    #  ForwardRef fields are expected to be references to other
                    #  BuildableAttrs
                    raise ValueError(
                        "build_dict should be a dictionary of "
                        "flat values. Should not contain any "
                        f"ForwardRef fields: {build_dict}"
                    )
                if field_type == BuildableAttrFieldType.ENUM:
                    type_cls = cached_attribute_info.enum_cls
                    if not type_cls:
                        raise ValueError(f"Expected Enum class for enum field {field}.")

                    value: Optional[Any] = cls.extract_enum_value(
                        type_cls, build_dict, field
                    )
                elif field_type == BuildableAttrFieldType.DATE:
                    value = cls.extract_date_value(build_dict, field)
                else:
                    value = build_dict.get(field)

                setattr(cls_builder, field, value)

        return cls_builder.build()

    @classmethod
    def extract_enum_value(
        cls,
        enum_cls: Type[Enum],
        build_dict: Dict[str, str],
        field: str,
    ) -> Optional[Enum]:
        value = build_dict.get(field)

        if value is None:
            return None

        if not isinstance(value, str) and not isinstance(value, enum_cls):
            raise ValueError(f"Unexpected type [{type(value)}] for value [{value}]")

        return enum_cls(value)

    @classmethod
    def extract_date_value(
        cls, build_dict: Dict[str, Any], field: str
    ) -> Optional[datetime.date]:
        value = build_dict.get(field)

        if value is None:
            return None

        if isinstance(value, str):
            if is_yyyymmdd_date(value):
                return parse_yyyymmdd_date(value)
            return datetime.datetime.strptime(value, "%Y-%m-%d").date()

        if isinstance(value, datetime.date):
            return value

        raise ValueError(f"Unexpected type [{type(value)}] for value [{value}].")


class BuilderException(Exception):
    """Exception raised if the Attr object cannot be built."""

    def __init__(
        self, cls: Type[ClsT], required_fields: Set[str], fields_with_value: Set[str]
    ) -> None:
        message = _error_message(cls, required_fields, fields_with_value)
        super().__init__(message)


def _error_message(
    cls: Type[ClsT], required_fields: Set[str], fields_with_value: Set[str]
) -> str:
    return f"""Failed to build {cls}.
        Expected Fields: {required_fields}
        Fields Provided or with Defaults: {fields_with_value}
        Missing Fields: {required_fields - fields_with_value}
        Extra Fields: {fields_with_value - required_fields}"""
