#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Utils for working with raw data file configs."""
from typing import Any, Optional, Type, get_args

import attr
from more_itertools import one

from recidiviz.common.attr_utils import get_non_optional_type, is_list_type
from recidiviz.utils.types import non_optional

# This metadata tag marks the attribute that uniquely identifies an item in a list
# when performing list diffing. Each class used as a list element must have exactly
# one field with this metadata tag set to True. This allows the system to match
# and compare list items by their unique identifier.
LIST_ITEM_IDENTIFIER_TAG = "list_item_identifier"


def get_list_item_identifier(cls: Type[Any]) -> Optional[str]:
    """Returns the name of the field in the class that has metadata
    {LIST_ITEM_IDENTIFIER_TAG: True}.

    Ensures that the class has exactly one such field. If more than one field is marked,
    raises a ValueError. If none are found, returns None.
    """
    id_field_name = None
    for field in attr.fields(cls):
        if field.metadata.get(LIST_ITEM_IDENTIFIER_TAG) is True:
            if id_field_name is not None:
                raise ValueError(
                    f"Class [{cls.__name__}] has multiple fields with metadata "
                    f"{LIST_ITEM_IDENTIFIER_TAG}: True. Only one field can have this "
                    f"metadata tag."
                )
            id_field_name = field.name

    return id_field_name


def validate_list_item_identifiers(
    cls: Type[Any], excluded_tag: Optional[str] = None
) -> None:
    """Recursively validates that all list-typed fields in the given class and its child classes refer to
     element classes that contain exactly one field marked with metadata {LIST_ITEM_IDENTIFIER_TAG: True}.

    This ensures that list elements can be uniquely identified when diffing.

    Ignores fields that have the provided |excluded_tag| set to true in their metadata. Ignores child
    classes that are not attr-decorated, as they are not expected to be used in list diffing.

    Assumes class definitions are non-cyclical. Assumes child classes are referenced directly
    or in lists (doesn't handle child class references in dicts, tuples, etc.).
    """

    for field in attr.fields(cls):
        if excluded_tag and field.metadata.get(excluded_tag) is True:
            continue

        field_type = get_non_optional_type(non_optional(field.type))
        if attr.has(field_type):
            validate_list_item_identifiers(cls=field_type, excluded_tag=excluded_tag)
            continue

        if not is_list_type(field_type):
            continue

        element_type = one(t for t in get_args(field_type))
        if not attr.has(element_type):
            continue

        if not get_list_item_identifier(element_type):
            raise ValueError(
                f"List field [{field.name}] in class [{cls.__name__}] has elements of type "
                f"[{element_type.__name__}], but that class has no field marked with "
                f"metadata LIST_ITEM_IDENTIFIER_TAG: True. Each list element class must have exactly one such field "
                f"in order to diff lists properly."
            )

        validate_list_item_identifiers(cls=element_type, excluded_tag=excluded_tag)


def is_meaningful_docstring(docstring: str | None) -> bool:
    """Returns true if the provided docstring gives meaningful information, i.e. it is
    non-empty and does not start with an obvious placeholder.
    """
    if not docstring:
        return False

    stripped_docstring = docstring.strip()
    if not stripped_docstring:
        return False

    return (
        # Split up into TO and DO to avoid lint errors
        not stripped_docstring.startswith("TO" + "DO")
        and not stripped_docstring.startswith("XXX")
    )
