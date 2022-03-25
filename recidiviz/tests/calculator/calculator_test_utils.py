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
"""Utils for testing the calculator code."""
from typing import Any, Dict, List, Optional

from recidiviz.common.constants.entity_enum import EntityEnum
from recidiviz.persistence.database.base_schema import StateBase

NormalizedDatabaseDict = Dict[str, Any]


def normalized_database_base_dict(
    database_base: StateBase,
    additional_attributes: Optional[NormalizedDatabaseDict] = None,
) -> NormalizedDatabaseDict:
    """Takes in a Base object and returns a dictionary with only keys
    that are column property names in the database. If any columns are not
    currently represented in the entity, sets the value as None for that key.
    For values that are EntityEnum, stores the value of the enum in the
    dictionary instead of the entire enum."""
    new_object_dict: NormalizedDatabaseDict = {}

    for column in database_base.get_column_property_names():
        # Set any required columns as None if they aren't present
        v = getattr(database_base, column, None)
        if isinstance(v, EntityEnum):
            new_object_dict[column] = v.value
        else:
            new_object_dict[column] = v

    if additional_attributes:
        new_object_dict.update(additional_attributes)

    return new_object_dict


def normalized_database_base_dict_list(
    database_bases: List[StateBase],
) -> List[NormalizedDatabaseDict]:
    """Returns a list of normalized database base dictionaries."""
    dict_list = []

    for database_base in database_bases:
        dict_list.append(normalized_database_base_dict(database_base))

    return dict_list


def remove_relationship_properties(database_base: StateBase) -> StateBase:
    """Removes the attributes corresponding to relationship properties
    on the Base. Used for tests to remove unwanted forward and back edges
    that mess with equality when hydrating only one degree away from a
    root entity."""
    for relationship_property in database_base.get_relationship_property_names():
        if (
            hasattr(database_base, relationship_property)
            and getattr(database_base, relationship_property) is not None
        ):
            database_base.__delattr__(relationship_property)

    return database_base
