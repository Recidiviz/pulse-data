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

"""Utilities for working with the database schemas."""

import inspect
import sys
from types import ModuleType
from typing import Iterator, Optional, Type, List

from functools import lru_cache
from sqlalchemy import Table
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.persistence.database.base_schema import JailsBase, \
    StateBase
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.aggregate import \
    schema as aggregate_schema
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.database.schema.state import schema as state_schema

_SCHEMA_MODULES: List[ModuleType] = \
    [aggregate_schema, county_schema, state_schema]


def get_all_table_classes() -> Iterator[Table]:
    for module in _SCHEMA_MODULES:
        yield from get_all_table_classes_in_module(module)


def get_all_table_classes_in_module(
        module: ModuleType) -> Iterator[Type[DatabaseEntity]]:
    all_members_in_current_module = \
        inspect.getmembers(sys.modules[module.__name__])
    for _, member in all_members_in_current_module:
        if (inspect.isclass(member)
                and issubclass(member, DatabaseEntity)
                and member is not DatabaseEntity
                and member is not JailsBase
                and member is not StateBase):
            yield member


def get_aggregate_table_classes() -> Iterator[Table]:
    yield from get_all_table_classes_in_module(aggregate_schema)


def get_state_table_classes() -> Iterator[Table]:
    yield from get_all_table_classes_in_module(state_schema)


@lru_cache(maxsize=None)
def get_state_table_class_with_name(class_name: str) -> Table:
    state_table_classes = get_state_table_classes()

    for member in state_table_classes:
        if member.__name__ == class_name:
            return member

    raise LookupError(f"Entity class {class_name} does not exist in"
                      f"{state_schema.__name__}.")


HISTORICAL_TABLE_CLASS_SUFFIX = 'History'


def historical_table_class_name_from_obj(schema_object: DatabaseEntity) -> str:
    obj_class_name = schema_object.__class__.__name__
    if obj_class_name.endswith(HISTORICAL_TABLE_CLASS_SUFFIX):
        return obj_class_name

    return f'{obj_class_name}{HISTORICAL_TABLE_CLASS_SUFFIX}'


def historical_table_class_from_obj(
        schema_object: DatabaseEntity) -> Optional[Type[DatabaseEntity]]:
    schema_module = inspect.getmodule(schema_object)
    history_table_class_name = \
        historical_table_class_name_from_obj(schema_object)
    return getattr(schema_module, history_table_class_name, None)


def schema_base_for_system_level(system_level: SystemLevel) -> DeclarativeMeta:
    if system_level == SystemLevel.STATE:
        return StateBase
    if system_level == SystemLevel.COUNTY:
        return JailsBase

    raise ValueError(f"Unsupported SystemLevel type: {system_level}")


def schema_base_for_schema_module(module: ModuleType) -> DeclarativeMeta:
    if module in (aggregate_schema, county_schema):
        return JailsBase
    if module == state_schema:
        return StateBase

    raise ValueError(f"Unsupported module: {module}")


def schema_base_for_object(schema_object: DatabaseEntity):
    if isinstance(schema_object, JailsBase):
        return JailsBase
    if isinstance(schema_object, StateBase):
        return StateBase

    raise ValueError(
        f"Object of type [{type(schema_object)}] has unknown schema base.")
