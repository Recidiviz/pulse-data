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
import functools
import inspect
import sys
from functools import lru_cache
from types import ModuleType
from typing import Any, Iterator, List, Type

from sqlalchemy import Table
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.persistence.database.base_schema import (
    CaseTriageBase,
    InsightsBase,
    JusticeCountsBase,
    OperationsBase,
    PathwaysBase,
    StateBase,
    WorkflowsBase,
)
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.case_triage import (
    schema as case_triage_schema,
)
from recidiviz.persistence.database.schema.insights import schema as insights_schema
from recidiviz.persistence.database.schema.justice_counts import (
    schema as justice_counts_schema,
)
from recidiviz.persistence.database.schema.operations import schema as operations_schema
from recidiviz.persistence.database.schema.pathways import schema as pathways_schema
from recidiviz.persistence.database.schema.resource_search.schema import (
    ResourceSearchBase,
)
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema.workflows import schema as workflows_schema
from recidiviz.persistence.database.schema_type import SchemaType

_SCHEMA_MODULES: List[ModuleType] = [
    case_triage_schema,
    justice_counts_schema,
    pathways_schema,
    state_schema,
    operations_schema,
    workflows_schema,
    insights_schema,
]


def get_all_table_classes() -> Iterator[Table]:
    for schema_type in SchemaType:
        yield from get_all_table_classes_in_schema(schema_type)


def get_table_class_by_name(table_name: str, schema_type: SchemaType) -> Table:
    """Return a Table class object by its table_name"""
    tables_by_name = {t.name: t for t in get_all_table_classes_in_schema(schema_type)}

    if table_name not in tables_by_name:
        raise ValueError(
            f"{table_name}: Table name not found in list of tables: "
            f"{sorted(tables_by_name)}"
        )

    return tables_by_name[table_name]


def is_association_table(table_name: str) -> bool:
    return table_name.endswith("_association")


@functools.cache
def get_all_table_classes_in_schema(schema_type: SchemaType) -> List[Table]:
    metadata_base = schema_type_to_schema_base(schema_type)
    tables = metadata_base.metadata.sorted_tables
    if not tables:
        raise ValueError(f"Found no tables in schema [{schema_type.value}]")
    return tables


def get_pathways_database_entities() -> List[Type[DatabaseEntity]]:
    return list(get_all_database_entities_in_module(pathways_schema))


def get_state_database_entities() -> List[Type[DatabaseEntity]]:
    to_return = []
    for cls in get_all_database_entities_in_module(state_schema):
        to_return.append(cls)
    return to_return


def get_all_database_entities_in_module(
    module: ModuleType,
) -> Iterator[Type[DatabaseEntity]]:
    """This should only be called in tests and by the
    `get_state_database_entity_with_name` function."""
    all_members_in_current_module = inspect.getmembers(sys.modules[module.__name__])
    for _, member in all_members_in_current_module:
        if _is_database_entity_subclass(member):
            yield member


def get_database_entity_by_table_name(
    module: ModuleType, table_name: str
) -> Type[DeclarativeMeta]:
    for name in dir(module):
        member = getattr(module, name)
        if not _is_database_entity_subclass(member):
            continue

        if member.__table__.name == table_name:
            return member

    raise ValueError(f"Could not find model with table named {table_name}")


def _is_database_entity_subclass(member: Any) -> bool:
    return (
        inspect.isclass(member)
        and issubclass(member, DatabaseEntity)
        and member is not DatabaseEntity
        and (
            member
            not in (
                schema_type_to_schema_base(schema_type) for schema_type in SchemaType
            )
        )
    )


@lru_cache(maxsize=None)
def get_state_database_entity_with_name(class_name: str) -> Type[StateBase]:
    state_table_classes = get_all_database_entities_in_module(state_schema)

    for member in state_table_classes:
        if member.__name__ == class_name:
            return member

    raise LookupError(
        f"Entity class {class_name} does not exist in " f"{state_schema.__name__}."
    )


def schema_type_to_schema_base(schema_type: SchemaType) -> DeclarativeMeta:
    match schema_type:
        case SchemaType.STATE:
            return StateBase
        case SchemaType.OPERATIONS:
            return OperationsBase
        case SchemaType.JUSTICE_COUNTS:
            return JusticeCountsBase
        case SchemaType.CASE_TRIAGE:
            return CaseTriageBase
        case SchemaType.PATHWAYS:
            return PathwaysBase
        case SchemaType.WORKFLOWS:
            return WorkflowsBase
        case SchemaType.INSIGHTS:
            return InsightsBase
        case SchemaType.RESOURCE_SEARCH:
            return ResourceSearchBase

    raise ValueError(f"Unexpected schema type [{schema_type}].")
