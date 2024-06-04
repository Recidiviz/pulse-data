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
from typing import Any, Iterator, List, Optional, Tuple, Type

import sqlalchemy
from sqlalchemy import ForeignKeyConstraint, Table
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import RelationshipProperty

from recidiviz.persistence.database.base_schema import (
    CaseTriageBase,
    InsightsBase,
    JusticeCountsBase,
    OperationsBase,
    OutliersBase,
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
from recidiviz.persistence.database.schema.outliers import schema as outliers_schema
from recidiviz.persistence.database.schema.pathways import schema as pathways_schema
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema.workflows import schema as workflows_schema
from recidiviz.persistence.database.schema_type import SchemaType

_SCHEMA_MODULES: List[ModuleType] = [
    case_triage_schema,
    justice_counts_schema,
    pathways_schema,
    state_schema,
    operations_schema,
    outliers_schema,
    workflows_schema,
    insights_schema,
]

BQ_TYPES = {
    sqlalchemy.Boolean: "BOOL",
    sqlalchemy.Date: "DATE",
    sqlalchemy.DateTime: "DATETIME",
    sqlalchemy.Enum: "STRING",
    sqlalchemy.Integer: "INT64",
    sqlalchemy.String: "STRING",
    sqlalchemy.Text: "STRING",
    sqlalchemy.ARRAY: "ARRAY",
}


def get_all_table_classes() -> Iterator[Table]:
    for schema_type in SchemaType:
        yield from get_all_table_classes_in_schema(schema_type)


def get_foreign_key_constraints(table: Table) -> List[ForeignKeyConstraint]:
    return [
        constraint
        for constraint in table.constraints
        if isinstance(constraint, ForeignKeyConstraint)
    ]


def get_table_class_by_name(table_name: str, schema_type: SchemaType) -> Table:
    """Return a Table class object by its table_name"""
    for table in get_all_table_classes_in_schema(schema_type):
        if table.name == table_name:
            return table
    raise ValueError(f"{table_name}: Table name not found in list of tables.")


def get_region_code_col(schema_type: SchemaType, table: Table) -> str:
    if schema_type is SchemaType.STATE:
        if hasattr(table.c, "state_code") or is_association_table(table.name):
            return "state_code"
    if schema_type is SchemaType.OPERATIONS:
        if hasattr(table.c, "region_code"):
            return "region_code"
    raise ValueError(f"Unexpected table is missing a region code field: [{table.name}]")


def schema_has_region_code_query_support(schema_type: SchemaType) -> bool:
    """NOTE: The CloudSQL -> BQ refresh must run once without any filtered region codes for each newly added SchemaType.
    This ensures the region_code column is added to tables that are missing it before a query tries to
    filter for that column.
    """
    return schema_type in (SchemaType.STATE, SchemaType.OPERATIONS)


def is_association_table(table_name: str) -> bool:
    return table_name.endswith("_association")


@functools.cache
def get_all_table_classes_in_schema(schema_type: SchemaType) -> List[Table]:
    metadata_base = schema_type_to_schema_base(schema_type)
    return metadata_base.metadata.sorted_tables


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


def get_database_entities_by_association_table(
    module: ModuleType, table_name: str
) -> Tuple[Type[DeclarativeMeta], Type[DeclarativeMeta]]:
    """Returns the database entities associated with the association table."""
    parent_member = None
    child_member = None
    for name in dir(module):
        member = getattr(module, name)
        if not _is_database_entity_subclass(member):
            continue

        try:
            for (
                relationship
            ) in member.get_relationship_property_names_and_properties().values():
                if relationship.secondary.name == table_name:
                    parent_member = member
                    child_member = getattr(module, relationship.argument)
        except AttributeError:
            continue

    if not child_member or not parent_member:
        raise ValueError(
            f"Could not find model with association table named {table_name}"
        )

    return child_member, parent_member


def get_primary_key_column_name(
    schema_module: ModuleType, table_name: str
) -> Optional[str]:
    """Return the column name of the primary key associated with the given table name"""
    if is_association_table(table_name):
        # Association tables don't have primary keys
        return None

    database_entity = get_database_entity_by_table_name(schema_module, table_name)
    return database_entity.get_primary_key_column_name()


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


@lru_cache(maxsize=None)
def get_state_database_association_with_names(
    child_class_name: str, parent_class_name: str
) -> Table:
    parent_entity = get_state_database_entity_with_name(parent_class_name)
    parent_relationships: List[
        RelationshipProperty
    ] = parent_entity.get_relationship_property_names_and_properties().values()

    for relationship in parent_relationships:
        if (
            relationship.secondary is not None
            and relationship.argument == child_class_name
        ):
            return relationship.secondary

    raise LookupError(
        f"Association table with {child_class_name} and {parent_class_name} does not in exist in "
        f"{state_schema.__name__}"
    )


def schema_type_for_object(schema_object: DatabaseEntity) -> SchemaType:
    match schema_object:
        case JusticeCountsBase():
            return SchemaType.JUSTICE_COUNTS
        case StateBase():
            return SchemaType.STATE
        case OperationsBase():
            return SchemaType.OPERATIONS
        case CaseTriageBase():
            return SchemaType.CASE_TRIAGE
        case PathwaysBase():
            return SchemaType.PATHWAYS
        case OutliersBase():
            return SchemaType.OUTLIERS
        case WorkflowsBase():
            return SchemaType.WORKFLOWS
        case InsightsBase():
            return SchemaType.INSIGHTS

    raise ValueError(f"Object of type [{type(schema_object)}] has unknown schema base.")


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
        case SchemaType.OUTLIERS:
            return OutliersBase
        case SchemaType.WORKFLOWS:
            return WorkflowsBase
        case SchemaType.INSIGHTS:
            return InsightsBase

    raise ValueError(f"Unexpected schema type [{schema_type}].")
