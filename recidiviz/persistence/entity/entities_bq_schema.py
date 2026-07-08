# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Helpers for deriving a BQ dataset schema from a module containing a collection of
related Entity classes.
"""
from functools import cache
from types import ModuleType
from typing import List, Type

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

from recidiviz.big_query.big_query_utils import schema_field_for_attribute
from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context import EntitiesModuleContext
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import (
    get_all_entity_classes_in_module,
    get_all_many_to_many_relationships_in_module,
    get_association_table_id,
)
from recidiviz.persistence.entity.root_entity_utils import (
    entity_class_for_foreign_key_column,
)

STATE_CODE_COL = "state_code"


@cache
def get_bq_schema_for_entities_module(
    entities_module: ModuleType,
) -> dict[str, list[SchemaField]]:
    """Derives a BQ dataset schema from a module containing a collection of related
    Entity classes.
    """
    entities_module_context = entities_module_context_for_module(entities_module)
    table_to_schema = _get_association_table_to_schema_map(entities_module)
    for entity_cls in get_all_entity_classes_in_module(entities_module):
        table_to_schema[entity_cls.get_table_id()] = _get_bq_schema_for_entity_class(
            entities_module_context, entity_cls
        )
    return table_to_schema


def get_bq_schema_for_entity_table(
    entities_module: ModuleType, table_id: str
) -> list[SchemaField]:
    """Returns the schema for a table in a schema derived from a module containing a
    collection of related Entity classes.
    """
    return get_bq_schema_for_entities_module(entities_module)[table_id]


def _get_bq_schema_for_entity_class(
    entities_module_context: EntitiesModuleContext,
    entity_cls: Type[Entity],
) -> list[SchemaField]:
    """Derives a BQ table schema for the provided Entity class."""
    schema = []
    foreign_key_column_names: set[str] = set()
    attr_class_reference = attribute_field_type_reference_for_class(entity_cls)

    # Sort fields by their declaration order so we produce schemas with
    # deterministic column orders.
    for field in attr_class_reference.sorted_fields:
        field_info = attr_class_reference.get_field_info(field)
        if not field_info.referenced_cls_name:
            # This is a flat field
            schema.append(
                schema_field_for_attribute(
                    field,
                    field_info.attribute,
                    entities_module_context.field_description(entity_cls, field),
                )
            )
            continue

        foreign_key_entity_cls = entity_class_for_foreign_key_column(
            entities_module_context=entities_module_context,
            entity_cls=entity_cls,
            field_name=field,
        )
        if foreign_key_entity_cls is None:
            continue

        # The entity_cls table carries the referenced entity's id in a foreign
        # key column whose name and type match that entity's primary-key field.
        foreign_key_field_name = foreign_key_entity_cls.get_class_id_name()
        foreign_key_column = schema_field_for_attribute(
            foreign_key_field_name,
            attribute_field_type_reference_for_class(foreign_key_entity_cls)
            .get_field_info(foreign_key_field_name)
            .attribute,
            description=(
                f"Foreign key reference to {foreign_key_entity_cls.get_table_id()}"
            ),
        )

        # No schema today has two fields that produce the same foreign key
        # column; if one ever does, fail so the collision is resolved
        # explicitly rather than silently merged.
        if foreign_key_field_name in foreign_key_column_names:
            raise ValueError(
                f"Entity [{entity_cls.__name__}] has multiple relationship "
                f"fields that produce a foreign key column named "
                f"[{foreign_key_field_name}]."
            )
        foreign_key_column_names.add(foreign_key_field_name)
        schema.append(foreign_key_column)

    return schema


def _get_association_table_to_schema_map(
    entities_module: ModuleType,
) -> dict[str, List[SchemaField]]:
    """For the given entities module, derives the set of association tables needed to
    encode many-to-many relationships and returns the schemas for those tables.
    """
    association_table_to_schema = {}

    entities_module_context = entities_module_context_for_module(entities_module)
    associated_entites = get_all_many_to_many_relationships_in_module(entities_module)
    for entity_class_a, entity_class_b in associated_entites:
        association_table_id = get_association_table_id(
            entity_class_a, entity_class_b, entities_module_context
        )
        schema = [
            SchemaField(
                STATE_CODE_COL,
                bigquery.enums.SqlTypeNames.STRING.value,
                mode="NULLABLE",
            ),
            SchemaField(
                entity_class_a.get_class_id_name(),
                bigquery.enums.SqlTypeNames.INTEGER.value,
                mode="NULLABLE",
            ),
            SchemaField(
                entity_class_b.get_class_id_name(),
                bigquery.enums.SqlTypeNames.INTEGER.value,
                mode="NULLABLE",
            ),
        ]
        association_table_to_schema[association_table_id] = schema

    return association_table_to_schema
