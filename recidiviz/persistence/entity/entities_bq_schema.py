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
from recidiviz.persistence.entity.entity_utils import (
    entities_have_direct_relationship,
    get_all_entity_classes_in_module,
    get_association_table_id,
    get_entity_class_in_module_with_name,
    is_many_to_many_relationship,
    is_many_to_one_relationship,
)
from recidiviz.persistence.entity.root_entity_utils import (
    get_root_entity_class_for_entity,
)

STATE_CODE_COL = "state_code"


@cache
def get_bq_schema_for_entities_module(
    entities_module: ModuleType,
) -> dict[str, list[SchemaField]]:
    """Derives a BQ dataset schema from a module containing a collection of related
    Entity classes.
    """
    table_to_schema = _get_association_table_to_schema_map(entities_module)
    for entity_cls in get_all_entity_classes_in_module(entities_module):
        table_to_schema[entity_cls.get_table_id()] = _get_bq_schema_for_entity_class(
            entities_module, entity_cls
        )
    return table_to_schema


def _get_bq_schema_for_entity_class(
    entities_module: ModuleType, entity_cls: Type[Entity]
) -> list[SchemaField]:
    """Derives a BQ table schema for the provided Entity class."""
    schema = []
    attr_class_reference = attribute_field_type_reference_for_class(entity_cls)
    # Sort fields by their declaration order so we produce schemas with
    # deterministic column orders.
    for field in attr_class_reference.sorted_fields:
        field_info = attr_class_reference.get_field_info(field)
        if not field_info.referenced_cls_name:
            # This is a flat field
            schema.append(schema_field_for_attribute(field, field_info.attribute))
            continue

        referenced_cls = get_entity_class_in_module_with_name(
            entities_module, field_info.referenced_cls_name
        )
        if referenced_cls != get_root_entity_class_for_entity(
            entity_cls
        ) and not is_many_to_one_relationship(entity_cls, referenced_cls):
            continue

        # The entity_cls has a foreign key reference to the referenced class on its
        # table.
        foreign_key_field_name = referenced_cls.get_class_id_name()
        schema.append(
            SchemaField(
                foreign_key_field_name,
                bigquery.enums.SqlTypeNames.INTEGER.value,
                mode="NULLABLE",
            )
        )

    return schema


def _get_association_table_to_schema_map(
    entities_module: ModuleType,
) -> dict[str, List[SchemaField]]:
    """For the given entities module, derives the set of association tables needed to
    encode many-to-many relationships and returns the schemas for those tables.
    """
    sorted_entities = sorted(
        get_all_entity_classes_in_module(entities_module),
        key=lambda entity_cls: entity_cls.get_table_id(),
    )
    association_table_to_schema = {}

    for i, entity_class_a in enumerate(sorted_entities):
        for entity_class_b in sorted_entities[i + 1 :]:
            if not entities_have_direct_relationship(entity_class_a, entity_class_b):
                continue

            if not is_many_to_many_relationship(entity_class_a, entity_class_b):
                continue

            association_table_id = get_association_table_id(
                entity_class_a, entity_class_b
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
