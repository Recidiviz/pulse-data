# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Builds entity LookML views"""
from types import ModuleType

import attr
from google.cloud import bigquery

from recidiviz.looker.lookml_field_registry import LookMLFieldRegistry
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.persistence.database.schema_utils import is_association_table
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.entity_metadata_helper import (
    AssociationTableMetadataHelper,
    EntityMetadataHelper,
)
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.tools.looker.entity.entity_custom_field_registry import (
    get_custom_field_registry_for_entity_module,
)
from recidiviz.tools.looker.entity.entity_custom_view_manager import (
    get_entity_custom_view_manager,
)
from recidiviz.tools.looker.entity.entity_field_builders import (
    AssociationTableLookMLFieldBuilder,
    EntityLookMLFieldBuilder,
    LookMLFieldBuilder,
)


def view_name_for_entity_table(entity_module: ModuleType, entity_table_id: str) -> str:
    """Returns the name of the view for the given entity table."""
    if entity_module == normalized_entities:
        return f"normalized_{entity_table_id}"
    return entity_table_id


@attr.define
class EntityLookMLViewsBuilder:
    """
    A builder class for constructing LookML views for BigQuery tables based on entity metadata and schema definitions.

    Attributes:
        dataset_id (str): The ID of the BigQuery dataset.
        entity_module (ModuleType): The module containing entity definitions.
        custom_field_registry (LookMLFieldRegistry): Registry to look up custom LookML fields
            (fields that don't map 1:1 a BQ schema field or require additional logic to build).
        schema_map (dict[str, list[bigquery.SchemaField]]): Mapping of table IDs to their schema fields.
    """

    dataset_id: str
    entity_module: ModuleType
    custom_field_registry: LookMLFieldRegistry
    schema_map: dict[str, list[bigquery.SchemaField]]

    def _build_lookml_view(self, table_id: str) -> LookMLView:
        """Constructs a LookML view for a given table."""
        schema_fields = self.schema_map[table_id]

        if is_association_table(table_id):
            builder: LookMLFieldBuilder = AssociationTableLookMLFieldBuilder(
                metadata=AssociationTableMetadataHelper.for_table_id(
                    entities_module=self.entity_module, table_id=table_id
                ),
                custom_field_registry=self.custom_field_registry,
                table_id=table_id,
                schema_fields=schema_fields,
            )
        else:
            builder = EntityLookMLFieldBuilder(
                metadata=EntityMetadataHelper.for_table_id(
                    entities_module=self.entity_module, table_id=table_id
                ),
                custom_field_registry=self.custom_field_registry,
                table_id=table_id,
                schema_fields=schema_fields,
            )

        return LookMLView.for_big_query_table(
            dataset_id=self.dataset_id,
            table_id=table_id,
            fields=builder.build_view_fields(),
            view_name=view_name_for_entity_table(self.entity_module, table_id),
        )

    def build_and_validate(self) -> list[LookMLView]:
        views_derived_from_schema = []
        for table_id, schema_fields in self.schema_map.items():
            view = self._build_lookml_view(table_id)
            view.validate_referenced_fields_exist_in_schema(schema_fields)
            views_derived_from_schema.append(view)

        return views_derived_from_schema


def generate_entity_lookml_views(
    dataset_id: str, entities_module: ModuleType
) -> list[LookMLView]:
    """Generates LookML views for all entities and association tables in the provided module."""
    schema_map = get_bq_schema_for_entities_module(entities_module)

    views_derived_from_schema = EntityLookMLViewsBuilder(
        dataset_id=dataset_id,
        entity_module=entities_module,
        custom_field_registry=get_custom_field_registry_for_entity_module(
            entities_module
        ),
        schema_map=schema_map,
    ).build_and_validate()

    return views_derived_from_schema + get_entity_custom_view_manager(
        dataset_id=dataset_id, entities_module=entities_module
    ).build_custom_views(schema_map=schema_map)
