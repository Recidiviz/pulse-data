# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""A script for generating one LookML view for each state schema table"""
from google.cloud import bigquery

from recidiviz.ingest.views.dataset_config import STATE_BASE_DATASET
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.persistence.database.schema_utils import is_association_table
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.entity_metadata_helper import (
    AssociationTableMetadataHelper,
    EntityMetadataHelper,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.tools.looker.entity.entity_field_builders import (
    AssociationTableLookMLFieldBuilder,
    EntityLookMLFieldBuilder,
    LookMLFieldBuilder,
)
from recidiviz.tools.looker.state.custom_views.person_periods import (
    PersonPeriodsLookMLViewBuilder,
)
from recidiviz.tools.looker.state.state_dataset_custom_view_fields import (
    BigQuerySchemaValidator,
    StateEntityLookMLCustomFieldProvider,
)

ENTITIES_MODULE = state_entities


def _get_custom_views(
    schema_map: dict[str, list[bigquery.SchemaField]]
) -> list[LookMLView]:
    return [PersonPeriodsLookMLViewBuilder.from_schema(schema_map).build()]


def generate_state_views() -> list[LookMLView]:
    """Generates LookML views for all state entities and association tables."""
    schema_map = get_bq_schema_for_entities_module(ENTITIES_MODULE)
    field_provider = StateEntityLookMLCustomFieldProvider(
        field_validator=BigQuerySchemaValidator(schema_map)
    )

    def build_lookml_view(
        table_id: str, schema_fields: list[bigquery.SchemaField]
    ) -> LookMLView:
        """Constructs a LookML view for a given table."""
        if is_association_table(table_id):
            builder: LookMLFieldBuilder = AssociationTableLookMLFieldBuilder(
                metadata=AssociationTableMetadataHelper.for_table_id(
                    entities_module=ENTITIES_MODULE, table_id=table_id
                ),
                custom_field_provider=field_provider,
                table_id=table_id,
                schema_fields=schema_fields,
            )
        else:
            builder = EntityLookMLFieldBuilder(
                metadata=EntityMetadataHelper.for_table_id(
                    entities_module=ENTITIES_MODULE, table_id=table_id
                ),
                custom_field_provider=field_provider,
                table_id=table_id,
                schema_fields=schema_fields,
            )

        return LookMLView.for_big_query_table(
            dataset_id=STATE_BASE_DATASET,
            table_id=table_id,
            fields=builder.build_view_fields(),
        )

    views_derived_from_schema = [
        build_lookml_view(table_id, schema_fields)
        for table_id, schema_fields in schema_map.items()
    ]

    return views_derived_from_schema + _get_custom_views(schema_map)
