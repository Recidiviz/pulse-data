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
"""Helpers for building source table collections for normalization pipeline outputs."""

from google.cloud.bigquery import SchemaField

from recidiviz.big_query.big_query_utils import schema_for_sqlalchemy_table
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.entity.normalized_entities_utils import (
    LEGACY_NORMALIZATION_ENTITY_CLASSES,
)
from recidiviz.pipelines.normalization.dataset_config import (
    normalized_state_dataset_for_state_code_legacy_normalization_output,
)
from recidiviz.pipelines.normalization.utils.entity_normalization_manager_utils import (
    NORMALIZATION_MANAGERS,
)
from recidiviz.pipelines.normalization.utils.normalized_entity_conversion_utils import (
    bq_schema_for_normalized_state_entity,
)
from recidiviz.pipelines.pipeline_names import NORMALIZATION_PIPELINE_NAME
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineSourceTableLabel,
    NormalizedStateSpecificEntitySourceTableLabel,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)


def build_normalization_pipeline_output_table_id_to_schemas() -> dict[
    str, list[SchemaField]
]:
    """Returns a map of table id to schema for all the tables that normalization
    pipelines actually hydrate.
    """
    table_id_to_schema_map = {}
    for entity_cls in LEGACY_NORMALIZATION_ENTITY_CLASSES:
        table_id = schema_utils.get_state_database_entity_with_name(
            entity_cls.base_class_name()
        ).__tablename__
        table_id_to_schema_map[table_id] = bq_schema_for_normalized_state_entity(
            entity_cls
        )

    for manager in NORMALIZATION_MANAGERS:
        for child_cls, parent_cls in manager.normalized_entity_associations():
            association_table = schema_utils.get_state_database_association_with_names(
                child_cls.__name__, parent_cls.__name__
            )
            table_id = association_table.name
            table_id_to_schema_map[table_id] = schema_for_sqlalchemy_table(
                association_table,
                add_state_code_field=True,
            )
    return table_id_to_schema_map


# TODO(#31741): Delete this collection once we have deleted the legacy normalization
#  pipeline.
def build_normalization_pipeline_output_source_table_collections() -> list[
    SourceTableCollection
]:
    """Builds the collection of source tables that are output by any normalization
    pipeline (i.e. `us_xx_normalized_state` datasets).
    """

    # Add definitions for state-specific normalization datasets
    state_specific_normalized_collections: list[SourceTableCollection] = [
        SourceTableCollection(
            labels=[
                DataflowPipelineSourceTableLabel(NORMALIZATION_PIPELINE_NAME),
                NormalizedStateSpecificEntitySourceTableLabel(state_code=state_code),
            ],
            update_config=SourceTableCollectionUpdateConfig.regenerable(),
            dataset_id=normalized_state_dataset_for_state_code_legacy_normalization_output(
                state_code=state_code
            ),
        )
        for state_code in get_direct_ingest_states_existing_in_env()
    ]

    for (
        table_id,
        schema_fields,
    ) in build_normalization_pipeline_output_table_id_to_schemas().items():
        for collection in state_specific_normalized_collections:
            collection.add_source_table(table_id=table_id, schema_fields=schema_fields)
    return state_specific_normalized_collections
