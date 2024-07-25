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
"""Helpers for building source table collections for the outputs of processes that union
together a set of state-specific datasets.
"""
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.persistence.database.bq_refresh.big_query_table_manager import (
    bq_schema_for_sqlalchemy_table,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import get_all_table_classes_in_schema
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.source_tables.normalization_pipeline_output_table_collector import (
    build_normalization_pipeline_output_table_id_to_schemas,
)
from recidiviz.source_tables.source_table_config import (
    NormalizedStateAgnosticEntitySourceTableLabel,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    UnionedStateAgnosticSourceTableLabel,
)


def build_unioned_state_source_table_collection() -> SourceTableCollection:
    """Builds the source table collections for the outputs of the update_state Airflow
    step.
    """
    state_agnostic_collection = SourceTableCollection(
        dataset_id=STATE_BASE_DATASET,
        labels=[
            UnionedStateAgnosticSourceTableLabel(STATE_BASE_DATASET),
        ],
    )
    for table_id, schema_fields in get_bq_schema_for_entities_module(
        state_entities
    ).items():
        state_agnostic_collection.add_source_table(
            table_id=table_id, schema_fields=schema_fields
        )

    return state_agnostic_collection


def build_unioned_normalized_state_source_table_collections() -> list[
    SourceTableCollection
]:
    """Builds the source table collections for the outputs of the
    update_normalized_state Airflow step.
    """
    state_agnostic_normalized_collection = SourceTableCollection(
        labels=[
            UnionedStateAgnosticSourceTableLabel(NORMALIZED_STATE_DATASET),
            NormalizedStateAgnosticEntitySourceTableLabel(
                source_is_normalization_pipeline=True
            ),
        ],
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        dataset_id=NORMALIZED_STATE_DATASET,
    )
    for (
        table_id,
        schema_fields,
    ) in build_normalization_pipeline_output_table_id_to_schemas().items():
        state_agnostic_normalized_collection.add_source_table(
            table_id=table_id, schema_fields=schema_fields
        )

    # These tables contain entities that are not output by the normalization pipelines,
    # so their data is pulled directly from non-normalized `state` tables.
    state_agnostic_non_normalized_collection = SourceTableCollection(
        labels=[
            NormalizedStateAgnosticEntitySourceTableLabel(
                source_is_normalization_pipeline=False
            ),
        ],
        dataset_id=NORMALIZED_STATE_DATASET,
    )
    # Add definitions for tables not hydrated by the normalization pipeline
    for table in get_all_table_classes_in_schema(SchemaType.STATE):
        if not state_agnostic_normalized_collection.has_table(table.name):
            state_agnostic_non_normalized_collection.add_source_table(
                table_id=table.name,
                schema_fields=bq_schema_for_sqlalchemy_table(SchemaType.STATE, table),
            )

    return [
        state_agnostic_normalized_collection,
        state_agnostic_non_normalized_collection,
    ]
