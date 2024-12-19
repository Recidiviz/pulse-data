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
"""Helpers for building source table collections for ingest pipeline outputs."""
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.state import entities, normalized_entities
from recidiviz.pipelines.ingest.dataset_config import (
    ingest_view_materialization_results_dataset,
    normalized_state_dataset_for_state_code,
    state_dataset_for_state_code,
)
from recidiviz.pipelines.ingest.state.constants import (
    INGEST_VIEW_RESULTS_SCHEMA_COLUMNS,
)
from recidiviz.pipelines.pipeline_names import INGEST_PIPELINE_NAME
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineSourceTableLabel,
    IngestViewResultsSourceTableLabel,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    StateSpecificSourceTableLabel,
)


def build_ingest_view_results_source_table_collection(
    state_code: StateCode,
) -> SourceTableCollection:
    """Build the source table collection for the `us_xx_ingest_view_results` ingest
    pipeline output dataset for a given state.
    """
    dataset_id = ingest_view_materialization_results_dataset(state_code)
    collection = SourceTableCollection(
        dataset_id=dataset_id,
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        labels=[
            IngestViewResultsSourceTableLabel(state_code=state_code),
            DataflowPipelineSourceTableLabel(pipeline_name=INGEST_PIPELINE_NAME),
            StateSpecificSourceTableLabel(state_code=state_code),
        ],
        description=(
            f"Contains materialized ingest view results produced by the ingest "
            f"pipeline for {state_code.value}."
        ),
    )

    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value
    )
    ingest_manifest_collector = IngestViewManifestCollector(
        region=region,
        delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
    )
    # We intentionally create a table for each ingest view with a defined manifest YAML,
    #  regardless of whether that ingest view is gated to produce results in this
    #  environment. This keeps the list of expected tables stable across local and
    #  deployed environments.
    for (
        ingest_view_name,
        manifest,
    ) in ingest_manifest_collector.ingest_view_to_manifest.items():
        schema = [
            bigquery.SchemaField(
                name=column_name, field_type=type_name, mode="NULLABLE"
            )
            for column_name, type_name in manifest.input_column_to_type.items()
        ] + INGEST_VIEW_RESULTS_SCHEMA_COLUMNS

        collection.add_source_table(table_id=ingest_view_name, schema_fields=schema)
    return collection


def build_state_output_source_table_collection(
    state_code: StateCode,
) -> SourceTableCollection:
    """Build the source table collection for the `us_xx_state` ingest pipeline output
    dataset for a given state.
    """
    collection = SourceTableCollection(
        dataset_id=state_dataset_for_state_code(state_code),
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        labels=[
            DataflowPipelineSourceTableLabel(INGEST_PIPELINE_NAME),
            StateSpecificSourceTableLabel(state_code=state_code),
        ],
        description=(
            f"Contains un-normalized versions of the entities produced by the ingest "
            f"pipeline for {state_code.value}."
        ),
    )
    for table_id, schema_fields in get_bq_schema_for_entities_module(entities).items():
        collection.add_source_table(table_id=table_id, schema_fields=schema_fields)
    return collection


def build_normalized_state_output_source_table_collection(
    state_code: StateCode,
) -> SourceTableCollection:
    """Build the source table collection for the `us_xx_normalized_state` ingest
    pipeline output dataset for a given state.
    """
    collection = SourceTableCollection(
        dataset_id=normalized_state_dataset_for_state_code(state_code),
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        labels=[
            DataflowPipelineSourceTableLabel(INGEST_PIPELINE_NAME),
            StateSpecificSourceTableLabel(state_code=state_code),
        ],
        description=(
            "Contains normalized versions of the entities in the state dataset "
            f"produced by the ingest pipeline for {state_code.value}."
        ),
    )
    for table_id, schema_fields in get_bq_schema_for_entities_module(
        normalized_entities
    ).items():
        collection.add_source_table(table_id=table_id, schema_fields=schema_fields)
    return collection


def _build_ingest_view_results_output_source_table_collections() -> list[
    SourceTableCollection
]:
    return [
        build_ingest_view_results_source_table_collection(state_code)
        for state_code in get_direct_ingest_states_existing_in_env()
    ]


def _build_state_output_source_table_collections() -> list[SourceTableCollection]:
    return [
        build_state_output_source_table_collection(state_code)
        for state_code in get_direct_ingest_states_existing_in_env()
    ]


def _build_normalized_state_output_source_table_collections() -> list[
    SourceTableCollection
]:
    return [
        build_normalized_state_output_source_table_collection(state_code)
        for state_code in get_direct_ingest_states_existing_in_env()
    ]


def build_ingest_pipeline_output_source_table_collections() -> list[
    SourceTableCollection
]:
    """Builds the collection of source tables that are output by any ingest
    pipeline.
    """
    return [
        *_build_ingest_view_results_output_source_table_collections(),
        *_build_state_output_source_table_collections(),
        *_build_normalized_state_output_source_table_collections(),
    ]
