# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Helpers for building source table collections for activity ingest pipeline
outputs.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.activity_ingest_view_manifest_compiler_delegate import (
    ActivityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.persistence.entity.activity import entities, normalized_entities
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.pipelines.ingest.activity.dataset_config import (
    ingest_view_materialization_results_dataset,
    normalized_state_dataset_for_state_code,
    state_dataset_for_state_code,
)
from recidiviz.pipelines.pipeline_names import INGEST_PIPELINE_NAME
from recidiviz.source_tables.ingest_pipeline_output_helpers import (
    build_ingest_view_results_source_table_collection_from_manifests,
)
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
    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value
    )
    return build_ingest_view_results_source_table_collection_from_manifests(
        dataset_id=ingest_view_materialization_results_dataset(state_code),
        description=(
            f"Contains materialized ingest view results produced by the ingest "
            f"pipeline for {state_code.value}."
        ),
        labels=[
            IngestViewResultsSourceTableLabel(state_code=state_code),
            DataflowPipelineSourceTableLabel(pipeline_name=INGEST_PIPELINE_NAME),
            StateSpecificSourceTableLabel(state_code=state_code),
        ],
        manifest_collector=IngestViewManifestCollector(
            region=region,
            delegate=ActivityIngestViewManifestCompilerDelegate(region=region),
            ingest_pipeline_type=IngestPipelineType.ACTIVITY,
        ),
    )


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


def build_activity_pipeline_output_source_table_collections() -> list[
    SourceTableCollection
]:
    """Builds the collection of source tables that are output by the activity
    ingest pipeline.
    """
    return [
        *_build_ingest_view_results_output_source_table_collections(),
        *_build_state_output_source_table_collections(),
        *_build_normalized_state_output_source_table_collections(),
    ]
