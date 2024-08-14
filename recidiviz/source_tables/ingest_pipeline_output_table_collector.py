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
import datetime
from concurrent import futures

from google.cloud.bigquery.table import RowIterator

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BQ_CLIENT_MAX_POOL_SIZE, BigQueryClient
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContextImpl,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.state import entities, normalized_entities
from recidiviz.pipelines.ingest.dataset_config import (
    ingest_view_materialization_results_dataset,
    state_dataset_for_state_code,
)
from recidiviz.pipelines.ingest.state.constants import (
    INGEST_VIEW_RESULTS_SCHEMA_COLUMNS,
)
from recidiviz.pipelines.normalization.dataset_config import (
    normalized_state_dataset_for_state_code_ingest_pipeline_output,
)
from recidiviz.pipelines.pipeline_names import INGEST_PIPELINE_NAME
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineSourceTableLabel,
    IngestPipelineEntitySourceTableLabel,
    IngestViewOutputSourceTableLabel,
    NormalizedStateSpecificEntitySourceTableLabel,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)
from recidiviz.utils import metadata


def build_state_output_source_table_collection(
    state_code: StateCode,
) -> SourceTableCollection:
    """Build the source table collection for the `us_xx_state` ingest pipeline output
    dataset for a given state.
    """
    collection = SourceTableCollection(
        dataset_id=state_dataset_for_state_code(state_code),
        labels=[
            DataflowPipelineSourceTableLabel(INGEST_PIPELINE_NAME),
            IngestPipelineEntitySourceTableLabel(state_code=state_code),
        ],
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
        dataset_id=normalized_state_dataset_for_state_code_ingest_pipeline_output(
            state_code
        ),
        labels=[
            DataflowPipelineSourceTableLabel(INGEST_PIPELINE_NAME),
            NormalizedStateSpecificEntitySourceTableLabel(state_code=state_code),
        ],
    )
    for table_id, schema_fields in get_bq_schema_for_entities_module(
        normalized_entities
    ).items():
        collection.add_source_table(table_id=table_id, schema_fields=schema_fields)
    return collection


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
        # TODO(#30495): This function should eventually also return ingest_view datasets
        #  once we can build those schemas from ingest mappings YAMLs.
        *_build_state_output_source_table_collections(),
        *_build_normalized_state_output_source_table_collections(),
    ]


def _get_ingest_view_builders(
    state_code: StateCode,
) -> list[DirectIngestViewQueryBuilder]:
    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value
    )
    ingest_manifest_collector = IngestViewManifestCollector(
        region=region,
        delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
    )
    view_collector = DirectIngestViewQueryBuilderCollector(
        region,
        ingest_manifest_collector.launchable_ingest_views(
            # We collect views that will be present in the project this is running for
            IngestViewContentsContextImpl.build_for_project(
                project_id=metadata.project_id()
            ),
        ),
    )

    return view_collector.get_query_builders()


def _build_ingest_view_source_table_collections(
    state_codes: list[StateCode],
) -> dict[str, SourceTableCollection]:
    dataset_to_source_table_collection: dict[str, SourceTableCollection] = {}

    for state_code in state_codes:
        dataset_id = ingest_view_materialization_results_dataset(state_code)
        source_table_collection = SourceTableCollection(
            dataset_id=dataset_id,
            update_config=SourceTableCollectionUpdateConfig.regenerable(),
            labels=[
                DataflowPipelineSourceTableLabel(pipeline_name=INGEST_PIPELINE_NAME),
                IngestViewOutputSourceTableLabel(state_code=state_code),
            ],
        )
        dataset_to_source_table_collection[dataset_id] = source_table_collection

    return dataset_to_source_table_collection


# TODO(#30495): This is not included in get_dataflow_output_source_table_collections as
#  it requires many async calls to BQ. This should be removed once ingest mappings
#  include BigQuery column types.
def build_ingest_view_source_table_configs(
    bq_client: BigQueryClient,
    state_codes: list[StateCode],
) -> list[SourceTableCollection]:
    """Builds SourceTableCollections for ingest views by submitting their queries to
    BigQuery and using the response schema to hydrate the SourceTableConfigs.
    """
    address_to_query: dict[BigQueryAddress, str] = {}
    dataset_to_source_table_collection = _build_ingest_view_source_table_collections(
        state_codes
    )

    for state_code in state_codes:
        dataset_id = ingest_view_materialization_results_dataset(state_code)
        ingest_view_builders = _get_ingest_view_builders(state_code)

        for ingest_view_builder in ingest_view_builders:
            address = BigQueryAddress(
                dataset_id=dataset_id, table_id=ingest_view_builder.ingest_view_name
            )
            address_to_query[address] = ingest_view_builder.build_query(
                DirectIngestViewQueryBuilder.QueryStructureConfig(
                    # We default to PRIMARY here because we want to build the source
                    # table definitions for our standard, deployed ingest view results
                    # source tables.
                    raw_data_source_instance=DirectIngestInstance.PRIMARY,
                    raw_data_datetime_upper_bound=datetime.datetime.now(),
                    limit_zero=True,
                )
            )

    def _run_query_to_fetch_schema(query_str: str) -> RowIterator:
        return bq_client.run_query_async(
            query_str=query_str,
            use_query_cache=False,
        ).result()

    with futures.ThreadPoolExecutor(
        max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
    ) as executor:
        futures_to_address = {
            executor.submit(_run_query_to_fetch_schema, query_str=query_str): address
            for address, query_str in address_to_query.items()
        }

        for job in futures.as_completed(futures_to_address):
            address = futures_to_address[job]

            result = job.result()
            source_table_collection = dataset_to_source_table_collection[
                address.dataset_id
            ]
            source_table_collection.add_source_table(
                table_id=address.table_id,
                description=f"Ingest view {address.table_id}",
                schema_fields=result.schema + INGEST_VIEW_RESULTS_SCHEMA_COLUMNS,
            )

    return list(dataset_to_source_table_collection.values())
