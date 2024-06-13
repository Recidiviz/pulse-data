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
from recidiviz.ingest.direct.dataset_config import (
    ingest_view_materialization_results_dataset,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.persistence.database.bq_refresh.big_query_table_manager import (
    bq_schema_for_sqlalchemy_table,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import get_all_table_classes_in_schema
from recidiviz.pipelines.ingest.dataset_config import state_dataset_for_state_code
from recidiviz.pipelines.ingest.state.constants import (
    INGEST_VIEW_RESULTS_SCHEMA_COLUMNS,
)
from recidiviz.pipelines.ingest.state.gating import (
    get_ingest_pipeline_enabled_state_and_instance_pairs,
)
from recidiviz.pipelines.pipeline_names import INGEST_PIPELINE_NAME
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineSourceTableLabel,
    IngestPipelineEntitySourceTableLabel,
    IngestViewOutputSourceTableLabel,
    SourceTableCollection,
)


def build_ingest_pipeline_output_source_table_collections() -> list[
    SourceTableCollection
]:
    """Builds the collection of source tables that are output by any ingest
    pipeline.

    # TODO(#30495): This function should eventually also return ingest_view datasets
    #  once we can build those schemas from ingest mappings YAMLs.
    """
    state_specific_collections: list[SourceTableCollection] = [
        SourceTableCollection(
            dataset_id=state_dataset_for_state_code(state_code, ingest_instance),
            labels=[
                DataflowPipelineSourceTableLabel(INGEST_PIPELINE_NAME),
                IngestPipelineEntitySourceTableLabel(
                    state_code=state_code, ingest_instance=ingest_instance
                ),
            ],
        )
        for state_code, ingest_instance in (
            get_ingest_pipeline_enabled_state_and_instance_pairs()
        )
    ]
    for table in list(get_all_table_classes_in_schema(SchemaType.STATE)):
        table_id = table.name
        schema_fields = bq_schema_for_sqlalchemy_table(SchemaType.STATE, table)
        for collection in state_specific_collections:
            collection.add_source_table(table_id=table_id, schema_fields=schema_fields)

    return state_specific_collections


def _get_ingest_view_builders(
    state_code: StateCode, ingest_instance: DirectIngestInstance
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
            ingest_instance=ingest_instance
        ),
    )

    return view_collector.collect_query_builders()


def _build_ingest_view_source_table_collections(
    state_instance_pairs: list[tuple[StateCode, DirectIngestInstance]]
) -> dict[str, SourceTableCollection]:
    dataset_to_source_table_collection: dict[str, SourceTableCollection] = {}

    for state_code, ingest_instance in state_instance_pairs:
        dataset_id = ingest_view_materialization_results_dataset(
            state_code, ingest_instance
        )
        source_table_collection = SourceTableCollection(
            dataset_id=dataset_id,
            labels=[
                DataflowPipelineSourceTableLabel(pipeline_name=INGEST_PIPELINE_NAME),
                IngestViewOutputSourceTableLabel(
                    state_code=state_code, ingest_instance=ingest_instance
                ),
            ],
        )
        dataset_to_source_table_collection[dataset_id] = source_table_collection

    return dataset_to_source_table_collection


# TODO(#30495): This is not included in get_dataflow_output_source_table_collections as
#  it requires many async calls to BQ. This should be removed once ingest mappings
#  include BigQuery column types.
def build_ingest_view_source_table_configs(
    bq_client: BigQueryClient,
    state_instance_pairs: list[tuple[StateCode, DirectIngestInstance]],
) -> list[SourceTableCollection]:
    """Builds SourceTableCollections for ingest views by submitting their queries to
    BigQuery and using the response schema to hydrate the SourceTableConfigs.
    """
    address_to_query: dict[BigQueryAddress, str] = {}
    dataset_to_source_table_collection = _build_ingest_view_source_table_collections(
        state_instance_pairs
    )

    for state_code, ingest_instance in state_instance_pairs:
        dataset_id = ingest_view_materialization_results_dataset(
            state_code, ingest_instance
        )
        ingest_view_builders = _get_ingest_view_builders(state_code, ingest_instance)

        for ingest_view_builder in ingest_view_builders:
            address = BigQueryAddress(
                dataset_id=dataset_id, table_id=ingest_view_builder.ingest_view_name
            )
            address_to_query[address] = ingest_view_builder.build_query(
                DirectIngestViewQueryBuilder.QueryStructureConfig(
                    raw_data_source_instance=ingest_instance,
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
