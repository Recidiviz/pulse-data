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
"""Contains utilities to collect and build source tables"""

from functools import cache
from types import ModuleType

from recidiviz.big_query.big_query_utils import schema_for_sqlalchemy_table
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    collect_all_extractor_configs_by_state,
)
from recidiviz.ingest.direct.dataset_config import (
    raw_data_pruning_new_raw_data_dataset,
    raw_data_pruning_raw_data_diff_results_dataset,
    raw_data_temp_load_dataset,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import FILE_ID_COL_NAME
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.source_tables.dataflow_output_table_collector import (
    get_dataflow_output_source_table_collections,
)
from recidiviz.source_tables.document_store_source_table_collection import (
    collect_document_store_source_tables,
)
from recidiviz.source_tables.externally_managed.collect_externally_managed_source_table_configs import (
    collect_externally_managed_source_table_collections,
)
from recidiviz.source_tables.extraction_results_source_table_collection import (
    collect_extraction_results_source_table_collections,
    collect_golden_eval_results_source_table_collection,
)
from recidiviz.source_tables.identity_pipeline_input_table_collector import (
    build_identity_pipeline_input_source_table_collections,
)
from recidiviz.source_tables.identity_service_export_source_tables import (
    build_identity_service_export_source_table_collection,
)
from recidiviz.source_tables.intercom_export_source_tables import (
    build_intercom_export_metadata_source_tables,
)
from recidiviz.source_tables.sentencing_source_table_collection import (
    collect_sentencing_source_tables,
)
from recidiviz.source_tables.source_table_config import (
    CALC_UPDATE_GROUPS,
    RAW_DATA_PRUNING_UPDATE_GROUPS,
    RAW_DATA_UPDATE_GROUPS,
    RawDataSourceTableLabel,
    SchemaTypeSourceTableLabel,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableLabel,
    StateSpecificSourceTableLabel,
)
from recidiviz.source_tables.us_mi_validation_oneoffs import (
    collect_duplicative_us_mi_validation_oneoffs,
)
from recidiviz.source_tables.yaml_managed.collect_yaml_managed_source_table_configs import (
    collect_yaml_managed_source_table_collections,
)
from recidiviz.utils import metadata

ONE_DAY_MS = 24 * 60 * 60 * 1000


def build_raw_data_source_table_collections_for_state_and_instance(
    state_code: StateCode,
    instance: DirectIngestInstance,
    region_module_override: ModuleType | None,
) -> list[SourceTableCollection]:
    """Collects datasets / source table definitions for all raw data configurations for
    a given state and raw data instance.
    """
    region_config = get_region_raw_file_config(state_code.value, region_module_override)
    # For a given state and instance, create the raw datasets used for housing temporary tables related to
    # raw data pruning. The tables within the dataset will be temporarily added and deleted in the process of
    # raw data pruning, but the datasets themselves won't.
    labels: list[SourceTableLabel] = [
        RawDataSourceTableLabel(state_code=state_code, ingest_instance=instance),
        StateSpecificSourceTableLabel(state_code=state_code),
    ]
    collections = [
        SourceTableCollection(
            update_groups=RAW_DATA_PRUNING_UPDATE_GROUPS,
            dataset_id=raw_data_pruning_new_raw_data_dataset(state_code, instance),
            labels=labels,
            update_config=SourceTableCollectionUpdateConfig.protected(),
            default_table_expiration_ms=ONE_DAY_MS,
            description=(
                "Contains intermediate results of the raw data import process that "
                "will be queried as part of the automatic raw data pruning process."
            ),
        ),
        SourceTableCollection(
            update_groups=RAW_DATA_PRUNING_UPDATE_GROUPS,
            dataset_id=raw_data_pruning_raw_data_diff_results_dataset(
                state_code, instance
            ),
            labels=labels,
            update_config=SourceTableCollectionUpdateConfig.protected(),
            default_table_expiration_ms=ONE_DAY_MS,
            description=(
                "Contains intermediate results of the raw data import process that "
                "will be queried as part of the automatic raw data pruning process."
            ),
        ),
        SourceTableCollection(
            update_groups=RAW_DATA_PRUNING_UPDATE_GROUPS,
            dataset_id=raw_data_temp_load_dataset(state_code, instance),
            labels=labels,
            update_config=SourceTableCollectionUpdateConfig.protected(),
            # TODO(#30687) consider raising this if we think that there are
            # certain temp tables we would want to keep around for longer
            # by default (i.e. those that have a pre-import validation
            # failure we'd like to be able to inspect)
            default_table_expiration_ms=ONE_DAY_MS,
            description=(
                "Contains intermediate results of the raw data import process that "
                "will be queried during the raw data import DAG."
            ),
        ),
    ]

    raw_data_collection = SourceTableCollection(
        update_groups=RAW_DATA_UPDATE_GROUPS,
        dataset_id=raw_tables_dataset_for_region(
            state_code=state_code,
            instance=instance,
        ),
        # Changes to raw data source tables must be manually executed by implementation
        # engineers
        update_config=SourceTableCollectionUpdateConfig.protected(),
        labels=labels,
        description=f"Raw data tables from {StateCode.get_state(state_code)}",
    )

    collections.append(raw_data_collection)

    for raw_file_tag in region_config.raw_file_configs:
        raw_data_collection.add_source_table(
            raw_file_tag,
            description=f"Raw data file for {raw_file_tag}",
            schema_fields=RawDataTableBigQuerySchemaBuilder.build_bq_schema_for_config(
                raw_file_config=region_config.raw_file_configs[raw_file_tag],
            ),
            clustering_fields=[FILE_ID_COL_NAME],
        )
    return collections


def collect_raw_data_source_table_collections() -> list[SourceTableCollection]:
    """Collects datasets / source table definitions for all raw data configurations"""
    collections: list[SourceTableCollection] = []

    state_codes = get_direct_ingest_states_existing_in_env()
    for state_code in state_codes:
        for instance in DirectIngestInstance:
            collections.extend(
                build_raw_data_source_table_collections_for_state_and_instance(
                    state_code, instance, region_module_override=None
                )
            )
    return collections


def _collect_cloudsql_mirror_source_table_collections() -> list[SourceTableCollection]:
    """Update all schemas for bq_refresh datasets in a parallelized way."""
    results: list[SourceTableCollection] = []
    export_configs = [
        CloudSqlToBQConfig.for_schema_type(s)
        for s in SchemaType
        if CloudSqlToBQConfig.is_valid_schema_type(s)
    ]

    for export_config in export_configs:
        collection = SourceTableCollection(
            update_groups=CALC_UPDATE_GROUPS,
            labels=[SchemaTypeSourceTableLabel(export_config.schema_type)],
            update_config=SourceTableCollectionUpdateConfig.regenerable(),
            dataset_id=export_config.multi_region_dataset(
                dataset_override_prefix=None,
            ),
            description=export_config.multi_region_dataset_description(),
        )
        results.append(collection)

        for table in export_config.get_tables_to_export():
            collection.add_source_table(
                table_id=table.name,
                description=f"Exported table for {table.name}",
                schema_fields=schema_for_sqlalchemy_table(table),
            )

    return results


@cache
def collect_source_table_collections_hydrated_outside_view_graphs(
    project_id: str,
) -> list[SourceTableCollection]:
    """Returns the source table collections for tables whose contents are hydrated by
    processes outside our view graphs (e.g. ingest, Dataflow pipelines, external
    integrations).
    """
    if project_id is not None and project_id != metadata.project_id():
        raise ValueError(
            f"Expected project_id [{project_id}] to match metadata.project_id() "
            f"[{metadata.project_id()}]"
        )

    extractor_configs_by_state = collect_all_extractor_configs_by_state()

    return [
        *collect_externally_managed_source_table_collections(project_id=project_id),
        *collect_yaml_managed_source_table_collections(project_id=project_id),
        *collect_raw_data_source_table_collections(),
        *_collect_cloudsql_mirror_source_table_collections(),
        *collect_duplicative_us_mi_validation_oneoffs(),
        *get_dataflow_output_source_table_collections(),
        *build_identity_pipeline_input_source_table_collections(),
        build_identity_service_export_source_table_collection(),
        *collect_sentencing_source_tables(),
        *collect_document_store_source_tables(),
        *collect_extraction_results_source_table_collections(
            configs=extractor_configs_by_state
        ),
        collect_golden_eval_results_source_table_collection(),
        build_intercom_export_metadata_source_tables(),
    ]
