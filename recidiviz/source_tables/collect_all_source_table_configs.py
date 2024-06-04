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
import glob
import os.path
from itertools import groupby

from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET
from recidiviz.ingest.direct.dataset_config import (
    raw_data_pruning_new_raw_data_dataset,
    raw_data_pruning_raw_data_diff_results_dataset,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportManager,
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import FILE_ID_COL_NAME
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.bq_refresh.big_query_table_manager import (
    bq_schema_for_sqlalchemy_table,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import get_all_table_classes_in_schema
from recidiviz.source_tables.dataflow_output_table_collector import (
    get_dataflow_output_source_table_collections,
)
from recidiviz.source_tables.source_table_config import (
    RawDataSourceTableLabel,
    SchemaTypeSourceTableLabel,
    SourceTableCollection,
    SourceTableConfig,
    SourceTableLabel,
)
from recidiviz.source_tables.source_table_repository import SourceTableRepository
from recidiviz.source_tables.us_mi_validation_oneoffs import (
    collect_duplicative_us_mi_validation_oneoffs,
)

ONE_DAY_MS = 24 * 60 * 60 * 1000


def _collect_raw_data_source_table_collections() -> list[SourceTableCollection]:
    """Collects datasets / source table definitions for all raw data configurations"""
    collections: list[SourceTableCollection] = []

    state_codes = get_direct_ingest_states_existing_in_env()
    for state_code in state_codes:
        for instance in DirectIngestInstance:
            region_config = get_region_raw_file_config(state_code.value)
            # For a given state and instance, create the raw datasets used for housing temporary tables related to
            # raw data pruning. The tables within the dataset will be temporarily added and deleted in the process of
            # raw data pruning, but the datasets themselves won't.
            labels: list[SourceTableLabel] = [
                RawDataSourceTableLabel(state_code=state_code, ingest_instance=instance)
            ]
            collections.extend(
                [
                    SourceTableCollection(
                        dataset_id=raw_data_pruning_new_raw_data_dataset(
                            state_code, instance
                        ),
                        labels=labels,
                        default_table_expiration_ms=ONE_DAY_MS,
                    ),
                    SourceTableCollection(
                        dataset_id=raw_data_pruning_raw_data_diff_results_dataset(
                            state_code, instance
                        ),
                        labels=labels,
                        default_table_expiration_ms=ONE_DAY_MS,
                    ),
                ]
            )

            raw_data_collection = SourceTableCollection(
                dataset_id=raw_tables_dataset_for_region(
                    state_code=state_code,
                    instance=instance,
                ),
                labels=labels,
            )

            collections.append(raw_data_collection)

            for raw_file_tag in region_config.raw_file_configs:
                raw_data_collection.add_source_table(
                    raw_file_tag,
                    description=f"Raw data file for {raw_file_tag}",
                    schema_fields=DirectIngestRawFileImportManager.create_raw_table_schema(
                        raw_file_config=region_config.raw_file_configs[raw_file_tag],
                    ),
                    clustering_fields=[FILE_ID_COL_NAME],
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
            labels=[SchemaTypeSourceTableLabel(export_config.schema_type)],
            dataset_id=export_config.multi_region_dataset(
                dataset_override_prefix=None,
            ),
        )
        results.append(collection)

        for table in export_config.get_tables_to_export():
            collection.add_source_table(
                table_id=table.name,
                description=f"Exported table for {table.name}",
                schema_fields=bq_schema_for_sqlalchemy_table(
                    export_config.schema_type, table
                ),
            )

    state_collection = SourceTableCollection(
        labels=[SchemaTypeSourceTableLabel(SchemaType.STATE)],
        dataset_id=STATE_BASE_DATASET,
    )
    results.append(state_collection)

    for table in list(get_all_table_classes_in_schema(SchemaType.STATE)):
        state_collection.add_source_table(
            table_id=table.name,
            description=f"State entity schema for {table.name}",
            schema_fields=bq_schema_for_sqlalchemy_table(SchemaType.STATE, table),
        )

    return results


def _collect_yaml_source_table_collections() -> list[SourceTableCollection]:
    yaml_paths = glob.glob(os.path.join(os.path.dirname(__file__), "schema/**/*.yaml"))

    source_tables_by_dataset = groupby(
        [SourceTableConfig.from_file(yaml_path) for yaml_path in yaml_paths],
        key=lambda source_table: source_table.address.dataset_id,
    )

    x = [
        SourceTableCollection(
            dataset_id=dataset_id,
            source_tables_by_address={
                source_table.address: source_table for source_table in source_tables
            },
        )
        for dataset_id, source_tables in source_tables_by_dataset
    ]

    return x


def build_source_table_repository_for_collected_schemata() -> SourceTableRepository:
    """Builds a source table repository for all source tables in our BigQuery graph"""
    return SourceTableRepository(
        source_table_collections=[
            *_collect_yaml_source_table_collections(),
            *_collect_raw_data_source_table_collections(),
            *_collect_cloudsql_mirror_source_table_collections(),
            *collect_duplicative_us_mi_validation_oneoffs(),
            *get_dataflow_output_source_table_collections(),
        ],
    )


if __name__ == "__main__":
    import pprint

    pprint.pprint(build_source_table_repository_for_collected_schemata())
