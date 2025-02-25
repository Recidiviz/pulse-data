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
from functools import cache
from itertools import groupby
from types import ModuleType

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.dataset_config import (
    AUTH0_EVENTS,
    AUTH0_PROD_ACTION_LOGS,
    EXPORT_ARCHIVES_DATASET,
    PULSE_DASHBOARD_SEGMENT_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    raw_data_pruning_new_raw_data_dataset,
    raw_data_pruning_raw_data_diff_results_dataset,
    raw_data_temp_load_dataset,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.raw_data_table_schema_utils import (
    RawDataTableBigQuerySchemaBuilder,
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
from recidiviz.source_tables.dataflow_output_table_collector import (
    get_dataflow_output_source_table_collections,
)
from recidiviz.source_tables.externally_managed.datasets import (
    EXTERNALLY_MANAGED_DATASETS_TO_DESCRIPTIONS,
)
from recidiviz.source_tables.sentencing_source_table_collection import (
    collect_sentencing_source_tables,
)
from recidiviz.source_tables.source_table_config import (
    RawDataSourceTableLabel,
    SchemaTypeSourceTableLabel,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableCollectionValidationConfig,
    SourceTableConfig,
    SourceTableLabel,
    StateSpecificSourceTableLabel,
)
from recidiviz.source_tables.source_table_repository import SourceTableRepository
from recidiviz.source_tables.us_mi_validation_oneoffs import (
    collect_duplicative_us_mi_validation_oneoffs,
)
from recidiviz.utils import environment, metadata
from recidiviz.utils.environment import GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override

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
            dataset_id=raw_data_pruning_new_raw_data_dataset(state_code, instance),
            labels=labels,
            default_table_expiration_ms=ONE_DAY_MS,
            description=(
                "Contains intermediate results of the raw data import process that "
                "will be queried as part of the automatic raw data pruning process."
            ),
        ),
        SourceTableCollection(
            dataset_id=raw_data_pruning_raw_data_diff_results_dataset(
                state_code, instance
            ),
            labels=labels,
            default_table_expiration_ms=ONE_DAY_MS,
            description=(
                "Contains intermediate results of the raw data import process that "
                "will be queried as part of the automatic raw data pruning process."
            ),
        ),
        SourceTableCollection(
            dataset_id=raw_data_temp_load_dataset(state_code, instance),
            labels=labels,
            # TODO(#30687) consider raising this if we think that there are
            # certain temp tables we would want to keep around for longer
            # by default (i.e. those that have a import-blocking validation)
            # failure we'd like to be able to inspect
            default_table_expiration_ms=ONE_DAY_MS,
            description=(
                "Contains intermediate results of the raw data import process that "
                "will be queried as part of the automatic raw data pruning process."
            ),
        ),
    ]

    raw_data_collection = SourceTableCollection(
        dataset_id=raw_tables_dataset_for_region(
            state_code=state_code,
            instance=instance,
        ),
        # Changes to raw data source tables must be manually executed by implementation engineers
        update_config=SourceTableCollectionUpdateConfig.static(),
        labels=labels,
        description=f"Raw data tables from {StateCode.get_state(state_code)}",
    )

    collections.append(raw_data_collection)

    for raw_file_tag in region_config.raw_file_configs:
        raw_data_collection.add_source_table(
            raw_file_tag,
            description=f"Raw data file for {raw_file_tag}",
            schema_fields=RawDataTableBigQuerySchemaBuilder.build_bq_schmea_for_config(
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
                schema_fields=bq_schema_for_sqlalchemy_table(
                    export_config.schema_type, table
                ),
            )

    return results


def collect_externally_managed_source_table_collections(
    project_id: str | None,
) -> list[SourceTableCollection]:
    """
    Collects all externally managed source tables.
    We declare datasets here where we are only interested in validating a subset of
    fields.

    If project_id is None, returns all source tables that exist in any project.
    Otherwise, only returns the collections that are deployed to the given project.
    """
    yaml_paths = glob.glob(
        os.path.join(os.path.dirname(__file__), "externally_managed/**/*.yaml")
    )

    def _source_table_sorter(source_table: SourceTableConfig) -> str:
        return source_table.address.dataset_id

    source_tables_by_dataset = groupby(
        sorted(
            [SourceTableConfig.from_file(yaml_path) for yaml_path in yaml_paths],
            key=_source_table_sorter,
        ),
        key=_source_table_sorter,
    )

    # "required" columns here means they are required by the view graph and should be
    # validated that the fields exist in BigQuery, not the column mode (REQUIRED vs NULLABLE)
    datasets_to_validation_config = {
        AUTH0_EVENTS: SourceTableCollectionValidationConfig(
            only_check_required_columns=True,
        ),
        AUTH0_PROD_ACTION_LOGS: SourceTableCollectionValidationConfig(
            only_check_required_columns=True,
        ),
        PULSE_DASHBOARD_SEGMENT_DATASET: SourceTableCollectionValidationConfig(
            only_check_required_columns=True,
        ),
        EXPORT_ARCHIVES_DATASET: SourceTableCollectionValidationConfig(
            only_check_required_columns=True,
        ),
    }

    return [
        SourceTableCollection(
            dataset_id=dataset_id,
            update_config=SourceTableCollectionUpdateConfig.unmanaged(),
            validation_config=datasets_to_validation_config.get(dataset_id, None),
            source_tables_by_address={
                source_table.address: source_table
                for source_table in source_tables
                # Filter project-specific source tables
                if (not project_id or not source_table.deployed_projects)
                or (project_id in source_table.deployed_projects)
            },
            description=EXTERNALLY_MANAGED_DATASETS_TO_DESCRIPTIONS[dataset_id],
        )
        for dataset_id, source_tables in source_tables_by_dataset
    ]


@cache
def build_source_table_repository_for_collected_schemata(
    # We require project_id as an argument so that we don't return incorrect cached
    # results when metadata.project_id() changes (e.g. in tests).
    project_id: str,
) -> SourceTableRepository:
    """Builds a source table repository for all source tables in a project's BigQuery graph
    If the project is unspecified, source tables for all projects are collected.
    """
    if project_id != metadata.project_id():
        raise ValueError(
            f"Expected project_id [{project_id}] to match metadata.project_id() "
            f"[{metadata.project_id()}]"
        )

    return SourceTableRepository(
        source_table_collections=[
            *collect_externally_managed_source_table_collections(
                project_id=metadata.project_id()
            ),
            *collect_raw_data_source_table_collections(),
            *_collect_cloudsql_mirror_source_table_collections(),
            *collect_duplicative_us_mi_validation_oneoffs(),
            *get_dataflow_output_source_table_collections(),
            *collect_sentencing_source_tables(),
        ],
    )


@cache
def get_source_table_datasets_to_descriptions(
    # We require project_id as an argument so that we don't return incorrect cached
    # results when metadata.project_id() changes (e.g. in tests).
    project_id: str,
) -> dict[str, str]:
    datasets_to_descriptions: dict[str, str] = {}
    for c in build_source_table_repository_for_collected_schemata(
        project_id
    ).source_table_collections:
        if (
            c.dataset_id in datasets_to_descriptions
            and c.description != datasets_to_descriptions[c.dataset_id]
        ):
            raise ValueError(
                f"Found description for dataset {c.dataset_id} [{c.description}] which "
                f"has conflicting versions across source table configurations. "
                f"Conflicting description: [{datasets_to_descriptions[c.dataset_id]}]"
            )

        datasets_to_descriptions[c.dataset_id] = c.description
    return datasets_to_descriptions


@cache
def get_source_table_addresses(
    # We require project_id as an argument so that we don't return incorrect cached
    # results when metadata.project_id() changes (e.g. in tests).
    project_id: str,
) -> set[BigQueryAddress]:
    """Returns the addresses of all the source tables deployed to the given project."""
    return set(
        build_source_table_repository_for_collected_schemata(
            project_id
        ).source_tables.keys()
    )


@environment.local_only
@cache
def get_all_source_table_addresses() -> set[BigQueryAddress]:
    """Returns the addresses of all the source tables deployed across any GCP
    project.
    """
    all_addresses = set()
    for project_id in GCP_PROJECTS:
        with local_project_id_override(project_id):
            all_addresses |= get_source_table_addresses(project_id)
    return all_addresses


@cache
def get_source_table_datasets(
    # We require project_id as an argument so that we don't return incorrect cached
    # results when metadata.project_id() changes (e.g. in tests).
    project_id: str,
) -> set[str]:
    """Returns the dataset ids of all the source tables deployed to the given
    project.
    """
    return set(a.dataset_id for a in get_source_table_addresses(project_id))


@environment.local_only
@cache
def get_all_source_table_datasets() -> set[str]:
    """Returns the dataset ids of all the source tables deployed across any GCP
    project.
    """
    return set(a.dataset_id for a in get_all_source_table_addresses())


if __name__ == "__main__":
    import pprint

    with local_project_id_override("recidiviz-staging"):
        pprint.pprint(
            build_source_table_repository_for_collected_schemata(metadata.project_id())
        )
