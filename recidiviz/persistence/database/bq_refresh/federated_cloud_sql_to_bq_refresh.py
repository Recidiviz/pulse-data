# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Export data from Cloud SQL and load it into BigQuery."""
import logging
import uuid
from datetime import datetime
from typing import List, Optional

import pytz
from sqlalchemy import Table

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.big_query.view_update_manager import (
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
    create_managed_dataset_and_deploy_views_for_view_builders,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.bq_refresh.bq_refresh_status_storage import (
    CloudSqlToBqRefreshStatus,
    store_bq_refresh_status_in_big_query,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_table_big_query_view import (
    FederatedCloudSQLTableBigQueryViewBuilder,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_table_big_query_view_collector import (
    StateSegmentedSchemaFederatedBigQueryViewCollector,
    UnsegmentedSchemaFederatedBigQueryViewCollector,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.view_registry.address_overrides_factory import (
    address_overrides_for_view_builders,
)
from recidiviz.view_registry.deployed_views import (
    CLOUDSQL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA,
    CLOUDSQL_UNIONED_REGIONAL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA,
)


def federated_bq_schema_refresh(
    schema_type: SchemaType,
    direct_ingest_instance: Optional[DirectIngestInstance] = None,
    dataset_override_prefix: Optional[str] = None,
) -> None:
    """Performs a full refresh of BigQuery data for a given schema, pulling data from
    the appropriate CloudSQL Postgres instance.
    """
    if (
        direct_ingest_instance == DirectIngestInstance.SECONDARY
        and not dataset_override_prefix
    ):
        raise ValueError(
            "Federated refresh can only proceed for secondary databases into a sandbox."
        )

    config = CloudSqlToBQConfig.for_schema_type(schema_type, direct_ingest_instance)
    # Query CloudSQL and export data into datasets with regions that match the instance
    # region (e.g. us-east1)
    refreshed_states = _federated_bq_regional_dataset_refresh(
        config, dataset_override_prefix
    )

    # Copy the regional datasets to their final resting place in multi-region datasets
    _copy_regional_dataset_to_multi_region(config, dataset_override_prefix)

    _save_status_in_bq(config, refreshed_states, dataset_override_prefix)


def _save_status_in_bq(
    config: CloudSqlToBQConfig,
    refreshed_states: Optional[List[StateCode]],
    dataset_override_prefix: Optional[str],
) -> None:
    bq_refresh_statuses: List[CloudSqlToBqRefreshStatus] = []
    last_refresh_datetime = datetime.now(tz=pytz.UTC)
    status_id = uuid.uuid4().hex
    if refreshed_states is None:
        bq_refresh_statuses.append(
            CloudSqlToBqRefreshStatus(
                refresh_run_id=status_id,
                schema=config.schema_type,
                last_refresh_datetime=last_refresh_datetime,
                region_code=None,
            )
        )
    else:
        for state_code in refreshed_states:
            bq_refresh_statuses.append(
                CloudSqlToBqRefreshStatus(
                    refresh_run_id=status_id,
                    schema=config.schema_type,
                    last_refresh_datetime=last_refresh_datetime,
                    region_code=state_code.name,
                )
            )

    store_bq_refresh_status_in_big_query(
        bq_client=BigQueryClientImpl(),
        bq_refresh_statuses=bq_refresh_statuses,
        dataset_override_prefix=dataset_override_prefix,
    )


def _federated_bq_regional_dataset_refresh(
    config: CloudSqlToBQConfig,
    dataset_override_prefix: Optional[str] = None,
) -> Optional[List[StateCode]]:
    """Queries data in the appropriate CloudSQL instance for the given schema / conifg
    and loads it into a single, unified dataset **in the same** region as the CloudSQL
    instance. In the process, creates / updates views that provide direct federated
    connections to the CloudSQL instance and intermediate state-segmented datasets
    (where appropriate).

    Returns the list of states that the data was refreshed for if this is a
    state-segmented schema, or None if it is not.

    Example resulting datasets (OPERATIONS schema):
      operations_cloudsql_connection  <-- Federated views
      us_xx_operations_regional  <-- Materialized data from most recent export for state
      us_yy_operations_regional
      operations_regional  <-- Materialized data from most recent export for each state
    """

    states_that_will_be_refreshed: Optional[List[StateCode]]
    if config.is_state_segmented_refresh_schema():
        state_segmented_collector = StateSegmentedSchemaFederatedBigQueryViewCollector(
            config
        )
        collector: BigQueryViewCollector[
            FederatedCloudSQLTableBigQueryViewBuilder
        ] = state_segmented_collector
        states_that_will_be_refreshed = state_segmented_collector.state_codes_to_collect
    else:
        collector = UnsegmentedSchemaFederatedBigQueryViewCollector(config)
        states_that_will_be_refreshed = None

    view_builders = collector.collect_view_builders()

    # TODO(#7285): Migrate Justice Counts connection to be in same region as instance
    if config.schema_type == SchemaType.JUSTICE_COUNTS:
        bq_region_override = None
    else:
        bq_region_override = SQLAlchemyEngineManager.get_cloudsql_instance_region(
            config.schema_type
        )

    historically_managed_datasets_for_schema = (
        CLOUDSQL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA[
            config.schema_type
        ]
    )

    if config.region_codes_to_exclude is not None:
        # Remove any datasets associated with region codes excluded from the current
        # refresh from the list of managed datasets that should be cleaned. Datasets
        # should not be cleaned up for any region that is still managed but is
        # temporarily not included in the refresh.
        historically_managed_datasets_for_schema = {
            dataset
            for dataset in historically_managed_datasets_for_schema
            for region_code in config.region_codes_to_exclude
            if region_code.lower() not in dataset
        }

    address_overrides = None
    if dataset_override_prefix:
        address_overrides = address_overrides_for_view_builders(
            view_dataset_override_prefix=dataset_override_prefix,
            view_builders=view_builders,
        )

    create_managed_dataset_and_deploy_views_for_view_builders(
        view_source_table_datasets=set(),
        view_builders_to_update=view_builders,
        address_overrides=address_overrides,
        bq_region_override=bq_region_override,
        force_materialize=True,
        historically_managed_datasets_to_clean=historically_managed_datasets_for_schema,
    )

    if config.is_state_segmented_refresh_schema():
        _hydrate_unioned_regional_dataset_for_schema(
            config,
            bq_region_override,
            dataset_override_prefix,
        )

    return states_that_will_be_refreshed


def _copy_regional_dataset_to_multi_region(
    config: CloudSqlToBQConfig, dataset_override_prefix: Optional[str]
) -> None:
    """Copies the unioned regional dataset for a schema to the multi-region dataset
    that contains the same data. Backs up the multi-region dataset before performing
    the copy. This backup dataset will get cleaned up if the copy succeeds, but
    otherwise will stick around for 1 week before tables expire.
    """
    bq_client = BigQueryClientImpl()

    source_dataset_id = config.unioned_regional_dataset(dataset_override_prefix)
    destination_dataset_id = config.unioned_multi_region_dataset(
        dataset_override_prefix
    )
    destination_dataset = bq_client.dataset_ref_for_id(destination_dataset_id)

    backup_dataset = bq_client.backup_dataset_tables_if_dataset_exists(
        destination_dataset_id
    )

    try:

        bq_client.create_dataset_if_necessary(
            destination_dataset,
            default_table_expiration_ms=(
                TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
                if dataset_override_prefix
                else None
            ),
        )

        # Copy into the canonical unioned source datasets in the US multi-region
        bq_client.copy_dataset_tables_across_regions(
            source_dataset_id=source_dataset_id,
            destination_dataset_id=destination_dataset_id,
            overwrite_destination_tables=True,
        )
    except Exception as e:
        logging.info(
            "Failed to flash [%s] to [%s] - contents backup can be found at [%s]",
            source_dataset_id,
            destination_dataset_id,
            backup_dataset.dataset_id if backup_dataset else "NO BACKUP",
        )
        raise e

    if backup_dataset:
        bq_client.delete_dataset(
            backup_dataset, delete_contents=True, not_found_ok=True
        )


class UnionedStateSegmentsViewBuilder(BigQueryViewBuilder[BigQueryView]):
    """A view that unions the contents of a given table across all state segments."""

    def __init__(
        self,
        *,
        config: CloudSqlToBQConfig,
        table: Table,
        state_codes: List[StateCode],
    ):
        if not config.is_state_segmented_refresh_schema():
            raise ValueError(f"Unexpected schema type [{config.schema_type.name}]")

        self.config = config
        self.table = table
        self.state_codes = state_codes
        # Dataset prefixing will ge handled automatically by view building logic
        self.dataset_id = config.unioned_regional_dataset(dataset_override_prefix=None)
        self.view_id = f"{table.name}_view"
        self.projects_to_deploy = None
        self.materialized_address = self._build_materialized_address(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            materialized_address_override=BigQueryAddress(
                dataset_id=self.dataset_id,
                table_id=table.name,
            ),
            should_materialize=True,
        )

    def _build(
        self, *, address_overrides: Optional[BigQueryAddressOverrides] = None
    ) -> BigQueryView:
        (
            table_union_query_fmt,
            kwargs,
        ) = self.config.get_unioned_table_view_query_format_string(
            self.state_codes, self.table
        )

        return BigQueryView(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            materialized_address=self.materialized_address,
            address_overrides=address_overrides,
            description=f"A view that unions the contents of the [{self.table.name}] "
            f"across all state segments.",
            view_query_template=table_union_query_fmt,
            clustering_fields=None,
            should_deploy_predicate=None,
            **kwargs,
        )


def _hydrate_unioned_regional_dataset_for_schema(
    config: CloudSqlToBQConfig,
    bq_region_override: Optional[str],
    dataset_override_prefix: Optional[str],
) -> None:
    """Given a set of already hydrated single-state datasets, unions the contents
    and copies the results to a dataset that lives in the same region as the CloudSQL
    instance (e.g. us-east1).

    For example given these tables:
        us_xx_operations_regional
            direct_ingest_raw_file_metadata
            direct_ingest_ingest_file_metadata
        us_yy_operations_regional
            direct_ingest_raw_file_metadata
            direct_ingest_ingest_file_metadata

    ...we will create a single dataset (or overwrite what exists):
        operations_regional
            direct_ingest_raw_file_metadata  <-- has data from US_XX and US_YY
            direct_ingest_ingest_file_metadata  <-- has data from US_XX and US_YY
    """

    if not config.is_state_segmented_refresh_schema():
        raise ValueError(f"Unexpected schema_type [{config.schema_type}].")

    state_codes = get_direct_ingest_states_existing_in_env()

    refreshed_source_table_datasets = {
        config.materialized_dataset_for_segment(state_code)
        for state_code in state_codes
        if state_code.value not in config.region_codes_to_exclude
    }

    stale_schema_datasets = {
        config.materialized_dataset_for_segment(state_code)
        for state_code in state_codes
        if state_code.value in config.region_codes_to_exclude
    }
    source_table_datasets = refreshed_source_table_datasets | stale_schema_datasets

    if stale_schema_datasets and refreshed_source_table_datasets:
        # We need to make sure the schemas match those that are refreshed.
        #
        # DISCLAIMER: if a column were renamed in a Postgres migration, that migration
        # would not be properly reflected with this schema update - the data in the new
        # column would be wiped for the new schemas. This code is meant to handle pure
        # column/table additions and deletions.
        reference_dataset_id = next(iter(refreshed_source_table_datasets))
        if dataset_override_prefix:
            reference_dataset_id = f"{dataset_override_prefix}_{reference_dataset_id}"
            stale_schema_datasets = {
                f"{dataset_override_prefix}_{dataset_id}"
                for dataset_id in stale_schema_datasets
            }

        bq_client = BigQueryClientImpl(region_override=bq_region_override)
        bq_client.update_datasets_to_match_reference_schema(
            reference_dataset_id, list(stale_schema_datasets)
        )

    view_builders = [
        UnionedStateSegmentsViewBuilder(config=config, table=t, state_codes=state_codes)
        for t in config.get_tables_to_export()
    ]
    address_overrides = None
    if dataset_override_prefix:
        address_overrides = address_overrides_for_view_builders(
            view_dataset_override_prefix=dataset_override_prefix,
            view_builders=view_builders,
        )
        address_overrides_builder = address_overrides.to_builder(
            sandbox_prefix=dataset_override_prefix
        )
        for dataset in source_table_datasets:
            address_overrides_builder.register_sandbox_override_for_entire_dataset(
                dataset
            )
        address_overrides = address_overrides_builder.build()

    historically_managed_unioned_regional_datasets_for_schema = CLOUDSQL_UNIONED_REGIONAL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA[
        config.schema_type
    ]

    create_managed_dataset_and_deploy_views_for_view_builders(
        view_source_table_datasets=source_table_datasets,
        view_builders_to_update=view_builders,
        address_overrides=address_overrides,
        bq_region_override=bq_region_override,
        force_materialize=True,
        historically_managed_datasets_to_clean=historically_managed_unioned_regional_datasets_for_schema,
    )
