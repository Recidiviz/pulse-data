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
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalkerProcessingFailureMode,
)
from recidiviz.big_query.constants import TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
from recidiviz.big_query.view_update_manager import (
    BigQueryViewUpdateSandboxContext,
    create_managed_dataset_and_deploy_views_for_view_builders,
)
from recidiviz.persistence.database.bq_refresh.bq_refresh_status_storage import (
    CloudSqlToBqRefreshStatus,
    store_bq_refresh_status_in_big_query,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_table_big_query_view_collector import (
    FederatedCloudSQLTableBigQueryViewCollector,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.view_registry.deployed_views import (
    CLOUDSQL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA,
)


def federated_bq_schema_refresh(
    schema_type: SchemaType,
    dataset_override_prefix: Optional[str] = None,
) -> List[Table]:
    """Performs a full refresh of BigQuery data for a given schema, pulling data from
    the appropriate CloudSQL Postgres instance.
    """
    config = CloudSqlToBQConfig.for_schema_type(schema_type)
    # Query CloudSQL and export data into datasets with regions that match the instance
    # region (e.g. us-east1)
    _federated_bq_regional_dataset_refresh(config, dataset_override_prefix)

    # Copy the regional datasets to their final resting place in multi-region datasets
    _copy_regional_dataset_to_multi_region(config, dataset_override_prefix)

    _save_status_in_bq(config, dataset_override_prefix)

    return config.get_tables_to_export()


def _save_status_in_bq(
    config: CloudSqlToBQConfig, dataset_override_prefix: Optional[str]
) -> None:
    bq_refresh_statuses: List[CloudSqlToBqRefreshStatus] = []
    last_refresh_datetime = datetime.now(tz=pytz.UTC)
    status_id = uuid.uuid4().hex
    bq_refresh_statuses.append(
        CloudSqlToBqRefreshStatus(
            refresh_run_id=status_id,
            schema=config.schema_type,
            last_refresh_datetime=last_refresh_datetime,
            region_code=None,
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
) -> None:
    """Queries data in the appropriate CloudSQL instance for the given schema / config
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

    collector = FederatedCloudSQLTableBigQueryViewCollector(config)

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

    view_update_sandbox_context = None
    if dataset_override_prefix:
        view_update_sandbox_context = BigQueryViewUpdateSandboxContext(
            output_sandbox_dataset_prefix=dataset_override_prefix,
            input_source_table_overrides=BigQueryAddressOverrides.empty(),
            parent_address_formatter_provider=None,
        )

    create_managed_dataset_and_deploy_views_for_view_builders(
        view_source_table_datasets=set(),
        view_builders_to_update=view_builders,
        view_update_sandbox_context=view_update_sandbox_context,
        bq_region_override=bq_region_override,
        materialize_changed_views_only=False,
        historically_managed_datasets_to_clean=historically_managed_datasets_for_schema,
        failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_EXHAUSTIVELY,
    )


def _copy_regional_dataset_to_multi_region(
    config: CloudSqlToBQConfig, dataset_override_prefix: Optional[str]
) -> None:
    """Copies the unioned regional dataset for a schema to the multi-region dataset
    that contains the same data. Backs up the multi-region dataset before performing
    the copy. This backup dataset will get cleaned up if the copy succeeds, but
    otherwise will stick around for 1 week before tables expire.
    """
    bq_client = BigQueryClientImpl()

    source_dataset_id = config.regional_dataset(dataset_override_prefix)
    destination_dataset_id = config.multi_region_dataset(dataset_override_prefix)

    backup_dataset_id = bq_client.backup_dataset_tables_if_dataset_exists(
        destination_dataset_id
    )

    try:
        bq_client.create_dataset_if_necessary(
            destination_dataset_id,
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
            backup_dataset_id if backup_dataset_id else "NO BACKUP",
        )
        raise e

    if backup_dataset_id:
        bq_client.delete_dataset(
            backup_dataset_id, delete_contents=True, not_found_ok=True
        )
