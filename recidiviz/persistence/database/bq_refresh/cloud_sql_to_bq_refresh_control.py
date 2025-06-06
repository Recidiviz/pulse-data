# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Control logic for the CloudSQL -> BigQuery refresh."""
import datetime

import pytz

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.big_query_row_streamer import BigQueryRowStreamer
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_to_bq_refresh import (
    federated_bq_schema_refresh,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.source_tables.yaml_managed.collect_yaml_managed_source_table_configs import (
    build_source_table_repository_for_yaml_managed_tables,
)
from recidiviz.source_tables.yaml_managed.datasets import VIEW_UPDATE_METADATA_DATASET
from recidiviz.utils import metadata
from recidiviz.utils.environment import gcp_only

LOCK_WAIT_SLEEP_MAXIMUM_TIMEOUT = 60 * 60 * 4  # 4 hours


# Table that holds information about refresh BQ dataset jobs
REFRESH_BQ_DATASET_TRACKER_ADDRESS = BigQueryAddress(
    dataset_id=VIEW_UPDATE_METADATA_DATASET, table_id="refresh_bq_dataset_tracker"
)


class RefreshBQDatasetSuccessPersister(BigQueryRowStreamer):
    """Class that persists runtime of successful refresh of BQ datasets."""

    def __init__(self, bq_client: BigQueryClient):
        source_table_repository = build_source_table_repository_for_yaml_managed_tables(
            metadata.project_id()
        )
        source_table_config = source_table_repository.get_config(
            REFRESH_BQ_DATASET_TRACKER_ADDRESS
        )
        super().__init__(
            bq_client, source_table_config.address, source_table_config.schema_fields
        )

    def record_success_in_bq(self, schema_type: SchemaType, runtime_sec: int) -> None:
        success_row = {
            "success_timestamp": datetime.datetime.now(tz=pytz.UTC).isoformat(),
            "schema_type": schema_type.value,
            "direct_ingest_instance": DirectIngestInstance.PRIMARY.value,
            "dataset_override_prefix": None,
            "refresh_bq_dataset_runtime_sec": runtime_sec,
        }

        self.stream_rows([success_row])


@gcp_only
def execute_cloud_sql_to_bq_refresh(schema_type: SchemaType) -> None:
    """Executes the Cloud SQL to BQ refresh for a given schema_type, ingest instance and
    sandbox_prefix.
    """
    if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
        raise ValueError(f"Unsupported schema type: [{schema_type}]")

    start = datetime.datetime.now()
    federated_bq_schema_refresh(
        schema_type=schema_type,
        dataset_override_prefix=None,
    )

    end = datetime.datetime.now()
    runtime_sec = int((end - start).total_seconds())
    success_persister = RefreshBQDatasetSuccessPersister(bq_client=BigQueryClientImpl())
    success_persister.record_success_in_bq(
        schema_type=schema_type,
        runtime_sec=runtime_sec,
    )
