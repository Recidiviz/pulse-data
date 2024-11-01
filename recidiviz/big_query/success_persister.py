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
"""Class that persists runtime of successful jobs to BQ."""
import abc
import datetime
from typing import List, Optional

import pytz
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_row_streamer import BigQueryRowStreamer
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

#  Dataset with metadata about view update operations
from recidiviz.persistence.database.schema_type import SchemaType

VIEW_UPDATE_METADATA_DATASET = "view_update_metadata"

SUCCESS_TIMESTAMP_COL = "success_timestamp"
NUM_DEPLOYED_VIEWS_COL = "num_deployed_views"

# Table that holds information about update all jobs
VIEW_UPDATE_TRACKER_TABLE_ID = "view_update_tracker"

ViEW_UPDATE_RUNTIME_SEC_COL = "view_update_runtime_sec"

# Table that holds information about refresh BQ dataset jobs
REFRESH_BQ_DATASET_TRACKER_TABLE_ID = "refresh_bq_dataset_tracker"

REFRESH_BQ_DATASET_RUNTIME_SEC_COL = "refresh_bq_dataset_runtime_sec"
SCHEMA_TYPE_COL = "schema_type"
DIRECT_INGEST_INSTANCE_COL = "direct_ingest_instance"
DATASET_OVERRIDE_PREFIX = "dataset_override_prefix"


class SuccessPersister:
    """Base class to persist runtime of successful view jobs in BQ."""

    def __init__(self, bq_client: BigQueryClient, table_id: str) -> None:
        self.table_address = BigQueryAddress(
            dataset_id=VIEW_UPDATE_METADATA_DATASET,
            table_id=table_id,
        )
        self.bq_row_streamer = BigQueryRowStreamer(
            bq_client=bq_client,
            table_address=self.table_address,
            table_schema=self._get_table_schema(),
        )

    @abc.abstractmethod
    def _get_table_schema(self) -> List[bigquery.SchemaField]:
        """Returns table schema to use for recording successes."""


class AllViewsUpdateSuccessPersister(SuccessPersister):
    """Class that persists runtime of successful updated view jobs to BQ"""

    def __init__(self, bq_client: BigQueryClient):
        super().__init__(bq_client, VIEW_UPDATE_TRACKER_TABLE_ID)

    def record_success_in_bq(
        self,
        deployed_view_builders: List[BigQueryViewBuilder],
        dataset_override_prefix: Optional[str],
        runtime_sec: int,
    ) -> None:
        num_deployed_views = len(deployed_view_builders)

        success_row = {
            SUCCESS_TIMESTAMP_COL: datetime.datetime.now(tz=pytz.UTC).isoformat(),
            DATASET_OVERRIDE_PREFIX: dataset_override_prefix,
            NUM_DEPLOYED_VIEWS_COL: num_deployed_views,
            ViEW_UPDATE_RUNTIME_SEC_COL: runtime_sec,
        }

        self.bq_row_streamer.stream_rows([success_row])

    def _get_table_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField(
                name=SUCCESS_TIMESTAMP_COL,
                field_type=bigquery.enums.SqlTypeNames.TIMESTAMP.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=DATASET_OVERRIDE_PREFIX,
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="NULLABLE",
            ),
            bigquery.SchemaField(
                name=NUM_DEPLOYED_VIEWS_COL,
                field_type=bigquery.enums.SqlTypeNames.INT64.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=ViEW_UPDATE_RUNTIME_SEC_COL,
                field_type=bigquery.enums.SqlTypeNames.INT64.value,
                mode="REQUIRED",
            ),
        ]


class RefreshBQDatasetSuccessPersister(SuccessPersister):
    """Class that persists runtime of successful refresh of BQ datasets."""

    def __init__(self, bq_client: BigQueryClient):
        super().__init__(bq_client, REFRESH_BQ_DATASET_TRACKER_TABLE_ID)

    def record_success_in_bq(
        self,
        schema_type: SchemaType,
        direct_ingest_instance: DirectIngestInstance,
        dataset_override_prefix: Optional[str],
        runtime_sec: int,
    ) -> None:
        success_row = {
            SUCCESS_TIMESTAMP_COL: datetime.datetime.now(tz=pytz.UTC).isoformat(),
            SCHEMA_TYPE_COL: schema_type.value,
            DIRECT_INGEST_INSTANCE_COL: direct_ingest_instance.value,
            DATASET_OVERRIDE_PREFIX: dataset_override_prefix,
            REFRESH_BQ_DATASET_RUNTIME_SEC_COL: runtime_sec,
        }

        self.bq_row_streamer.stream_rows([success_row])

    def _get_table_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField(
                name=SUCCESS_TIMESTAMP_COL,
                field_type=bigquery.enums.SqlTypeNames.TIMESTAMP.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=SCHEMA_TYPE_COL,
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=DIRECT_INGEST_INSTANCE_COL,
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="NULLABLE",
            ),
            bigquery.SchemaField(
                name=DATASET_OVERRIDE_PREFIX,
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="NULLABLE",
            ),
            bigquery.SchemaField(
                name=REFRESH_BQ_DATASET_RUNTIME_SEC_COL,
                field_type=bigquery.enums.SqlTypeNames.INT64.value,
                mode="REQUIRED",
            ),
        ]
