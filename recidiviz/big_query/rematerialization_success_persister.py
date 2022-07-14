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
"""Class that persists runtime of successful view rematerialization jobs to BQ."""
import datetime
from typing import List

import pytz
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_row_streamer import BigQueryRowStreamer
from recidiviz.big_query.big_query_view import BigQueryViewBuilder

#  Dataset with metadata about view update operations
VIEW_UPDATE_METADATA_DATASET = "view_update_metadata"

# Table that holds info about successful rematerialization jobs
REMATERIALIZATION_TRACKER_TABLE_ID = "rematerialization_tracker"

CLOUD_TASK_ID_COL = "cloud_task_id"
SUCCESS_TIMESTAMP_COL = "success_timestamp"
NUM_DEPLOYED_VIEWS_COL = "num_deployed_views"
NUM_MATERIALIZED_VIEWS_COL = "num_materialized_views"
REMATERIAIZATION_RUNTIME_SEC_COL = "rematerializaton_runtime_sec"


class RematerializationSuccessPersister:
    """Class that persists runtime of successful view rematerialization jobs to BQ."""

    def __init__(self, bq_client: BigQueryClient) -> None:
        self.table_address = BigQueryAddress(
            dataset_id=VIEW_UPDATE_METADATA_DATASET,
            table_id=REMATERIALIZATION_TRACKER_TABLE_ID,
        )
        self.bq_row_streamer = BigQueryRowStreamer(
            bq_client=bq_client,
            table_address=self.table_address,
            table_schema=self._get_table_schema(),
        )

    def record_success_in_bq(
        self,
        deployed_view_builders: List[BigQueryViewBuilder],
        rematerialization_runtime_sec: int,
        cloud_task_id: str,
    ) -> None:
        num_deployed_views = len(deployed_view_builders)
        num_views_materialized = len(
            [v for v in deployed_view_builders if v.materialized_address]
        )

        success_row = {
            CLOUD_TASK_ID_COL: cloud_task_id,
            SUCCESS_TIMESTAMP_COL: datetime.datetime.now(tz=pytz.UTC).isoformat(),
            NUM_DEPLOYED_VIEWS_COL: num_deployed_views,
            NUM_MATERIALIZED_VIEWS_COL: num_views_materialized,
            REMATERIAIZATION_RUNTIME_SEC_COL: rematerialization_runtime_sec,
        }

        self.bq_row_streamer.stream_rows([success_row])

    def _get_table_schema(self) -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField(
                name=CLOUD_TASK_ID_COL,
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=SUCCESS_TIMESTAMP_COL,
                field_type=bigquery.enums.SqlTypeNames.TIMESTAMP.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=NUM_DEPLOYED_VIEWS_COL,
                field_type=bigquery.enums.SqlTypeNames.INT64.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=NUM_MATERIALIZED_VIEWS_COL,
                field_type=bigquery.enums.SqlTypeNames.INT64.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=REMATERIAIZATION_RUNTIME_SEC_COL,
                field_type=bigquery.enums.SqlTypeNames.INT64.value,
                mode="REQUIRED",
            ),
        ]
