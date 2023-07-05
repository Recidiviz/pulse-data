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
"""Class that persists runtime of successful view export jobs to BQ."""
import datetime
from typing import List, Optional

import pytz
from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.success_persister import (
    SUCCESS_TIMESTAMP_COL,
    SuccessPersister,
)

# Table that holds information about metric view data export jobs
METRIC_VIEW_DATA_EXPORT_TRACKER_TABLE_ID = "metric_view_data_export_tracker"

EXPORT_JOB_NAME_COL = "export_job_name"
STATE_CODE_COL = "state_code"
METRIC_VIEW_DATA_EXPORT_RUNTIME_SEC_COL = "metric_view_data_export_runtime_sec"
DESTINATION_OVERRIDE_COL = "destination_override"
SANDBOX_DATASET_PREFIX_COL = "sandbox_dataset_prefix"


class MetricViewDataExportSuccessPersister(SuccessPersister):
    """Class that persists runtime of successful export of metric view data."""

    def __init__(self, bq_client: BigQueryClient):
        super().__init__(bq_client, METRIC_VIEW_DATA_EXPORT_TRACKER_TABLE_ID)

    def record_success_in_bq(
        self,
        export_job_name: str,
        runtime_sec: int,
        state_code: Optional[str],
        destination_override: Optional[str],
        sandbox_dataset_prefix: Optional[str],
    ) -> None:
        success_row = {
            SUCCESS_TIMESTAMP_COL: datetime.datetime.now(tz=pytz.UTC).isoformat(),
            EXPORT_JOB_NAME_COL: export_job_name,
            STATE_CODE_COL: state_code,
            DESTINATION_OVERRIDE_COL: destination_override,
            SANDBOX_DATASET_PREFIX_COL: sandbox_dataset_prefix,
            METRIC_VIEW_DATA_EXPORT_RUNTIME_SEC_COL: runtime_sec,
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
                name=EXPORT_JOB_NAME_COL,
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=STATE_CODE_COL,
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
            ),
            bigquery.SchemaField(
                name=DESTINATION_OVERRIDE_COL,
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
            ),
            bigquery.SchemaField(
                name=SANDBOX_DATASET_PREFIX_COL,
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
            ),
            bigquery.SchemaField(
                name=METRIC_VIEW_DATA_EXPORT_RUNTIME_SEC_COL,
                field_type=bigquery.enums.SqlTypeNames.INT64.value,
                mode="REQUIRED",
            ),
        ]
