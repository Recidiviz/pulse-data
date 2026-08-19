# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Write to and read from the intercom_export_metadata.export_tracker BQ table"""

from datetime import datetime

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.intercom.intercom_export_columns import (
    EXPORT_WINDOW_END_INCLUSIVE_COLUMN_NAME,
    STATUS_COLUMN_NAME,
)
from recidiviz.intercom.types import IntercomCloudRunJobInfo, IntercomCloudRunJobStatus
from recidiviz.source_tables.intercom_export_source_tables import (
    INTERCOM_EXPORT_METADATA_DATASET,
    INTERCOM_EXPORT_METADATA_EXPORT_TRACKER_TABLE_ID,
)

DEFAULT_TIMEOUT = 60  # seconds


@attr.define(frozen=True, kw_only=True)
class IntercomExportBigQueryTableManager:
    """Write to and read from the intercom_export_metadata.export_tracker BQ table"""

    client: BigQueryClientImpl = attr.ib(
        validator=attr.validators.instance_of(BigQueryClientImpl)
    )

    @property
    def intercom_export_tracker_address(self) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=INTERCOM_EXPORT_METADATA_DATASET,
            table_id=INTERCOM_EXPORT_METADATA_EXPORT_TRACKER_TABLE_ID,
        )

    def write_to_table(
        self,
        cloud_run_job_info: IntercomCloudRunJobInfo,
    ) -> None:
        """Write to the intercom_export_metadata.export_tracker BQ table."""

        tracker_table_row = cloud_run_job_info.to_json()

        tracker_job = self.client.load_into_table_async(
            address=self.intercom_export_tracker_address,
            rows=[tracker_table_row],
        )
        tracker_job.result(timeout=DEFAULT_TIMEOUT)

    def get_latest_export_window_end(self) -> datetime:
        """
        Returns the export_window_end_inclusive timestamp for the most recent
        successful export job on the intercom_export_metadata.export_tracker BQ table.
        """

        query = f"""
            SELECT MAX({EXPORT_WINDOW_END_INCLUSIVE_COLUMN_NAME}) AS {EXPORT_WINDOW_END_INCLUSIVE_COLUMN_NAME}
            FROM {self.intercom_export_tracker_address.to_str()}
            WHERE {STATUS_COLUMN_NAME} = '{IntercomCloudRunJobStatus.SUCCESS.value}'
        """

        query_job = self.client.run_query_async(query_str=query, use_query_cache=False)
        results = query_job.result(timeout=DEFAULT_TIMEOUT)
        most_recent_end_datetime = next(results).export_window_end_inclusive

        if not most_recent_end_datetime:
            raise ValueError(
                f"No successful Intercom export job runs found for query: [{query}]."
            )

        return most_recent_end_datetime
