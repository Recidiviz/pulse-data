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
"""Write to and read from the intercom_export.export_table BQ table"""

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator.query.state.dataset_config import INTERCOM_EXPORT_DATASET
from recidiviz.intercom.types import IntercomCloudRunJobInfo
from recidiviz.source_tables.intercom_exports_source_table import (
    INTERCOM_EXPORT_TRACKER_TABLE_ID,
)

DEFAULT_TIMEOUT = 60  # seconds


@attr.define(frozen=True, kw_only=True)
class IntercomExportBigQueryTableManager:
    """Write to and read from the intercom_export.export_table BQ table"""

    client: BigQueryClientImpl = attr.ib(
        validator=attr.validators.instance_of(BigQueryClientImpl)
    )

    def write_to_table(
        self,
        cloud_run_job_info: IntercomCloudRunJobInfo,
    ) -> None:
        """Write to the intercom_export.export_table BQ table."""

        big_query_address = BigQueryAddress(
            dataset_id=INTERCOM_EXPORT_DATASET,
            table_id=INTERCOM_EXPORT_TRACKER_TABLE_ID,
        )
        tracker_table_row = [cloud_run_job_info.to_json()]

        tracker_job = self.client.load_into_table_async(
            address=big_query_address,
            rows=tracker_table_row,
        )
        tracker_job.result(timeout=DEFAULT_TIMEOUT)
