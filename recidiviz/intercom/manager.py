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
"""Manager for IntercomAPIClient"""

import io
import os
import re
import time
import zipfile
from datetime import datetime, timedelta

import attr
import pandas as pd

from recidiviz.common import attr_validators
from recidiviz.intercom.client import IntercomAPIClient
from recidiviz.intercom.types import IntercomExportJobResponse, IntercomJobStatus

DEFAULT_MAX_POLL_ATTEMPTS = 30
DEFAULT_POLL_SLEEP_TIME = 10
UPDATE_DATETIME = "update_datetime"
# Matches Intercom export filenames like "receipt_20251121.csv" or
# "hard_bounce_20251121.csv", capturing the base name (everything before
# the trailing `_YYYYMMDD.csv`).
INTERCOM_EXPORT_FILENAME_REGEX = re.compile(r"^(?P<base_name>.+)_\d{8}\.csv$")


def extract_base_name(filename: str) -> str:
    """Returns the base name of an Intercom export CSV filename, stripping the
    trailing `_YYYYMMDD.csv` suffix (e.g. "receipt" from "receipt_20251121.csv",
    or "hard_bounce" from "hard_bounce_20251121.csv")."""
    match = INTERCOM_EXPORT_FILENAME_REGEX.match(filename)
    if not match:
        raise ValueError(
            f"Filename [{filename}] does not match expected Intercom export "
            f"filename pattern [<base_name>_YYYYMMDD.csv]"
        )
    return match.group("base_name")


@attr.define(frozen=True, kw_only=True)
class IntercomAPIManager:
    """Export, download and process Intercom data."""

    client: IntercomAPIClient = attr.ib(
        validator=attr.validators.instance_of(IntercomAPIClient)
    )
    execution_datetime: datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )

    def export_intercom_data(self) -> IntercomExportJobResponse:
        """Create Intercom export job for the previous day."""

        # Calculate date range for previous day
        created_at_after = self.execution_datetime - timedelta(days=1)
        created_at_before = self.execution_datetime

        export_job = self.client.create_data_export(created_at_after, created_at_before)

        return export_job

    def poll_export_status(
        self, job_identifier: str, max_attempts: int = DEFAULT_MAX_POLL_ATTEMPTS
    ) -> IntercomExportJobResponse:
        """Poll export status until complete."""

        for _ in range(max_attempts):
            export_job = self.client.get_export_status(job_identifier)

            if export_job.status == IntercomJobStatus.COMPLETED:
                return export_job
            # Intercom doesn't return an error message so all we know is that we failed
            if export_job.status == IntercomJobStatus.FAILED:
                raise ValueError(f"Export job [{job_identifier}] failed")

            time.sleep(DEFAULT_POLL_SLEEP_TIME)

        raise TimeoutError(
            f"Export job [{job_identifier}] did not complete within {DEFAULT_POLL_SLEEP_TIME * max_attempts} seconds"
        )

    def download_and_process_export(
        self, job_identifier: str, output_dir: str
    ) -> dict[str, str]:
        """
        Download export and extract CSV files to local temp directory.
        Returns dictionary of output filepaths for each base name.
        """

        zip_data = self.client.download_export_data(job_identifier)

        file_paths = {}

        # Extract all CSV files
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zip_file:
            for filename in zip_file.namelist():
                if filename.endswith(".csv"):
                    # Extract base name (e.g., "receipt" from "receipt_20251121.csv")
                    base_name = extract_base_name(filename)

                    # Read CSV and save to temp file
                    with zip_file.open(filename) as csv_file:
                        df = pd.read_csv(csv_file)

                        df[UPDATE_DATETIME] = self.execution_datetime

                        output_path = os.path.join(
                            output_dir,
                            filename,
                        )
                        df.to_csv(output_path, index=False)
                        file_paths[base_name] = output_path

        return file_paths
