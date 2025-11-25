# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""
Determines latest dates in storage for various states. Could be used to when copying
data from prod to staging.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.ingest.operations.get_last_stored_date_for_states \
    --states US_ND,US_TN,US_MI,US_OR
"""


import argparse
import logging

from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_storage_directory_path_for_state,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.gsutil_shell_helpers import gsutil_ls
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS


def get_latest_date_folders(
    states: list[StateCode], project_id: str, ingest_instance: DirectIngestInstance
) -> dict[StateCode, dict]:
    """Gets and logs the latest date folders for each state in the given project.

    Args:
        states: List of StateCode objects to gather dates for.
        project_id: GCP project ID to query.
        ingest_instance: DirectIngestInstance to query.

    Returns:
        A dictionary mapping StateCode to a dict containing:
            - 'date': The latest date in YYYY/MM/DD format
            - 'file_count': Number of files in the latest date folder
            - 'path': The full GCS path to the latest date folder
    """
    logging.info(
        "Gathering last dates. Be patient -- this is slower than you would think..."
    )

    results: dict[StateCode, dict] = {}

    # Initialize a GCS client
    for state in states:
        state_storage_bucket = gcsfs_direct_ingest_storage_directory_path_for_state(
            region_code=state.value,
            ingest_instance=ingest_instance,
            project_id=project_id,
        )

        uri = state_storage_bucket.uri() + "raw/"

        last_year = gsutil_ls(uri + "*", directories_only=True)[-1]
        last_month = gsutil_ls(last_year + "*", directories_only=True)[-1]
        last_day = gsutil_ls(last_month + "*", directories_only=True)[-1]

        # Count files in the latest date folder
        files_in_latest = gsutil_ls(last_day + "*", directories_only=False)
        file_count = len(files_in_latest)

        # Remove 'gs://' prefix from last_day for the link so we can make a clickable
        # link
        last_day_path = GcsfsDirectoryPath.from_absolute_path(last_day)
        last_day_console_link = last_day_path.cloud_console_link_for_gcs_path()
        logging.info(
            "Last path for %s is: %s (%d files). \n Link: %s",
            state.value,
            last_day,
            file_count,
            last_day_console_link,
        )

        # Extract date in YYYY/MM/DD format from the path
        # last_day format: gs://bucket/us_xx/raw/YYYY/MM/DD/
        path_parts = last_day.rstrip("/").split("/")
        date_str = "/".join(path_parts[-3:])  # Get last 3 parts: YYYY/MM/DD

        results[state] = {
            "date": date_str,
            "file_count": file_count,
            "path": last_day,
        }

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--states",
        required=True,
        type=StateCode,
        nargs="+",
        choices=list(StateCode),
        help="State codes for which to return latest dates.",
    )

    parser.add_argument(
        "--project_id",
        choices=DATA_PLATFORM_GCP_PROJECTS,
        required=True,
        type=str,
        help="Which project's files should be moved from (e.g. recidiviz-123).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    result = get_latest_date_folders(
        args.states, args.project_id, DirectIngestInstance.PRIMARY
    )
    # When run as a script, also print the results for visibility
    logging.info("Results: %s", result)
