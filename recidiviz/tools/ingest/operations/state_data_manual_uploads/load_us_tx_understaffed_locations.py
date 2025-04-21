#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
#  #
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  #
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  #
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
#
"""
Critical understaffed offices are determined each month based on staffing vacancies.
An office determined to be critically understaffed has reduced contact standards for that month. 
We receive an excel file each month from TDCJ with the critically understaffed locations.

This script verifies the schema of the file and loads it to GCS for ingest.


Usage
-----
# Download the Google Sheet/ Excel file as a CSV.
# Make sure the file has the expected columns, including month/year and office information.

python -m recidiviz.tools.ingest.operations.state_data_manual_uploads.load_us_tx_understaffed_locations \
   --excel_path <path> \
   --project_id <project_id> \
   [--dry_run <true/false>]
"""
import argparse
import os
from datetime import datetime

import pandas as pd

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.tools.ingest.operations.upload_raw_state_files_to_ingest_bucket_with_date import (
    upload_raw_state_files_to_ingest_bucket_with_date,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

DTYPES = {
    "DPO": str,
    "ALLOTTED": int,
    "VACANT": int,
    "LEAVE": int,
    "VACANT_AND_LEAVE_TOTAL": int,
    "PERCENTAGE_NON_CASE_CARRYING": str,
    "MONTH": int,
    "YEAR": int,
    "OFFC_REGION": str,
    "OFFC_DISTRICT": str,
    "OFFC_TYPE": str,
}
FILE_TAG = "manual_upload_critically_understaffed_locations"


def _parse_args() -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(
        description="Load the monthly US TX critically understaffed locations file"
    )
    parser.add_argument(
        "--csv_path",
        required=True,
        type=str,
        help="The file path of the excel to load",
    )
    parser.add_argument(
        "--dry_run",
        type=str_to_bool,
        default=True,
        help="Whether or not to run this script in dry run (log only) mode.",
    )
    parser.add_argument(
        "--project_id",
        required=True,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Which project the file(s) should be uploaded to (e.g. recidiviz-123).",
    )
    return parser.parse_args()


def _load_df(path: str) -> pd.DataFrame:
    """Loads the CSV file and confirms all columns exist."""
    df = pd.read_csv(path, usecols=DTYPES.keys(), dtype=DTYPES)
    if df.isnull().values.any():
        raise ValueError("There are NULL values in the data!")
    # These values are zero-padded in the Staff raw data file
    df["OFFC_REGION"] = df["OFFC_REGION"].str.zfill(2)
    df["OFFC_DISTRICT"] = df["OFFC_DISTRICT"].str.zfill(2)
    return df


def main() -> None:
    args = _parse_args()
    df = _load_df(args.csv_path)
    print(df.to_markdown(tablefmt="grid", index=False))
    _ = prompt_for_confirmation(f"Load this file to {FILE_TAG} in {args.project_id}?")

    tmp_file = f"{FILE_TAG}.csv"
    print("Creating temporary CSV", tmp_file)
    df.to_csv(tmp_file, index=False)

    try:
        with (
            local_project_id_override(args.project_id),
            cloudsql_proxy_control.connection(schema_type=SchemaType.OPERATIONS),
        ):
            upload_raw_state_files_to_ingest_bucket_with_date(
                paths=[tmp_file],
                project_id=args.project_id,
                region=StateCode.US_TX.value,
                date=str(datetime.utcnow()),
                dry_run=args.dry_run,
            )
    finally:
        print("Removing temporary CSV", tmp_file)
        os.remove(tmp_file)


if __name__ == "__main__":
    main()
