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


- Loading the excel file from a given path.
- Verifying the schema of the file is expected and doing some light cleaning.
- Loading the file as a CSV to GCS for ingest, with file_tag "manual_upload_critically_understaffed_locations"


Usage
-----
# Download the excel file

python -m recidiviz.tools.ingest.operations.state_data_manual_uploads.load_us_tx_understaffed_locations \
   --excel_path <path> \
   --project_id <project_id> \
   [--dry_run <true/false>]
"""
import argparse
import os
from datetime import date, datetime

import pandas as pd

from recidiviz.common.constants.states import StateCode
from recidiviz.tools.ingest.operations.upload_raw_state_files_to_ingest_bucket_with_date import (
    upload_raw_state_files_to_ingest_bucket_with_date,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.params import str_to_bool

# input from excel --> output name for csv and BQ
COLUMNS = {
    "DPO": "DPO",
    "ALLOTTED": "ALLOTTED",
    "VACANT": "VACANT",
    "LEAVE": "LEAVE",
    "VACANT & LEAVE TOTAL": "VACANT_AND_LEAVE_TOTAL",
    "PERCENTAGE NON-CASE CARRYING": "PERCENTAGE_NON_CASE_CARRYING",
}
FILE_TAG = "manual_upload_critically_understaffed_locations"


def _parse_args(today: date) -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(
        description="Load the monthly US TX critically understaffed locations file"
    )
    parser.add_argument(
        "--excel_path",
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
    parser.add_argument(
        "--month",
        type=int,
        default=today.month,
        help="The month this data is for. Defaults to the current month.",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=today.year,
        help="The year this data is for. Defaults to the current year.",
    )
    return parser.parse_args()


def _load_df(path: str, month: int, year: int) -> pd.DataFrame:
    """
    Loads the excel file.
      - Confirms all COLUMNS exist. If more columns exist, they are ignored.
      - Adds MONTH and YEAR columns
      - Removes "Region" header and total rows
    """
    df = pd.read_excel(path)
    df.columns = df.columns.str.strip().str.upper()
    # Sometimes staff writes out notes in the margin of the Excel file.
    # We are only interested in the columns we've defined in the constant,
    # so this step will remove any columns that are not in the COLUMNS dict.
    # This will fail if not all of the COLUMNS are present in the file.
    df = df[list(COLUMNS.keys())].rename(columns=COLUMNS)
    # filters out any row where DPO starts with region
    df = df.loc[~df.DPO.str.lower().str.startswith("region")]
    df["MONTH"] = month
    df["YEAR"] = year
    if df.isnull().values.any():
        raise ValueError("There are NULL values in the data!")
    return df


def main() -> None:
    now = datetime.utcnow()
    args = _parse_args(now.date())
    df = _load_df(args.excel_path, args.month, args.year)
    print(df.to_markdown(tablefmt="heavy_grid", index=False))
    _ = prompt_for_confirmation(
        f"Load this file to {FILE_TAG} for {args.year},{args.month} in {args.project_id}?"
    )

    tmp_file = f"{FILE_TAG}.csv"
    print("Creating temporary CSV", tmp_file)
    df.to_csv(tmp_file, index=False)

    try:
        upload_raw_state_files_to_ingest_bucket_with_date(
            paths=[tmp_file],
            project_id=args.project_id,
            region=StateCode.US_TX.value,
            date=str(now),
            dry_run=args.dry_run,
        )
    finally:
        print("Removing temporary CSV", tmp_file)
        os.remove(tmp_file)


if __name__ == "__main__":
    main()
