#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
""" Script that reads the TDPOP excel spreadsheet specified in the command line arguments,
    converts it to a CSV, add a date column with the passed in as a command line argument,
    cleans up the headers, and uploads to a specified GCS bucket.
    Example usage:
    uv run python -m recidiviz.tools.ingest.regions.us_tn.convert_tdpop_to_csv_and_push_to_bucket \
    --excel_absolute_file_path ~/Desktop/TDPOP.xlsx --date 2022-09-22 --region us_tn \
    --project_id recidiviz-staging --destination_bucket recidiviz-staging-us-tn-scratch
"""
import argparse
import datetime
import logging
import os
import sys
from typing import List, Tuple

import numpy as np
import pandas as pd

import recidiviz
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.tools.ingest.operations.upload_raw_state_files_to_ingest_bucket_with_date import (
    upload_raw_state_files_to_ingest_bucket_with_date,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def read_and_convert_excel_to_csv(
    excel_absolute_file_path: str,
    date_str: str,
    region: str,
    project_id: str,
    destination_bucket: str,
) -> None:
    """
    Read the TDPOP excel spreadsheet and convert it to a CSV. Add a date column with the passed in `date` value,
    and clean up the headers so that they can be parsed by BQ. Push the converted CSV to the specified
    project id and bucket.
    """

    def clean_up_header(header: str) -> str:
        """Update all headers and replace spaces with underscores. Fix dashes to be underscores as well."""
        convert_spaces = header.replace(" ", "_")
        return convert_spaces.replace("-", "_")

    file_name = "TDPOP.csv"

    # Specify the path where the converted CSV will temporarily be stored.
    csv_path = os.path.join(
        os.path.dirname(recidiviz.__file__),
        "ingest",
        "direct",
        "regions",
        "us_tn",
        "raw_data",
        file_name,
    )

    # Read the excel spreadsheet and convert it to a CSV.
    logging.info(
        "Reading in TDPOP excel sheet from path: [%s]", excel_absolute_file_path
    )
    tdpop_excel = pd.read_excel(excel_absolute_file_path, sheet_name="TDPOP", dtype=str)

    logging.info(
        "Converting sheet to CSV and storing in path: %s",
        csv_path,
    )
    tdpop_excel.to_csv(csv_path, encoding="cp1252", index=False, header=True)

    # Pull in the CSV.
    df = pd.read_csv(csv_path, encoding="cp1252", dtype=str)

    logging.info("Modifying CSV.")

    # Clean up headers
    updated_headers = [clean_up_header(header) for header in df.columns]
    df.columns = updated_headers

    # Add new column with passed in date

    # This will crash if things aren't formatted correctly
    date = datetime.date.fromisoformat(date_str)

    # Convert back to an ISO string
    df["Date"] = date.isoformat()

    # Remove extra leading and trailing whitespace.
    df["First_Name"] = df["First_Name"].str.strip()
    df["Last_Name"] = df["Last_Name"].str.strip()
    df["Offense"] = df["Offense"].str.strip()
    df["County_Conviction"] = df["County_Conviction"].str.strip()
    df["Admit_Reason"] = df["Admit_Reason"].str.strip()
    df["Site"] = df["Site"].str.strip()

    # Rename OffenderID to match yaml and remove / from Last_RNA column name"
    df.rename(
        columns={
            "Offender_ID": "OffenderID",
            "Last_RNA_in_Prison/Jail": "Last_RNA_in_Prison_Jail",
        },
        inplace=True,
    )
    logging.info("**************")
    logging.info(
        "Confirming that the actual columns match expected columns from raw config file "
    )
    # Confirm that columns of this dataframe match columns in raw data config for TDPOP file
    region_config = DirectIngestRegionRawFileConfig(region_code="us_tn")
    expected_columns = region_config.raw_file_configs["TDPOP"].current_columns
    tdpop_column_list = np.array([o.name for o in expected_columns])
    actual_columns = np.array(df.columns)
    matching = (actual_columns == tdpop_column_list).all()
    if matching:
        prompt_for_confirmation(
            "Confirmed that the actual columns match expected columns from raw config file...If you want to continue "
            "enter `y`. "
        )
    else:
        return logging.info("Columns do not match")
    logging.info("**************")

    # Override the CSV with the updates and set delimiter to double dagger.
    df.to_csv(csv_path, encoding="cp1252", index=False, sep="‡")

    # trigger `upload_raw_state_files_to_ingest_bucket_with_date`, but confirm first that push to bucket should proceed.
    prompt_for_confirmation(
        f"Excel spreadsheet has successfully been converted and saved in `{file_name}`. Are you sure that you would "
        f"like to push it to `{project_id}` in the bucket: `{destination_bucket}`? If you do not want to push it "
        "manually, enter `n`."
    )
    logging.info(
        "Launching script `upload_raw_state_files_to_ingest_bucket_with_date` to upload %s to bucket...",
        file_name,
    )
    logging.info("**************")

    upload_raw_state_files_to_ingest_bucket_with_date(
        paths=[csv_path],
        date=date_str,
        project_id=project_id,
        region=region,
        dry_run=False,
        destination_bucket=destination_bucket,
    )


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--excel_absolute_file_path",
        dest="excel_absolute_file_path",
        help="The absolute file path of the CSV that is imported.",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--date",
        dest="date",
        help="The date that the sheet was generated (format must be: YYYY-mm-dd, ex: 2022-02-22).",
        type=str,
        required=True,
    )

    parser.add_argument("--region", dest="region", required=True, help="E.g. 'us_tn'")

    parser.add_argument(
        "--project_id",
        dest="project_id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="The environment would like the converted CSV to be uploaded to (recidiviz-staging or recidiviz-123)",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--destination_bucket",
        dest="destination_bucket",
        help="The destination bucket would like the converted CSV to be uploaded to"
        " (ex: `recidiviz-staging-my-test-bucket`)",
        type=str,
        required=True,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)
    with local_project_id_override(known_args.project_id):
        with cloudsql_proxy_control.connection(schema_type=SchemaType.OPERATIONS):
            read_and_convert_excel_to_csv(
                known_args.excel_absolute_file_path,
                known_args.date,
                known_args.region,
                known_args.project_id,
                known_args.destination_bucket,
            )
