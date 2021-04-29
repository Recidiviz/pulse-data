# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Script to pull Justice Counts data for US_DE and push it to Google Drive."""

import argparse
import difflib
import logging
import os
import sys
import tempfile
import uuid

import pandas as pd

from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.common.constants import states
from recidiviz.tools.justice_counts import google_drive, manual_upload

DATA_URL = "https://data.delaware.gov/api/views/vnau-c4rn/rows.csv?accessType=DOWNLOAD"

YEAR_COL = "Year"
MONTH_COL = "Month"
INSTITUTION_COL = "Institution"
COUNTY_COL = "County Name"
TYPE_OF_INSTITUTION_COL = "Type of Institution"
SENTENCE_TYPE_COL = "Sentence Type"
GENDER_COL = "Gender"
RACE_COL = "Race"
ETHNICITY_COL = "Ethnicity"

SPREADSHEET_NAME = "DE_A"
WORKSHEET_NAME = "Inmate_Population"


def pull_new_data(directory: str) -> str:
    df = pd.read_csv(DATA_URL)

    year_col = df.pop(YEAR_COL).astype(str)
    month_col = df.pop(MONTH_COL).astype(str)
    month_col = month_col.str.split("-").str[0].str.strip()
    df.insert(0, MONTH_COL, year_col + "-" + month_col)

    df[INSTITUTION_COL] = df[INSTITUTION_COL].str.strip()

    df = df.sort_values(
        by=[
            MONTH_COL,
            COUNTY_COL,
            TYPE_OF_INSTITUTION_COL,
            INSTITUTION_COL,
            SENTENCE_TYPE_COL,
            GENDER_COL,
            RACE_COL,
            ETHNICITY_COL,
        ]
    )

    path = os.path.join(directory, f"{uuid.uuid4()}.csv")
    df.to_csv(path, index=False)
    return path


def pull_old_data(
    drive: google_drive.Drive,
    local_destination_dir: str,
    base_drive_folder_id: str,
) -> str:
    """Pulls data from the Google Sheet and returns the full path to the downloaded csv"""
    data_folder = google_drive.get_data_folder(
        drive, states.StateCode.US_DE, schema.System.CORRECTIONS, base_drive_folder_id
    )
    drive.download_sheets(data_folder.id, local_destination_dir)

    expected_path = os.path.join(
        local_destination_dir,
        manual_upload.csv_filename(SPREADSHEET_NAME, WORKSHEET_NAME),
    )
    if not os.path.exists(expected_path):
        raise FileNotFoundError(
            f"Expected old data to be downloaded to '{expected_path}'."
        )

    return expected_path


def overwrite_with_new_data(
    drive: google_drive.Drive,
    csv_path: str,
    base_drive_folder_id: str,
) -> None:
    """Overwrites the Google Sheet with the new data"""
    data_folder = google_drive.get_data_folder(
        drive, states.StateCode.US_DE, schema.System.CORRECTIONS, base_drive_folder_id
    )
    [sheet] = drive.get_items(
        data_folder.id, google_drive.FileType.SHEET, SPREADSHEET_NAME
    )
    drive.upload_csv(sheet, csv_path, WORKSHEET_NAME)


def main(drive_folder_id: str, credentials_directory: str) -> None:
    temp_dir = os.path.join(tempfile.gettempdir(), "us_de")
    os.makedirs(temp_dir, exist_ok=True)

    drive = google_drive.Drive(credentials_directory, readonly=False)

    new_filepath = pull_new_data(temp_dir)
    old_filepath = pull_old_data(drive, temp_dir, drive_folder_id)

    sys.stdout.writelines(
        difflib.unified_diff(
            open(old_filepath).readlines(),
            open(new_filepath).readlines(),
            "old",
            "new",
        )
    )

    response = input("Proceed (y/N)? ")
    if response not in ("y", "Y"):
        sys.exit(1)

    overwrite_with_new_data(drive, new_filepath, drive_folder_id)


def _create_parser() -> argparse.ArgumentParser:
    """Creates the CLI argument parser."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--drive-folder-id",
        required=True,
        type=str,
        help="The id for the folder root Justice Counts Data Collection folder, which contains subdirectories with all "
        "of the states. The id is the last part of the url, e.g. 'abc123' from "
        "'https://drive.google.com/drive/folders/abc123'.",
    )
    parser.add_argument(
        "--credentials-directory",
        required=False,
        default=".",
        type=str,
        help="Directory where the 'credentials.json' for accessing Google Drive "
        "lives, as well as the cached token. If you do not yet have credentials, "
        "download them from https://console.developers.google.com/apis/credentials",
    )
    parser.add_argument(
        "--log",
        required=False,
        default="INFO",
        type=logging.getLevelName,
        help="Set the logging level",
    )
    return parser


def _configure_logging(level: str) -> None:
    root = logging.getLogger()
    root.setLevel(level)


if __name__ == "__main__":
    arg_parser = _create_parser()
    arguments = arg_parser.parse_args()

    _configure_logging(arguments.log)
    main(
        arguments.drive_folder_id,
        arguments.credentials_directory,
    )
