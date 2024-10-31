# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
This script uploads large data files to Google Cloud Storage in manageable chunks.
It splits an input CSV or Excel file into smaller chunks of 5000 rows each, saves each chunk
as a new Excel/CSV file, and uploads it to a specified Google Cloud Storage path. If the original
file is a CSV, each chunked Excel file will use the CSV's filename (excluding the `.csv`
extension) as its sheet name.

Usage:
    python -m recidiviz.tools.justice_counts.large_file_upload \
      --file_path=<FILE_PATH> \
      --dry-run=<TRUE|FALSE> \
      [--system=<SYSTEM_NAME>]

Arguments:
    --file_path (str): Path to the local file to upload (CSV or Excel).
    --dry-run (bool): If True, the file will not be uploaded, only processed.
    --system (str, optional): System name used in the destination path for organized storage.

Example:
    python -m recidiviz.tools.justice_counts.large_file_upload \
      --file_path="data/large_data_file.csv" \
      --dry-run=true
"""

import argparse
import getpass
import logging
import os
import tempfile  # Import tempfile module
import time

import pandas as pd
import pysftp

from recidiviz.utils.params import str_to_bool

logger = logging.getLogger(__name__)

CHUNK_SIZE = 5000
WAIT_TIME = 3000  # seconds (5 minutes)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    parser.add_argument("--file_path", type=str, required=True)
    parser.add_argument("--system", type=str, default=None)
    return parser


def get_file_chunk(
    sheet_df: pd.DataFrame, file_path: str, sheet_name: str, index: int
) -> str:
    """
    Extracts a chunk of rows from a DataFrame and saves it as a temporary file.

    This function takes a slice of `sheet_df`, specified by `index`,
    and saves it as a temporary file in either CSV or Excel format,
    based on the `file_path` extension. Each chunk corresponds to
    `CHUNK_SIZE` rows, starting from `index * CHUNK_SIZE`.

    Args:
        sheet_df (pd.DataFrame): The DataFrame to extract the chunk from.
        file_path (str): The original file path, used to determine the format
            (CSV or Excel) of the output.
        sheet_name (str): The name of the sheet to save in the Excel file,
            or the name for the CSV chunk.
        index (int): The chunk index, used to calculate the starting row
            for the data slice.

    Returns:
        str: The path to the temporary file where the chunk is saved.

    Raises:
        ValueError: If `file_path` does not have a recognized extension.
    """

    start_row = index * CHUNK_SIZE
    end_row = start_row + CHUNK_SIZE
    chunk_df = sheet_df.iloc[start_row:end_row]

    # Create a temporary file for the chunk securely
    temp_dir = tempfile.gettempdir()
    if file_path.endswith(".csv"):
        # Save each chunk to a new CSV file with the specified sheet name
        temp_file_path = f"{temp_dir}/{sheet_name}"
        chunk_df.to_csv(temp_file_path, index=False)
    else:
        # Create a temporary file for the chunk
        temp_file_path = f"{temp_dir}/{os.path.basename(file_path).replace('.', f'_{sheet_name}_part_{index + 1}.')}"
        # Save each chunk to a new Excel file with the specified sheet name
        with pd.ExcelWriter(  # pylint: disable=abstract-class-instantiated
            temp_file_path, engine="xlsxwriter"
        ) as writer:
            chunk_df.to_excel(writer, index=False, sheet_name=sheet_name)

    return temp_file_path


def break_down_file_and_upload(dry_run: bool, file_path: str, system: str) -> None:
    """
    Splits a CSV or Excel file into smaller chunks of 5000 rows each, saves each chunk
    as an Excel file with a specified sheet name (based on the CSV filename, if applicable),
    and uploads the chunks to Google Cloud Storage.

    Args:
        dry_run (bool): If True, skips the upload step.
        file_path (str): Local path to the input file.
        system (str): Optional system identifier for naming the destination path.
    """

    sftp_host = "sftp.justice-counts.org"
    sftp_port = 2022

    # Prompt for SFTP username securely
    sftp_username = input("Enter SFTP username: ")

    # Prompt for password securely
    sftp_password = getpass.getpass(prompt="Enter SFTP password: ")

    # Load file into a pandas DataFrame and determine sheet name
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path)
        sheets = {os.path.basename(file_path): df}
    elif file_path.endswith(".xlsx") or file_path.endswith(".xls"):
        xls = pd.ExcelFile(file_path)
        sheets = {
            sheet_name: pd.read_excel(xls, sheet_name) for sheet_name in xls.sheet_names
        }  # Load all sheets into a dict
    else:
        logger.error("Unsupported file type: %s", file_path)
        return

    # Set up SFTP connection
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # Disable host key checking for simplicity

    with pysftp.Connection(
        host=sftp_host,
        port=sftp_port,
        username=sftp_username,
        password=sftp_password,
        cnopts=cnopts,
    ) as sftp:
        for sheet_name, sheet_df in sheets.items():
            num_chunks = (len(sheet_df) // CHUNK_SIZE) + (
                1 if len(sheet_df) % CHUNK_SIZE > 0 else 0
            )
            logger.info(
                "Splitting data from sheet '%s' into %d chunks of %d rows each",
                sheet_name,
                num_chunks,
                CHUNK_SIZE,
            )

            for i in range(num_chunks):
                temp_file_path = get_file_chunk(
                    sheet_df=sheet_df,
                    sheet_name=sheet_name,
                    index=i,
                    file_path=file_path,
                )
                # Upload file if not a dry run
                if not dry_run:
                    remote_path = (
                        f"{system}/{os.path.basename(temp_file_path)}"
                        if system
                        else os.path.basename(temp_file_path)
                    )

                    # Upload file to SFTP server
                    logger.info(
                        "Uploading chunk %d of sheet '%s' to SFTP at path: %s",
                        i + 1,
                        sheet_name,
                        remote_path,
                    )
                    sftp.put(temp_file_path, remote_path)

                    # Wait 5 minutes between uploads if this isn't the last chunk
                    if i < num_chunks - 1:
                        logger.info(
                            "Waiting 5 minutes before uploading the next chunk...",
                        )
                        time.sleep(WAIT_TIME)

                else:
                    logger.info(
                        "Dry run: Skipping upload for chunk %d of sheet '%s'",
                        i + 1,
                        sheet_name,
                    )

        logger.info("File split and upload process completed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    break_down_file_and_upload(
        dry_run=args.dry_run,
        file_path=args.file_path,
        system=args.system,
    )
