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
"""Processes files created when the memory usage of a Dataflow job is profiled.
Consolidates the information of the profile files into a single CSV to be analyzed.

Run with the following command:

    python -m recidiviz.tools.process_dataflow_memory_profiling \
     --path_to_files [PATH_TO_MEMORY_PROFILE_FILES]
"""
import argparse
import csv
import logging
import os
import sys
from typing import List, Optional, Tuple

OUTPUT_FILE_NAME = "memory_profile_output.csv"


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to call the desired function."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--path_to_files",
        dest="path_to_files",
        type=str,
        required=True,
    )

    return parser.parse_known_args(argv)


def clean_files(memory_profile_files_path: str) -> List[List[str]]:
    """Cleans up memory profile text files so that they can be converted to CSVs.

    Takes lines that look like:

        15     20   0     2080   0   9909277 100 dict of
                                             recidiviz.common.attr_mixins.CachedAttributeInfo

    And puts them into a list like:
        [17_39_53,9103411181572081227,15,20,0,2080,0,9909277,100,dict ofrecidiviz.common.attr_mixins.CachedAttributeInfo]

    Where the first two elements are the timestamp of the profile file and the worker ID
    of the Dataflow worker that wrote the memory profile file.

    Returns a list of a list of strings to be converted into a single CSV.
    """
    files = [
        os.path.join(memory_profile_files_path, file_name)
        for file_name in os.listdir(f"{memory_profile_files_path}/")
        if file_name != OUTPUT_FILE_NAME
    ]

    additional_header_columns = ["timestamp", "worker_id"]
    table_header: Optional[List[str]] = None
    processed_lines: List[List[str]] = []

    for filePath in files:
        with open(  # pylint: disable=unspecified-encoding
            filePath, "r"
        ) as file_to_read:
            file_timestamp = filePath.split("/")[-1][11:19]
            worker_id = filePath.split("/")[-1][35:].split("-")[0]
            file_in_lines = file_to_read.readlines()

            for i, line in enumerate(file_in_lines):
                updated_line = line.lstrip().strip()
                line_columns = updated_line.split(maxsplit=7)

                if not table_header and i == 1:
                    # Save the table header for the output file
                    table_header = additional_header_columns + line_columns

                # Don't process the header rows
                if i < 2:
                    continue

                # Skip header and other message output rows inside of the file
                if updated_line.startswith("<") or updated_line.startswith("Index"):
                    continue

                if len(line_columns) > 1:
                    processed_lines.append([file_timestamp, worker_id] + line_columns)
                else:
                    existing_line = processed_lines.pop()
                    existing_line[-1] = existing_line[-1] + line_columns[0]

                    processed_lines.append(existing_line)

    if not table_header:
        raise ValueError(
            "Unexpected empty table header. Should have been taken from one of the "
            "files."
        )

    return [table_header] + processed_lines


def write_to_csv(memory_profile_files_path: str, file_rows: List[List[str]]) -> None:
    """Writes the processed file rows into a single CSV."""
    output_file_path = os.path.join(memory_profile_files_path, OUTPUT_FILE_NAME)

    with open(output_file_path, "w", encoding="utf-8") as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerows(file_rows)

    logging.info("Wrote output to file path: %s", output_file_path)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    profile_files_path = os.path.normpath(known_args.path_to_files)

    processed_file_rows = clean_files(profile_files_path)
    write_to_csv(profile_files_path, processed_file_rows)
