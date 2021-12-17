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
"""Processes files created when the CPU usage of a Dataflow job is profiled.
Consolidates the information of the profile files into a single file to be analyzed.

Run with the following command:

    python -m recidiviz.tools.process_dataflow_cpu_profiling \
     --path_to_files [PATH_TO_CPU_PROFILE_FILES]
"""
import argparse
import csv
import logging
import os
import pstats
import sys
from typing import List, Tuple, Union

from recidiviz.utils.params import str_to_bool

CLEANED_FILE_SUFFIX = "_CLEANED"
OUTPUT_FILE_NAME = "cpu_profile_output.csv"


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to call the desired function."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--path_to_files",
        dest="path_to_files",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--limit_to_recid_modules",
        dest="limit_to_recid_modules",
        type=str_to_bool,
        default=True,
    )

    return parser.parse_known_args(argv)


def write_cpu_stats_csv(
    cpu_profile_files_path: str, limit_to_recid_modules: bool
) -> None:
    """Processes the CPU profile stats and writes the output to a single CSV."""
    files = [
        os.path.join(cpu_profile_files_path, file_name)
        for file_name in os.listdir(f"{cpu_profile_files_path}/")
        if file_name != OUTPUT_FILE_NAME
    ]

    stats_dict = pstats.Stats(*files).stats  # type: ignore[attr-defined]

    if limit_to_recid_modules:
        stats_dict = {k: v for k, v in stats_dict.items() if "recidiviz" in k[0]}

    fn_calls: List[List[Union[str, float]]] = []

    for function_info, cpu_info in stats_dict.items():
        file_name, line_num, function = function_info
        _, num_calls, _, cum_time, _ = cpu_info
        avg_time = cum_time / num_calls
        row_items = [
            file_name,
            line_num,
            function,
            cum_time,
            num_calls,
            avg_time,
        ]
        fn_calls.append(row_items)

    # Sorts the rows by the cumulative CPU time in descending order
    sorted_rows = sorted(fn_calls, key=lambda x: x[3], reverse=True)

    table_header: List[Union[str, float]] = [
        "file_name",
        "line_num",
        "function",
        "cum_time",
        "num_calls",
        "avg_time",
    ]

    rows_for_file = [table_header] + sorted_rows

    output_file_path = os.path.join(cpu_profile_files_path, OUTPUT_FILE_NAME)

    with open(output_file_path, "w", encoding="utf-8") as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerows(rows_for_file)

    logging.info("Wrote output to file path: %s", output_file_path)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    profile_files_path = os.path.normpath(known_args.path_to_files)

    write_cpu_stats_csv(profile_files_path, known_args.limit_to_recid_modules)
