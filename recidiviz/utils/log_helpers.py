# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Helpers for writing log files from ingest tools"""

import datetime
import difflib
import logging
import os

import recidiviz

RECIDIVIZ_ROOT = os.path.abspath(os.path.join(recidiviz.__file__, "../.."))

# This directory is in the .gitignore so these files won't get committed
LOG_OUTPUT_DIR = "logs/"

LOGGING_WIDTH = 80
FILL_CHAR = "#"


def write_html_diff_to_file(
    expected_output_filepath: str, actual_output_filepath: str, region_code: str
) -> str:
    """Diffs two files at the provided filepaths and renders this diff to an HTML file
    which can be viewed in a browser. The |region_code| is used in the filename.
    """
    html_filepath = make_log_output_path(
        operation_name="diff_from_controller_test",
        region_code=region_code,
        extension=".html",
    )

    with open(
        expected_output_filepath,
        encoding="utf-8",
    ) as expected_output_file, open(
        actual_output_filepath,
        encoding="utf-8",
    ) as actual_output_file:
        diff = difflib.HtmlDiff()
        html_file_as_string = diff.make_file(
            expected_output_file.readlines(),
            actual_output_file.readlines(),
            "Expected output",
            "Actual output",
        )

    with open(html_filepath, "w", encoding="utf-8") as html_file:
        html_file.writelines(html_file_as_string)

    return html_filepath


def make_log_output_path(
    operation_name: str,
    region_code: str,
    date_string: str = "",
    dry_run: bool = False,
    extension: str = ".txt",
) -> str:
    if date_string:
        file_name = (
            f"{operation_name}_result_{region_code}_{date_string}_dry_run_{dry_run}_"
        )
    else:
        file_name = f"{operation_name}_result_{region_code}_dry_run_{dry_run}_"
    file_name = file_name + f"{datetime.datetime.now().isoformat()}{extension}"

    folder = os.path.join(RECIDIVIZ_ROOT, LOG_OUTPUT_DIR)
    if not os.path.isdir(folder):
        os.mkdir(folder)
    return os.path.join(folder, file_name)


def log_info_with_fill(
    s: str, logging_width: int = LOGGING_WIDTH, fill_char: str = FILL_CHAR
) -> None:
    logging.info(s.center(logging_width, fill_char))
