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
"""Utilities for working with CSV files."""
import csv
from typing import Dict, Iterator, List, Tuple


def get_csv_columns(csv_filename: str) -> List[str]:
    """Returns the list of columns in the provided CSV, assuming the columns are
    the first row of the file.
    """
    with open(csv_filename, mode="r", encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        if csv_reader.fieldnames is None:
            raise ValueError(f"Found fieldnames is None for [{csv_filename}]")
        return list(csv_reader.fieldnames)


def get_rows_as_key_value_pairs(csv_filename: str) -> Iterator[Dict[str, str]]:
    """Generator that reads a CSV file and yields a key/value dictionary for each row the file."""
    with open(csv_filename, mode="r", encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            yield row


def get_rows_as_tuples(
    csv_filename: str, skip_header_row: bool = True
) -> Iterator[Tuple[str, ...]]:
    """Generator that reads a CSV file and yields a list of tuples for each row in the file. This utility skips the
    header row by default with the skip_header_row parameter."""
    with open(csv_filename, mode="r", encoding="utf-8") as csv_file:
        csv_reader = csv.reader(csv_file)
        if skip_header_row:
            try:
                next(csv_reader)
            except StopIteration:
                return
        for row in csv_reader:
            yield tuple(row)
