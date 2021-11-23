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
from typing import Iterator, List


def get_rows_from_csv(csv_filename: str) -> Iterator[List[str]]:
    with open(csv_filename, mode="r", encoding="utf-8") as dropped_rows_file:
        dropped_rows_reader = csv.reader(dropped_rows_file)
        for dropped_row in dropped_rows_reader:
            yield dropped_row
