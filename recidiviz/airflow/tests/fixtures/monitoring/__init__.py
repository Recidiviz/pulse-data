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
"""Fixture container for monitoring tests"""
import csv
import os
from typing import Iterator


def read_csv_fixture(file: str) -> Iterator[dict]:
    with open(
        os.path.join(os.path.dirname(__file__), file), encoding="utf-8"
    ) as fixture_file:
        reader = csv.reader(fixture_file)
        try:
            header = next(reader)
        except StopIteration:
            return

        for row in reader:
            yield dict(zip(header, row))


def write_fixture(file: str, contents: str) -> None:
    with open(
        os.path.join(os.path.dirname(__file__), file), "w", encoding="utf-8"
    ) as fixture_file:
        fixture_file.write(contents)
