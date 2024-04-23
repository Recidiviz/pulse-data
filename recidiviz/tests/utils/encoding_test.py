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
"""Tests for the encoding.py file in the utils module."""
from typing import List

import pytest
from parameterized import parameterized

from recidiviz.utils.encoding import to_python_standard


@parameterized.expand(
    [
        [
            ["latin_1", "iso-8859-1,", "iso8859-1", "8859", "cp819", "latin", "latin1"],
            "iso8859-1",
        ],
        [["U8", "UTF", "utf8", "cp65001"], "utf-8"],
        [["windows-1252", "cp1252"], "cp1252"],
    ],
)
def test_encoding_translation_success(aliases: List[str], encoding: str) -> None:
    for alias in aliases:
        assert encoding == to_python_standard(alias)


def test_encoding_translation_fail() -> None:
    with pytest.raises(LookupError):
        to_python_standard("aaaaaaa")
