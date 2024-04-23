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
"""Tests for encoding.py in the common module"""
from recidiviz.common.constants.encoding import (
    BIG_QUERY_FIXED_LENGTH_ENCODINGS,
    BIG_QUERY_VARIABLE_LENGTH_ENCODINGS,
    PYTHON_STANDARD_ENCODINGS_TO_BIG_QUERY_ENCODING,
)


def test_all_encodings_are_either_fixed_or_variable() -> None:
    for encoding in PYTHON_STANDARD_ENCODINGS_TO_BIG_QUERY_ENCODING.values():
        assert (
            encoding in BIG_QUERY_VARIABLE_LENGTH_ENCODINGS
            or encoding in BIG_QUERY_FIXED_LENGTH_ENCODINGS
        )
