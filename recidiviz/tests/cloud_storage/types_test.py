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
"""Tests for the types in the cloud_storage module"""

import unittest

from recidiviz.cloud_storage.types import CsvChunkBoundary


class TestChunkBoundary(unittest.TestCase):
    """Tests for CsvChunkBoundary"""

    def test_serialization(self) -> None:
        chunk_boundary = CsvChunkBoundary(
            start_inclusive=0, end_exclusive=100, chunk_num=1
        )

        assert chunk_boundary == CsvChunkBoundary.deserialize(
            chunk_boundary.serialize()
        )
