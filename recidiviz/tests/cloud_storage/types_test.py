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


class TestCsvChunkBoundary(unittest.TestCase):
    """Test serialization/deserialization for CsvChunkBoundary"""

    def test_serialize_deserialize(self) -> None:
        original = CsvChunkBoundary(start_inclusive=0, end_exclusive=100, chunk_num=1)

        serialized = original.serialize()
        deserialized = CsvChunkBoundary.deserialize(serialized)

        self.assertEqual(original.start_inclusive, deserialized.start_inclusive)
        self.assertEqual(original.end_exclusive, deserialized.end_exclusive)
        self.assertEqual(original.chunk_num, deserialized.chunk_num)

    def test_get_chunk_size(self) -> None:
        boundary = CsvChunkBoundary(start_inclusive=0, end_exclusive=100, chunk_num=1)
        self.assertEqual(boundary.get_chunk_size(), 100)
