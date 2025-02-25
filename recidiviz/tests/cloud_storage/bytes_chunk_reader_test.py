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
"""Tests for bytes_chunk_reader.py"""
import unittest
from contextlib import contextmanager
from typing import IO, Iterator
from unittest.mock import create_autospec

from recidiviz.cloud_storage.bytes_chunk_reader import BytesChunkReader
from recidiviz.tests.ingest import fixtures

NORMALIZED_FILE = fixtures.as_filepath("normalized_file.csv")


class BytesChunkReaderTest(unittest.TestCase):
    """Tests for the BytesChunkReader class"""

    @contextmanager
    def make_reader(self, path: str, start: int, end: int) -> Iterator[IO]:
        with open(path, "rb") as f:
            yield BytesChunkReader(f, start, end)

    def test_read_start_lt_read_end(self) -> None:
        seekable_io = create_autospec(IO)
        seekable_io.seekable.return_value = True
        with self.assertRaisesRegex(
            ValueError,
            r"read_end_exclusive \[1\] must be greater than or equal to "
            r"read_start_inclusive \[10\]",
        ):

            _ = BytesChunkReader(seekable_io, 10, 1)

    def test_non_seekable_io(self) -> None:
        non_seekable_io = create_autospec(IO)
        non_seekable_io.seekable.return_value = False
        with self.assertRaisesRegex(
            ValueError, r"Cannot have an offset reader for io that is not seekable"
        ):

            _ = BytesChunkReader(non_seekable_io, 0, 10)

    def test_read_all_entire_file(self) -> None:
        expected = b'"col_1","col2","col3"\n"abc","it\'s easy","as"\n"123","as simple","as\n"do re mi","abc","123"\n'
        with self.make_reader(NORMALIZED_FILE, 0, 90) as r:
            assert r.read() == expected

    def test_read_all_offset_i(self) -> None:
        expected = b'col_1","col2","col3"\n"abc","it\'s easy","as"\n"123","as simple","as\n"do re mi","abc","123"'
        with self.make_reader(NORMALIZED_FILE, 1, 89) as r:
            assert r.read() == expected

    def test_read_all_offset_ii(self) -> None:
        expected = b'ol2","col3'
        with self.make_reader(NORMALIZED_FILE, 10, 20) as r:
            assert r.read() == expected

    def test_read_n_entire_file(self) -> None:
        expected_chunks = [
            b'"col_1","col2","col3"\n"abc","i',
            b't\'s easy","as"\n"123","as simpl',
            b'e","as\n"do re mi","abc","123"\n',
            b"",
            b"",
            b"",
        ]
        with self.make_reader(NORMALIZED_FILE, 0, 90) as r:
            for expected in expected_chunks:
                assert r.read(30) == expected

    def test_read_n_offset(self) -> None:
        expected_chunks = [
            b'col_1","col2","col3"\n"abc","it',
            b'\'s easy","as"\n"123","as simple',
            b'","as\n"do re mi","abc","123"',
            b"",
            b"",
            b"",
        ]
        with self.make_reader(NORMALIZED_FILE, 1, 89) as r:
            for expected in expected_chunks:
                assert r.read(30) == expected

    def test_read_n_equal(self) -> None:
        expected_chunks = [
            b"",
            b"",
        ]
        with self.make_reader(NORMALIZED_FILE, 1, 1) as r:
            for expected in expected_chunks:
                assert r.read(30) == expected

    def test_read_n_offset_reader_advanced_in_stream(self) -> None:
        expected_chunks = [
            b'col_1","col2","col3"\n"abc","it',
            b'\'s easy","as"\n"123","as simple',
            b'","as\n"do re mi","abc","123"',
            b"",
            b"",
            b"",
        ]

        with open(NORMALIZED_FILE, "rb") as f:
            f.seek(30)
            r = BytesChunkReader(f, 1, 89)
            for expected in expected_chunks:
                assert r.read(30) == expected

    def test_read_n_offset_reader_at_end(self) -> None:
        expected_chunks = [
            b'col_1","col2","col3"\n"abc","it',
            b'\'s easy","as"\n"123","as simple',
            b'","as\n"do re mi","abc","123"',
            b"",
            b"",
            b"",
        ]

        with open(NORMALIZED_FILE, "rb") as f:
            f.seek(0, 2)
            r = BytesChunkReader(f, 1, 89)
            for expected in expected_chunks:
                assert r.read(30) == expected
