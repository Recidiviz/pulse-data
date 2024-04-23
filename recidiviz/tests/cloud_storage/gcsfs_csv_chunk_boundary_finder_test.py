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
"""Tests for the accounting pass file"""
import unittest
from typing import List

from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import (
    DEFAULT_CSV_ENCODING,
    DEFAULT_CSV_LINE_TERMINATOR,
    CsvChunkBoundary,
    GcsfsCsvChunkBoundaryFinder,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest import fixtures

NORMALIZED_FILE = fixtures.as_filepath("normalized_file.csv")

NORMALIZED_FILE_NO_ENDING_NEWLINE = fixtures.as_filepath(
    "normalized_file_no_ending_newline.csv"
)

WINDOWS_FILE = fixtures.as_filepath("windows_file.csv")

WINDOWS_FILE_CUSTOM_NEWLINES = fixtures.as_filepath(
    "windows_file_with_custom_newlines.csv"
)

WINDOWS_FILE_MULIBYTE_NEWLINES = fixtures.as_filepath(
    "windows_file_with_multibyte_newlines.csv"
)

WINDOWS_FILE_CUSTOM_NEWLINES_CUSTOM_DELIM = fixtures.as_filepath(
    "windows_file_with_custom_newlines_custom_delim.csv"
)

EMPTY_FILE = fixtures.as_filepath("empty_file.csv")


class DirectIngestRawFileAccountingPassTest(unittest.TestCase):
    """Tests for the DirectIngestRawFileAccountingPass class."""

    def run_local_test(
        self,
        path: str,
        expected_boundaries: List[CsvChunkBoundary],
        expected_chunks: List[str],
        chunk_size: int = 20,
        peek_size: int = 5,
        encoding: str = DEFAULT_CSV_ENCODING,
        line_terminator: str = DEFAULT_CSV_LINE_TERMINATOR,
    ) -> None:
        fs = FakeGCSFileSystem()
        input_gcs_path = GcsfsFilePath.from_absolute_path("gs://my-bucket/input.csv")
        fs.test_add_path(path=input_gcs_path, local_path=path)

        finder = GcsfsCsvChunkBoundaryFinder(
            fs=fs,
            chunk_size=chunk_size,
            peek_size=peek_size,
            encoding=encoding,
            line_terminator=line_terminator,
        )
        actual_boundaries = finder.get_chunks_for_gcs_path(input_gcs_path)
        assert actual_boundaries == expected_boundaries

        with fs.open(input_gcs_path, "r", encoding=encoding) as f:
            actual_chunks = [
                f.read(b.end_exclusive - b.start_inclusive) for b in actual_boundaries
            ]

        assert actual_chunks == expected_chunks

    def test_simple_file(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=22, chunk_num=0),
            CsvChunkBoundary(start_inclusive=22, end_exclusive=45, chunk_num=1),
            CsvChunkBoundary(start_inclusive=45, end_exclusive=67, chunk_num=2),
            CsvChunkBoundary(start_inclusive=67, end_exclusive=90, chunk_num=3),
        ]
        expected_chunks = [
            '"col_1","col2","col3"\n',
            '"abc","it\'s easy","as"\n',
            '"123","as simple","as\n',
            '"do re mi","abc","123"\n',
        ]
        self.run_local_test(NORMALIZED_FILE, expected_boundaries, expected_chunks)

    def test_simple_file_no_ending_newline(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=22, chunk_num=0),
            CsvChunkBoundary(start_inclusive=22, end_exclusive=45, chunk_num=1),
            CsvChunkBoundary(start_inclusive=45, end_exclusive=67, chunk_num=2),
            CsvChunkBoundary(start_inclusive=67, end_exclusive=89, chunk_num=3),
        ]
        expected_chunks = [
            '"col_1","col2","col3"\n',
            '"abc","it\'s easy","as"\n',
            '"123","as simple","as\n',
            '"do re mi","abc","123"',
        ]
        self.run_local_test(
            NORMALIZED_FILE_NO_ENDING_NEWLINE, expected_boundaries, expected_chunks
        )

    def test_windows_encoded_file(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=28, chunk_num=0),
            CsvChunkBoundary(start_inclusive=28, end_exclusive=81, chunk_num=1),
            CsvChunkBoundary(start_inclusive=81, end_exclusive=93, chunk_num=2),
        ]
        expected_chunks = [
            "col1,col2,col3\nhello,its,me\n",
            "i was,wondering,if\nyou could, decode, the following:\n",
            "á,æ,Ö\nì,ÿ,÷\n",
        ]
        self.run_local_test(
            WINDOWS_FILE, expected_boundaries, expected_chunks, encoding="WINDOWS-1252"
        )

    def test_windows_file_custom_newline(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=28, chunk_num=0),
            CsvChunkBoundary(start_inclusive=28, end_exclusive=81, chunk_num=1),
            CsvChunkBoundary(start_inclusive=81, end_exclusive=93, chunk_num=2),
        ]
        expected_chunks = [
            "col1,col2,col3‡hello,its,me‡",
            "i was,wondering,if‡you could, decode, the following:‡",
            "á,æ,Ö‡ì,ÿ,÷‡",
        ]
        self.run_local_test(
            WINDOWS_FILE_CUSTOM_NEWLINES,
            expected_boundaries,
            expected_chunks,
            encoding="WINDOWS-1252",
            line_terminator="‡",
        )

    def test_windows_file_custom_newline_custom_delim(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=40, chunk_num=0),
            CsvChunkBoundary(start_inclusive=40, end_exclusive=96, chunk_num=1),
            CsvChunkBoundary(start_inclusive=96, end_exclusive=108, chunk_num=2),
        ]
        expected_chunks = [
            "col1†col2†col3‡hello,,,†its,,,,†me,,,,,‡",
            "i was†wondering†if‡you could†,,decode†,,,the following:‡",
            "á†æ†Ö‡ì†ÿ†÷‡",
        ]
        self.run_local_test(
            WINDOWS_FILE_CUSTOM_NEWLINES_CUSTOM_DELIM,
            expected_boundaries,
            expected_chunks,
            encoding="WINDOWS-1252",
            line_terminator="‡",
        )

    def test_windows_file_multibyte_newline(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=42, chunk_num=0),
            CsvChunkBoundary(start_inclusive=42, end_exclusive=100, chunk_num=1),
            CsvChunkBoundary(start_inclusive=100, end_exclusive=114, chunk_num=2),
        ]
        expected_chunks = [
            "col1†col2†col3‡\nhello,,,†its,,,,†me,,,,,‡\n",
            "i was†wondering†if‡\nyou could†,,decode†,,,the following:‡\n",
            "á†æ†Ö‡\nì†ÿ†÷‡\n",
        ]
        self.run_local_test(
            WINDOWS_FILE_MULIBYTE_NEWLINES,
            expected_boundaries,
            expected_chunks,
            encoding="WINDOWS-1252",
            line_terminator="‡\n",
        )

    def test_empty_file(self) -> None:
        expected_boundaries: List[CsvChunkBoundary] = []
        expected_chunks: List[str] = []
        self.run_local_test(EMPTY_FILE, expected_boundaries, expected_chunks)

    def test_simple_file_chunk_larger_than_file(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=90, chunk_num=0),
        ]
        expected_chunks = [
            '"col_1","col2","col3"\n'
            '"abc","it\'s easy","as"\n'
            '"123","as simple","as\n'
            '"do re mi","abc","123"\n'
        ]
        self.run_local_test(
            NORMALIZED_FILE,
            expected_boundaries,
            expected_chunks,
            chunk_size=100000000000,
        )

    def test_invalid_sizes(self) -> None:
        invalid_sizes = [0, -1, -100]
        fs = FakeGCSFileSystem()
        for invalid_size in invalid_sizes:
            with self.assertRaises(ValueError):
                _ = GcsfsCsvChunkBoundaryFinder(fs=fs, chunk_size=invalid_size)

        for invalid_size in invalid_sizes:
            with self.assertRaises(ValueError):
                _ = GcsfsCsvChunkBoundaryFinder(fs=fs, peek_size=invalid_size)

    def test_chunk_size_one(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=16, chunk_num=0),
            CsvChunkBoundary(start_inclusive=16, end_exclusive=42, chunk_num=1),
            CsvChunkBoundary(start_inclusive=42, end_exclusive=62, chunk_num=2),
            CsvChunkBoundary(start_inclusive=62, end_exclusive=100, chunk_num=3),
            CsvChunkBoundary(start_inclusive=100, end_exclusive=107, chunk_num=4),
            CsvChunkBoundary(start_inclusive=107, end_exclusive=114, chunk_num=5),
        ]
        expected_chunks = [
            "col1†col2†col3‡\n",
            "hello,,,†its,,,,†me,,,,,‡\n",
            "i was†wondering†if‡\n",
            "you could†,,decode†,,,the following:‡\n",
            "á†æ†Ö‡\n",
            "ì†ÿ†÷‡\n",
        ]
        self.run_local_test(
            WINDOWS_FILE_MULIBYTE_NEWLINES,
            expected_boundaries,
            expected_chunks,
            chunk_size=1,
            encoding="WINDOWS-1252",
            line_terminator="‡\n",
        )

    def test_simple_no_newlines_at_all_no_not_at_all(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=90, chunk_num=0),
        ]
        expected_chunks = [
            '"col_1","col2","col3"\n'
            '"abc","it\'s easy","as"\n'
            '"123","as simple","as\n'
            '"do re mi","abc","123"\n'
        ]
        self.run_local_test(
            NORMALIZED_FILE,
            expected_boundaries,
            expected_chunks,
            line_terminator="no_newlines_here",
        )
