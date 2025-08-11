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
"""Tests for the boundary finder file"""
import csv
import re
import unittest
from typing import List, Optional

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import (
    GcsfsCsvChunkBoundaryFinder,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.cloud_storage.types import CsvChunkBoundary
from recidiviz.common.constants.csv import (
    CARRIAGE_RETURN,
    DEFAULT_CSV_ENCODING,
    DEFAULT_CSV_LINE_TERMINATOR,
    DEFAULT_CSV_QUOTE_CHAR,
    DEFAULT_CSV_SEPARATOR,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest import fixtures

NORMALIZED_FILE = "normalized_file.csv"

NORMALIZED_FILE_NO_ENDING_NEWLINE = "normalized_file_no_ending_newline.csv"

WINDOWS_FILE = "windows_file.csv"

WINDOWS_FILE_CUSTOM_NEWLINES = "windows_file_with_custom_newlines.csv"

WINDOWS_FILE_MULIBYTE_NEWLINES = "windows_file_with_multibyte_newlines.csv"

WINDOWS_FILE_CUSTOM_NEWLINES_CUSTOM_DELIM = (
    "windows_file_with_custom_newlines_custom_delim.csv"
)

QUOTED_NEWLINE = "example_file_structure_commas.csv"

QUOTED_CARRIAGE_NEWLINE = "example_file_structure_commas_carriage_return.csv"

LINE_TERMINATOR_CONTAINS_NEWLINE = "custom_line_terminator_contains_new_line.csv"

QUOTED_NEWLINE_NO_ENDING_NEWLINE = (
    "example_file_structure_commas_no_trailing_newline.csv"
)

NOT_QUOTED = "encoded_utf_8.csv"

EMPTY_FILE = "empty_file.csv"


class GcsfsCsvChunkBoundaryFinderChunksForPathTests(unittest.TestCase):
    """Tests for the GcsfsCsvChunkBoundaryFinder class that check the correctness of
    GcsfsCsvChunkBoundaryFinder.get_chunks_for_gcs_path
    """

    @staticmethod
    def _get_finder(
        *,
        fs: Optional[GCSFileSystem] = None,
        chunk_size: int = 20,
        peek_size: int = 5,
        encoding: str = DEFAULT_CSV_ENCODING,
        line_terminator: str = DEFAULT_CSV_LINE_TERMINATOR,
        separator: str = DEFAULT_CSV_SEPARATOR,
        quoting_mode: int = csv.QUOTE_NONE,
        quote_char: str = DEFAULT_CSV_QUOTE_CHAR,
        max_cell_size: Optional[int] = None,
    ) -> GcsfsCsvChunkBoundaryFinder:
        fs = fs if fs is not None else FakeGCSFileSystem()
        return GcsfsCsvChunkBoundaryFinder(
            fs=fs,
            chunk_size=chunk_size,
            peek_size=peek_size,
            encoding=encoding,
            separator=separator,
            line_terminator=line_terminator,
            quoting_mode=quoting_mode,
            max_cell_size=max_cell_size,
            quote_char=quote_char,
        )

    def run_local_test(
        self,
        path: str,
        expected_boundaries: List[CsvChunkBoundary],
        expected_chunks: List[str] | List[bytes],
        *,
        chunk_size: int = 20,
        peek_size: int = 5,
        encoding: str = DEFAULT_CSV_ENCODING,
        line_terminator: str = DEFAULT_CSV_LINE_TERMINATOR,
        separator: str = DEFAULT_CSV_SEPARATOR,
        quoting_mode: int = csv.QUOTE_NONE,
        max_cell_size: Optional[int] = None,
    ) -> None:
        """Executes a test for GcsfsCsvChunkBoundaryFinder against a local file"""
        fs = FakeGCSFileSystem()
        input_gcs_path = GcsfsFilePath.from_absolute_path("gs://my-bucket/input.csv")
        fs.test_add_path(path=input_gcs_path, local_path=fixtures.as_filepath(path))

        finder = self._get_finder(
            fs=fs,
            chunk_size=chunk_size,
            peek_size=peek_size,
            encoding=encoding,
            separator=separator,
            line_terminator=line_terminator,
            quoting_mode=quoting_mode,
            max_cell_size=max_cell_size,
        )
        actual_boundaries = finder.get_chunks_for_gcs_path(input_gcs_path)

        assert actual_boundaries == expected_boundaries

        check_output_in_bytes = len(expected_chunks) > 0 and isinstance(
            expected_chunks[0], bytes
        )
        if check_output_in_bytes:
            mode, open_encoding = "rb", None
        else:
            mode, open_encoding = "r", encoding

        with fs.open(input_gcs_path, mode=mode, encoding=open_encoding) as f:
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

    def test_invalid_params(self) -> None:
        invalid_sizes = [0, -1, -100]
        for invalid_size in invalid_sizes:
            with self.assertRaisesRegex(
                ValueError, "chunk_size must be at least 1 byte"
            ):
                self._get_finder(chunk_size=invalid_size)

        for invalid_size in invalid_sizes:
            with self.assertRaisesRegex(
                ValueError, "peek_size must be at least 1 byte"
            ):
                self._get_finder(peek_size=invalid_size)

        for invalid_size in invalid_sizes:
            with self.assertRaisesRegex(
                ValueError, "max_cell_size must be at least 1 byte"
            ):
                self._get_finder(max_cell_size=invalid_size)

        with self.assertRaisesRegex(
            ValueError, "chunk_size should never be smaller than peek_size"
        ):
            self._get_finder(peek_size=10, chunk_size=5)

        for invalid_quote_char in ["/", "''", "‘"]:
            with self.assertRaises(ValueError):
                self._get_finder(quote_char=invalid_quote_char)

    # pylint: disable=protected-access
    def test_valid_params(self) -> None:
        finder = self._get_finder(max_cell_size=10)
        assert finder._max_read_size == 20

        finder = self._get_finder(line_terminator=DEFAULT_CSV_LINE_TERMINATOR)
        assert finder._line_terminators == [
            DEFAULT_CSV_LINE_TERMINATOR.encode(),
            CARRIAGE_RETURN.encode(),
        ]

        finder = self._get_finder(line_terminator=CARRIAGE_RETURN)
        assert finder._line_terminators == [CARRIAGE_RETURN.encode()]

        finder = self._get_finder(line_terminator="‡\n")
        assert finder._line_terminators == ["‡\n".encode(), "‡\r\n".encode()]

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
            peek_size=1,
            encoding="WINDOWS-1252",
            line_terminator="‡\n",
        )

    def test_quoted_newlines_simple(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=79, chunk_num=0),
            CsvChunkBoundary(start_inclusive=79, end_exclusive=169, chunk_num=1),
        ]
        expected_chunks = [
            '"numerical_col","string_col_1","string_col_2"\n"1234","Hello, world","I\'m Anna"\n',
            '"4567","This line\nis split in two","This word is ""quoted"""\n"7890","""quoted value""",""\n',
        ]
        self.run_local_test(
            QUOTED_NEWLINE,
            expected_boundaries,
            expected_chunks,
            chunk_size=75,
            peek_size=40,
            encoding="UTF-8",
            quoting_mode=csv.QUOTE_MINIMAL,
        )

    def test_quoted_newlines_on_border(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=140, chunk_num=0),
            CsvChunkBoundary(start_inclusive=140, end_exclusive=169, chunk_num=1),
        ]
        expected_chunks = [
            '"numerical_col","string_col_1","string_col_2"\n"1234","Hello, world","I\'m Anna"\n"4567","This line\nis split in two","This word is ""quoted"""\n',
            '"7890","""quoted value""",""\n',
        ]
        self.run_local_test(
            QUOTED_NEWLINE,
            expected_boundaries,
            expected_chunks,
            chunk_size=79,
            peek_size=40,
            encoding="UTF-8",
            quoting_mode=csv.QUOTE_MINIMAL,
        )

    def test_quoted_newlines_one_line_each(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=46, chunk_num=0),
            CsvChunkBoundary(start_inclusive=46, end_exclusive=79, chunk_num=1),
            CsvChunkBoundary(start_inclusive=79, end_exclusive=140, chunk_num=2),
            CsvChunkBoundary(start_inclusive=140, end_exclusive=169, chunk_num=3),
        ]
        expected_chunks = [
            '"numerical_col","string_col_1","string_col_2"\n',
            '"1234","Hello, world","I\'m Anna"\n',
            '"4567","This line\nis split in two","This word is ""quoted"""\n',
            '"7890","""quoted value""",""\n',
        ]
        self.run_local_test(
            QUOTED_NEWLINE,
            expected_boundaries,
            expected_chunks,
            chunk_size=1,
            peek_size=1,
            encoding="UTF-8",
            quoting_mode=csv.QUOTE_MINIMAL,
        )

    def test_quoted_newlines_one_line_each_carriage(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=71, chunk_num=0),
            CsvChunkBoundary(start_inclusive=71, end_exclusive=132, chunk_num=1),
            CsvChunkBoundary(start_inclusive=132, end_exclusive=158, chunk_num=2),
        ]
        expected_chunks = [
            b'numerical_col,string_col_1,string_col_2\r\n1234,"Hello, world",I\'m Anna\r\n',
            b'4567,"This line\r\nis split in two","This word is ""quoted"""\r\n',
            b'7890,"""quoted value""",\r\n',
        ]
        self.run_local_test(
            QUOTED_CARRIAGE_NEWLINE,
            expected_boundaries,
            expected_chunks,
            chunk_size=50,
            peek_size=10,
            encoding="UTF-8",
            quoting_mode=csv.QUOTE_MINIMAL,
            line_terminator=CARRIAGE_RETURN,
        )

        # okay if re run it again but this time dont specify the carriage return we
        # should have the same outcome -- the case this is really catching is the
        #       "quoted field"\r\n
        # case if we didnt automatically look for carriage returns, we'd classify this
        # as invalid as we'd parse it as d"\r which is invalid
        self.run_local_test(
            QUOTED_CARRIAGE_NEWLINE,
            expected_boundaries,
            expected_chunks,
            chunk_size=50,
            peek_size=10,
            encoding="UTF-8",
            quoting_mode=csv.QUOTE_MINIMAL,
        )

    def test_custom_line_terminator_contains_newline(self) -> None:
        """This file uses custom line terminator "‡\n" but the file contains "‡\r\n" line terminators."""
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=77, chunk_num=0),
            CsvChunkBoundary(start_inclusive=77, end_exclusive=141, chunk_num=1),
            CsvChunkBoundary(start_inclusive=141, end_exclusive=170, chunk_num=2),
        ]
        expected_chunks = [
            b'numerical_col,string_col_1,string_col_2\xe2\x80\xa1\r\n1234,"Hello, world",I\'m Anna\xe2\x80\xa1\r\n',
            b'4567,"This line\r\nis split in two","This word is ""quoted"""\xe2\x80\xa1\r\n',
            b'7890,"""quoted value""",\xe2\x80\xa1\r\n',
        ]
        self.run_local_test(
            LINE_TERMINATOR_CONTAINS_NEWLINE,
            expected_boundaries,
            expected_chunks,
            chunk_size=50,
            peek_size=10,
            encoding="UTF-8",
            quoting_mode=csv.QUOTE_MINIMAL,
            line_terminator="‡\n",
        )

    def test_quoted_newlines_no_ending_newline_one_line_each(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=46, chunk_num=0),
            CsvChunkBoundary(start_inclusive=46, end_exclusive=79, chunk_num=1),
            CsvChunkBoundary(start_inclusive=79, end_exclusive=139, chunk_num=2),
        ]
        expected_chunks = [
            '"numerical_col","string_col_1","string_col_2"\n',
            '"1234","Hello, world","I\'m Anna"\n',
            '"4567","This line\nis split in two","This word is ""quoted"""',
        ]
        self.run_local_test(
            QUOTED_NEWLINE_NO_ENDING_NEWLINE,
            expected_boundaries,
            expected_chunks,
            chunk_size=1,
            peek_size=1,
            encoding="UTF-8",
            quoting_mode=csv.QUOTE_MINIMAL,
        )

    def test_quoted_mode_on_no_quotes_so_we_guess(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=52, chunk_num=0)
        ]
        expected_chunks = ["symbol,name\n?,question mark\n+,plus\n\x80,euro\n£,pound\n"]
        self.run_local_test(
            NOT_QUOTED,
            expected_boundaries,
            expected_chunks,
            chunk_size=10,
            peek_size=10,
            encoding="UTF-8",
            quoting_mode=csv.QUOTE_MINIMAL,
            max_cell_size=500,
        )

        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=12, chunk_num=0),
            CsvChunkBoundary(start_inclusive=12, end_exclusive=28, chunk_num=1),
            CsvChunkBoundary(start_inclusive=28, end_exclusive=43, chunk_num=2),
            CsvChunkBoundary(start_inclusive=43, end_exclusive=52, chunk_num=3),
        ]
        new_expected_chunks = [
            b"symbol,name\n",
            b"?,question mark\n",
            b"+,plus\n\xc2\x80,euro\n",
            b"\xc2\xa3,pound\n",
        ]
        self.run_local_test(
            NOT_QUOTED,
            expected_boundaries,
            new_expected_chunks,
            chunk_size=10,
            peek_size=10,
            encoding="UTF-8",
            quoting_mode=csv.QUOTE_MINIMAL,
            max_cell_size=5,
        )

    def test_quoted_newlines_cell_size_too_small_so_we_make_a_mistake(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=46, chunk_num=0),
            CsvChunkBoundary(start_inclusive=46, end_exclusive=79, chunk_num=1),
            CsvChunkBoundary(start_inclusive=79, end_exclusive=97, chunk_num=2),
            CsvChunkBoundary(start_inclusive=97, end_exclusive=139, chunk_num=3),
        ]
        # this is a case where max_cell_size is not actually accurate
        # "4567","This line\nis split in two","This word is ""quoted"""', is longer than
        # 20 chars so we fail to split it.
        expected_chunks = [
            '"numerical_col","string_col_1","string_col_2"\n',
            '"1234","Hello, world","I\'m Anna"\n',
            '"4567","This line\n',
            'is split in two","This word is ""quoted"""',
        ]
        self.run_local_test(
            QUOTED_NEWLINE_NO_ENDING_NEWLINE,
            expected_boundaries,
            expected_chunks,
            chunk_size=10,
            peek_size=10,
            encoding="UTF-8",
            quoting_mode=csv.QUOTE_MINIMAL,
            max_cell_size=20,
        )

    def test_simple_no_newlines_at_all_no_not_at_all(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=90, chunk_num=0)
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

    def test_no_new_lines(self) -> None:
        expected_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=90, chunk_num=0)
        ]
        expected_chunks = [
            '"col_1","col2","col3"\n'
            '"abc","it\'s easy","as"\n'
            '"123","as simple","as\n'
            '"do re mi","abc","123"\n'
        ]
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Could not find line terminator between bytes offset [20] and [60] without exceeding our max_read_size of [40] for [gs://my-bucket/input.csv]. Please ensure that the encoding [utf-8], separator [b','], line terminator [b'no_newlines_here'] quote char [b'\"'] and quoting mode [3] are accurate for this file. If they are, consider increasing the [max_cell_size] for this file."
            ),
        ):
            self.run_local_test(
                NORMALIZED_FILE,
                expected_boundaries,
                expected_chunks,
                line_terminator="no_newlines_here",
                max_cell_size=20,
            )


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
