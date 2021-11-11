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
"""Tests for CsvNormalizing IO."""
import csv
import unittest
from typing import List, Optional

import pandas as pd

from recidiviz.cloud_storage.gcsfs_csv_reader import GcsfsCsvReader
from recidiviz.cloud_storage.gcsfs_csv_reader_delegates import (
    SimpleGcsfsCsvReaderDelegate,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.cloud_storage.read_only_csv_normalizing_stream import (
    ReadOnlyCsvNormalizingStream,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest import fixtures

NO_TRAILING_LINE_TERMINATOR_PATH = fixtures.as_filepath(
    "example_file_structure_windows_1252_no_trailing_newline.csv"
)

NO_TRAILING_LINE_TERMINATOR_EXPECTED_OUTPUT_PATH = fixtures.as_filepath(
    "example_file_structure_commas_no_trailing_newline.csv"
)

TRAILING_LINE_TERMINATOR_PATH = fixtures.as_filepath(
    "example_file_structure_windows_1252.csv"
)

TRAILING_LINE_TERMINATOR_EXPECTED_OUTPUT_PATH = fixtures.as_filepath(
    "example_file_structure_commas.csv"
)

EMPTY_FILE_PATH = fixtures.as_filepath("completely_empty.csv")
ONLY_A_NEWLINE_PATH = fixtures.as_filepath("only_a_newline.csv")
SINGLE_LINE_PATH = fixtures.as_filepath("columns_no_contents.csv")
SINGLE_LINE_EXPECTED_OUTPUT_PATH = fixtures.as_filepath(
    "columns_no_contents_quoted.csv"
)
SINGLE_LINE_NO_TRAILING_NEWLINE_PATH = fixtures.as_filepath(
    "columns_no_contents_no_trailing_newline.csv"
)
SINGLE_LINE_NO_TRAILING_NEWLINE_EXPECTED_OUTPUT_PATH = fixtures.as_filepath(
    "columns_no_contents_quoted_no_trailing_newline.csv"
)


class _FakeDfCapturingDelegate(SimpleGcsfsCsvReaderDelegate):
    def __init__(self) -> None:
        self.dfs: List[pd.DataFrame] = []

    def on_dataframe(self, encoding: str, chunk_num: int, df: pd.DataFrame) -> bool:
        self.dfs += [df]
        return True


class TestCsvNormalizingIO(unittest.TestCase):
    """Tests for CsvNormalizing IO."""

    def run_read_all_test(
        self,
        input_file_path: str,
        expected_result_path: str,
        encoding: str,
        delimiter: str,
        line_terminator: str,
    ) -> None:
        with open(input_file_path, encoding=encoding) as fp:
            preprocessed_fp = ReadOnlyCsvNormalizingStream(
                fp,
                delimiter=delimiter,
                line_terminator=line_terminator,
                quoting=csv.QUOTE_NONE,
            )
            contents = preprocessed_fp.read()

        with open(expected_result_path, encoding="utf-8") as fp_expected:
            expected_contents = fp_expected.read()

        self.assertEqual(expected_contents, contents)

    def run_read_lines_test(
        self,
        input_file_path: str,
        expected_result_path: str,
        encoding: str,
        delimiter: str,
        line_terminator: str,
        num_reads: int,
        max_read_length: Optional[int] = None,
    ) -> None:
        lines = []
        with open(input_file_path, encoding=encoding) as fp:
            preprocessed_fp = ReadOnlyCsvNormalizingStream(
                fp,
                delimiter=delimiter,
                line_terminator=line_terminator,
                quoting=csv.QUOTE_NONE,
            )
            for _ in range(num_reads):
                lines.append(preprocessed_fp.readline(max_read_length))

        expected_lines = []
        with open(expected_result_path, encoding="utf-8") as fp_expected:
            for _ in range(num_reads):
                expected_lines.append(fp_expected.readline(max_read_length or -1))

        self.assertEqual(
            expected_lines,
            lines,
            f"Mismatched read values for max_read_length={max_read_length}",
        )

    def run_read_iterataive_test(
        self,
        input_file_path: str,
        expected_result_path: str,
        block_size: int,
        encoding: str,
        delimiter: str,
        line_terminator: str,
    ) -> None:
        """Runs a test that iteratively reads chunks from a normalized stream and
        compares the output to expected contents.
        """
        contents = ""
        with open(input_file_path, encoding=encoding) as fp:
            preprocessed_fp = ReadOnlyCsvNormalizingStream(
                fp,
                delimiter=delimiter,
                line_terminator=line_terminator,
                quoting=csv.QUOTE_NONE,
            )
            next_block_must_be_empty = False
            while True:
                block_contents = preprocessed_fp.read(block_size)
                if not block_contents:
                    break
                if next_block_must_be_empty:
                    raise ValueError(f"Expected empty block, got [{block_contents}]")
                if len(block_contents) > block_size:
                    raise ValueError(
                        f"Returned block with length [{len(block_contents)}], expected length [{block_contents}]"
                    )
                if len(block_contents) < block_size:
                    next_block_must_be_empty = True

                contents += block_contents

        expected_contents = ""
        with open(expected_result_path, encoding="utf-8") as fp_expected:
            while True:
                block_contents = fp_expected.read(block_size)
                if not block_contents:
                    break
                expected_contents += block_contents

        self.assertEqual(
            expected_contents,
            contents,
            f"Found contents that differ for read length [{block_size}]",
        )

    def run_gcs_csv_reader_test(
        self,
        input_file_path: str,
        expected_result_path: str,
        encoding: str,
        delimiter: str,
        line_terminator: str,
    ) -> None:
        """Runs a test reads a normalized stream using the csv reader."""
        fake_fs = FakeGCSFileSystem()
        input_gcs_path = GcsfsFilePath.from_absolute_path("gs://my-bucket/input.csv")
        fake_fs.test_add_path(path=input_gcs_path, local_path=input_file_path)
        input_delegate = _FakeDfCapturingDelegate()
        csv_reader = GcsfsCsvReader(fake_fs)
        csv_reader.streaming_read(
            path=input_gcs_path,
            delegate=input_delegate,
            dtype=str,
            chunk_size=1,
            encodings_to_try=[encoding],
            sep=delimiter,
            lineterminator=line_terminator,
            quoting=csv.QUOTE_NONE,
            engine="python",
        )

        expected_gcs_path = GcsfsFilePath.from_absolute_path(
            "gs://my-bucket/expected.csv"
        )
        fake_fs.test_add_path(path=expected_gcs_path, local_path=expected_result_path)
        expected_delegate = _FakeDfCapturingDelegate()
        csv_reader.streaming_read(
            path=expected_gcs_path,
            delegate=expected_delegate,
            dtype=str,
            chunk_size=1,
            quoting=csv.QUOTE_ALL,
        )
        self.assertEqual(len(expected_delegate.dfs), len(input_delegate.dfs))
        for i, expected_df in enumerate(expected_delegate.dfs):
            expected_df.equals(input_delegate.dfs[i])

    def run_single_line_gcs_csv_reader_test(
        self,
        input_file_path: str,
        expected_result_path: str,
        encoding: str,
        delimiter: str,
        line_terminator: str,
    ) -> None:
        """Runs a test reads a single line of a normalized stream using the csv reader,
        mimicking the way we read the columns from each file.
        """
        fake_fs = FakeGCSFileSystem()
        input_gcs_path = GcsfsFilePath.from_absolute_path("gs://my-bucket/input.csv")
        fake_fs.test_add_path(path=input_gcs_path, local_path=input_file_path)
        input_delegate = _FakeDfCapturingDelegate()
        csv_reader = GcsfsCsvReader(fake_fs)
        csv_reader.streaming_read(
            path=input_gcs_path,
            dtype=str,
            delegate=input_delegate,
            chunk_size=1,
            encodings_to_try=[encoding],
            nrows=1,
            sep=delimiter,
            quoting=csv.QUOTE_NONE,
            lineterminator=line_terminator,
            engine="python",
        )

        expected_gcs_path = GcsfsFilePath.from_absolute_path(
            "gs://my-bucket/expected.csv"
        )
        fake_fs.test_add_path(path=expected_gcs_path, local_path=expected_result_path)
        expected_delegate = _FakeDfCapturingDelegate()
        csv_reader.streaming_read(
            path=expected_gcs_path,
            delegate=expected_delegate,
            dtype=str,
            chunk_size=1,
            nrows=1,
        )

        self.assertEqual(len(expected_delegate.dfs), len(input_delegate.dfs))
        for i, expected_df in enumerate(expected_delegate.dfs):
            expected_df.equals(input_delegate.dfs[i])

    def test_read_empty_file(self) -> None:
        self.run_read_all_test(
            input_file_path=EMPTY_FILE_PATH,
            expected_result_path=EMPTY_FILE_PATH,
            encoding="UTF-8",
            line_terminator="\n",
            delimiter=",",
        )

    def test_read_single_newline_file(self) -> None:
        self.run_read_all_test(
            input_file_path=ONLY_A_NEWLINE_PATH,
            expected_result_path=ONLY_A_NEWLINE_PATH,
            encoding="UTF-8",
            line_terminator="\n",
            delimiter=",",
        )

    def test_read_single_line_file(self) -> None:
        self.run_read_all_test(
            input_file_path=SINGLE_LINE_PATH,
            expected_result_path=SINGLE_LINE_EXPECTED_OUTPUT_PATH,
            encoding="UTF-8",
            line_terminator="\n",
            delimiter=",",
        )

    def test_read_single_line_file_no_trailing_newline(self) -> None:
        self.run_read_all_test(
            input_file_path=SINGLE_LINE_NO_TRAILING_NEWLINE_PATH,
            expected_result_path=SINGLE_LINE_NO_TRAILING_NEWLINE_EXPECTED_OUTPUT_PATH,
            encoding="UTF-8",
            line_terminator="\n",
            delimiter=",",
        )

    def test_read_whole_file_ending_in_line_terminator(self) -> None:
        self.run_read_all_test(
            input_file_path=TRAILING_LINE_TERMINATOR_PATH,
            expected_result_path=TRAILING_LINE_TERMINATOR_EXPECTED_OUTPUT_PATH,
            encoding="WINDOWS-1252",
            line_terminator="†",
            delimiter="‡",
        )

    def test_read_whole_file_ending_no_trailing_line_terminator(self) -> None:
        self.run_read_all_test(
            input_file_path=NO_TRAILING_LINE_TERMINATOR_PATH,
            expected_result_path=NO_TRAILING_LINE_TERMINATOR_EXPECTED_OUTPUT_PATH,
            encoding="WINDOWS-1252",
            line_terminator="†",
            delimiter="‡",
        )

    def test_read_line(self) -> None:
        self.run_read_lines_test(
            input_file_path=TRAILING_LINE_TERMINATOR_PATH,
            expected_result_path=TRAILING_LINE_TERMINATOR_EXPECTED_OUTPUT_PATH,
            encoding="WINDOWS-1252",
            line_terminator="†",
            delimiter="‡",
            num_reads=4,
        )

    def test_read_line_bounded(self) -> None:
        for i in range(1, 30):
            self.run_read_lines_test(
                input_file_path=TRAILING_LINE_TERMINATOR_PATH,
                expected_result_path=TRAILING_LINE_TERMINATOR_EXPECTED_OUTPUT_PATH,
                encoding="WINDOWS-1252",
                line_terminator="†",
                delimiter="‡",
                num_reads=50,
                max_read_length=i,
            )

    def test_read_iterative_file_ending_in_line_terminator(self) -> None:
        for i in range(1, 45):
            self.run_read_iterataive_test(
                input_file_path=TRAILING_LINE_TERMINATOR_PATH,
                expected_result_path=TRAILING_LINE_TERMINATOR_EXPECTED_OUTPUT_PATH,
                block_size=i,
                encoding="WINDOWS-1252",
                line_terminator="†",
                delimiter="‡",
            )

    def test_read_iterative_file_ending_no_trailing_line_terminator(self) -> None:
        for i in range(1, 45):
            self.run_read_iterataive_test(
                input_file_path=NO_TRAILING_LINE_TERMINATOR_PATH,
                expected_result_path=NO_TRAILING_LINE_TERMINATOR_EXPECTED_OUTPUT_PATH,
                block_size=i,
                encoding="WINDOWS-1252",
                line_terminator="†",
                delimiter="‡",
            )

    def test_read_dataframe_whole_file_ending_in_line_terminator(self) -> None:
        self.run_gcs_csv_reader_test(
            input_file_path=TRAILING_LINE_TERMINATOR_PATH,
            expected_result_path=TRAILING_LINE_TERMINATOR_EXPECTED_OUTPUT_PATH,
            encoding="WINDOWS-1252",
            line_terminator="†",
            delimiter="‡",
        )

    def test_read_dataframe_whole_file_ending_no_trailing_line_terminator(self) -> None:
        self.run_gcs_csv_reader_test(
            input_file_path=NO_TRAILING_LINE_TERMINATOR_PATH,
            expected_result_path=NO_TRAILING_LINE_TERMINATOR_EXPECTED_OUTPUT_PATH,
            encoding="WINDOWS-1252",
            line_terminator="†",
            delimiter="‡",
        )

    def test_read_dataframe_single_line(self) -> None:
        self.run_single_line_gcs_csv_reader_test(
            input_file_path=TRAILING_LINE_TERMINATOR_PATH,
            expected_result_path=TRAILING_LINE_TERMINATOR_EXPECTED_OUTPUT_PATH,
            encoding="WINDOWS-1252",
            line_terminator="†",
            delimiter="‡",
        )
