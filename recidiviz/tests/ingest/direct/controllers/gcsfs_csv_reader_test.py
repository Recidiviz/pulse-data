# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for the GcsfsCsvReader."""

import unittest
from typing import IO, List, Optional

import gcsfs
import pandas as pd
from mock import create_autospec

from recidiviz.ingest.direct.controllers.gcsfs_csv_reader import GcsfsCsvReader, GcsfsCsvReaderDelegate, \
    COMMON_RAW_FILE_ENCODINGS
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.tests.ingest import fixtures


class _TestGcsfsCsvReaderDelegate(GcsfsCsvReaderDelegate):
    """Test-only GcsfsCsvReaderDelegate for tracking work done in the reader."""

    def __init__(self) -> None:
        self.dataframes: List[pd.DataFrame] = []
        self.encodings_attempted: List[str] = []
        self.decode_errors = 0
        self.exceptions = 0
        self.successful_encoding: Optional[str] = None

    def on_start_read_with_encoding(self, encoding: str) -> None:
        self.encodings_attempted.append(encoding)

    def on_dataframe(self, encoding: str, chunk_num: int, df: pd.DataFrame) -> bool:
        self.dataframes.append((encoding, df))
        return True

    def on_unicode_decode_error(self, encoding: str, e: UnicodeError) -> bool:
        self.decode_errors += 1
        return False

    def on_exception(self, encoding: str, e: Exception) -> bool:
        self.exceptions += 1
        return True

    def on_file_read_success(self, encoding: str) -> None:
        if self.successful_encoding:
            raise ValueError('Should not set successful encoding twice.')
        self.successful_encoding = encoding


def _fake_gcsfs_open(
        path_str: str,
        *,
        encoding: str,
        # pylint: disable=unused-argument
        token: str) -> IO:
    if not path_str.startswith('gs://'):
        raise ValueError(f'Expected gs:// path URI, got this instead: {path_str}')

    # Convert to local absolute path
    return open('/' + path_str[len('gs://'):], encoding=encoding)


class GcsfsCsvReaderTest(unittest.TestCase):
    """Tests for the GcsfsCsvReader."""

    def setUp(self) -> None:

        self.mock_gcsfs = create_autospec(gcsfs.GCSFileSystem)
        self.mock_gcsfs.open = _fake_gcsfs_open
        self.reader = GcsfsCsvReader(self.mock_gcsfs)

    def _validate_empty_file_result(self, delegate: _TestGcsfsCsvReaderDelegate) -> None:
        self.assertEqual(1, len(delegate.encodings_attempted))
        self.assertEqual(delegate.encodings_attempted[0], delegate.successful_encoding)
        self.assertEqual(0, len(delegate.dataframes))
        self.assertEqual(0, delegate.decode_errors)
        self.assertEqual(0, delegate.exceptions)

    def test_read_completely_empty_file(self) -> None:
        empty_file_path = fixtures.as_filepath('tagA.csv')

        delegate = _TestGcsfsCsvReaderDelegate()
        self.reader.streaming_read(GcsfsFilePath.from_absolute_path(empty_file_path), delegate=delegate, chunk_size=1)
        self.assertEqual(1, len(delegate.encodings_attempted))
        self.assertEqual(delegate.encodings_attempted[0], delegate.successful_encoding)
        self.assertEqual(0, len(delegate.dataframes))
        self.assertEqual(0, delegate.decode_errors)
        self.assertEqual(0, delegate.exceptions)

        delegate = _TestGcsfsCsvReaderDelegate()
        self.reader.streaming_read(GcsfsFilePath.from_absolute_path(empty_file_path), delegate=delegate, chunk_size=10)
        self.assertEqual(1, len(delegate.encodings_attempted))
        self.assertEqual(delegate.encodings_attempted[0], delegate.successful_encoding)
        self.assertEqual(0, len(delegate.dataframes))
        self.assertEqual(0, delegate.decode_errors)
        self.assertEqual(0, delegate.exceptions)

    def test_read_file_with_columns_no_contents(self) -> None:
        empty_file_path = fixtures.as_filepath('tagB.csv')

        delegate = _TestGcsfsCsvReaderDelegate()
        self.reader.streaming_read(GcsfsFilePath.from_absolute_path(empty_file_path), delegate=delegate, chunk_size=1)
        self.assertEqual(1, len(delegate.encodings_attempted))
        self.assertEqual(delegate.encodings_attempted[0], delegate.successful_encoding)
        self.assertEqual(1, len(delegate.dataframes))
        encoding, df = delegate.dataframes[0]
        self.assertEqual(encoding, delegate.successful_encoding)
        self.assertEqual(0, df.shape[0])  # No rows
        self.assertEqual(7, df.shape[1])  # 7 columns
        self.assertEqual(0, delegate.decode_errors)
        self.assertEqual(0, delegate.exceptions)

        delegate = _TestGcsfsCsvReaderDelegate()
        self.reader.streaming_read(GcsfsFilePath.from_absolute_path(empty_file_path), delegate=delegate, chunk_size=10)
        self.assertEqual(1, len(delegate.encodings_attempted))
        self.assertEqual(delegate.encodings_attempted[0], delegate.successful_encoding)
        self.assertEqual(1, len(delegate.dataframes))
        encoding, df = delegate.dataframes[0]
        self.assertEqual(encoding, delegate.successful_encoding)
        self.assertEqual(0, df.shape[0])  # No rows
        self.assertEqual(7, df.shape[1])  # 7 columns
        self.assertEqual(0, delegate.decode_errors)
        self.assertEqual(0, delegate.exceptions)

    def test_read_no_encodings_match(self) -> None:
        file_path = fixtures.as_filepath('encoded_latin_1.csv')
        delegate = _TestGcsfsCsvReaderDelegate()
        encodings_to_try = ['UTF-8', 'UTF-16']
        with self.assertRaises(ValueError):
            self.reader.streaming_read(GcsfsFilePath.from_absolute_path(file_path),
                                       delegate=delegate, chunk_size=10, encodings_to_try=encodings_to_try)
        self.assertEqual(encodings_to_try, delegate.encodings_attempted)
        self.assertEqual(2, len(delegate.encodings_attempted))
        self.assertIsNone(delegate.successful_encoding)
        self.assertEqual(0, len(delegate.dataframes))
        self.assertEqual(2, delegate.decode_errors)
        self.assertEqual(0, delegate.exceptions)

    def test_read_with_failure_first(self) -> None:
        file_path = fixtures.as_filepath('encoded_latin_1.csv')
        delegate = _TestGcsfsCsvReaderDelegate()
        self.reader.streaming_read(GcsfsFilePath.from_absolute_path(file_path), delegate=delegate, chunk_size=1)

        index = COMMON_RAW_FILE_ENCODINGS.index('ISO-8859-1')
        self.assertEqual(index + 1, len(delegate.encodings_attempted))
        self.assertEqual(COMMON_RAW_FILE_ENCODINGS[:(index+1)], delegate.encodings_attempted)
        self.assertEqual('ISO-8859-1', delegate.successful_encoding)
        self.assertEqual(4, len(delegate.dataframes))
        self.assertEqual({'ISO-8859-1'}, {encoding for encoding, df in delegate.dataframes})
        self.assertEqual(1, delegate.decode_errors)
        self.assertEqual(0, delegate.exceptions)

    def test_read_with_no_failure(self) -> None:
        file_path = fixtures.as_filepath('encoded_utf_8.csv')
        delegate = _TestGcsfsCsvReaderDelegate()
        self.reader.streaming_read(GcsfsFilePath.from_absolute_path(file_path), delegate=delegate, chunk_size=1)

        self.assertEqual(1, len(delegate.encodings_attempted))
        self.assertEqual('UTF-8', delegate.encodings_attempted[0])
        self.assertEqual('UTF-8', delegate.successful_encoding)
        self.assertEqual(4, len(delegate.dataframes))
        self.assertEqual({'UTF-8'}, {encoding for encoding, df in delegate.dataframes})
        self.assertEqual(0, delegate.decode_errors)
        self.assertEqual(0, delegate.exceptions)

    def test_read_with_exception(self) -> None:
        class _TestException(ValueError):
            pass

        class _ExceptionDelegate(_TestGcsfsCsvReaderDelegate):
            def on_dataframe(self, encoding: str, chunk_num: int, df: pd.DataFrame) -> bool:
                should_continue = super().on_dataframe(encoding, chunk_num, df)
                if chunk_num > 0:
                    raise _TestException('We crashed processing!')
                return should_continue

        file_path = fixtures.as_filepath('encoded_utf_8.csv')
        delegate = _ExceptionDelegate()

        with self.assertRaises(_TestException):
            self.reader.streaming_read(GcsfsFilePath.from_absolute_path(file_path), delegate=delegate, chunk_size=1)

        self.assertEqual(1, len(delegate.encodings_attempted))
        self.assertEqual('UTF-8', delegate.encodings_attempted[0])
        self.assertIsNone(delegate.successful_encoding)
        self.assertEqual(2, len(delegate.dataframes))
        self.assertEqual({'UTF-8'}, {encoding for encoding, df in delegate.dataframes})
        self.assertEqual(0, delegate.decode_errors)
        self.assertEqual(1, delegate.exceptions)
