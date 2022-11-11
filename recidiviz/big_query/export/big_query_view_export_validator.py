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

"""Interface which validates the results of BigQuery view exports in specific formats."""

import abc
import logging
from typing import Optional

import pandas as pd

from recidiviz.big_query.export.export_query_config import ExportOutputFormatType
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_csv_reader import GcsfsCsvReader
from recidiviz.cloud_storage.gcsfs_csv_reader_delegates import (
    SimpleGcsfsCsvReaderDelegate,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


class BigQueryViewExportValidator:
    """Interface for implementations which validate BigQuery view export results in specific formats."""

    def __init__(self, fs: GCSFileSystem):
        self.fs = fs

    def __repr__(self) -> str:
        return self.__class__.__name__

    @abc.abstractmethod
    def supports_output_type(self, output_type: ExportOutputFormatType) -> bool:
        """Whether or not this export validator supports the given output type"""

    @abc.abstractmethod
    def validate(self, path: GcsfsFilePath, allow_empty: bool) -> bool:
        """Validates whether the exported results are valid for presentation.

        Allow empty specifies whether an empty file is still valid."""


class ExistsBigQueryViewExportValidator(BigQueryViewExportValidator):
    """View validator which validates that a path exists. If allow_empty is False, also verifies
    that the file is not empty."""

    def supports_output_type(self, output_type: ExportOutputFormatType) -> bool:
        return True

    def validate(self, path: GcsfsFilePath, allow_empty: bool) -> bool:
        file_size = self.fs.get_file_size(path) or 0
        return self.fs.exists(path) and (allow_empty or file_size > 0)


class NonEmptyColumnsBigQueryViewExportValidator(BigQueryViewExportValidator):
    """View validator which validates that no column is composed entirely of empty values. The file
    may be empty if allow_empty is true."""

    class NonEmptyColumnsGcsfsCsvReaderDelegate(SimpleGcsfsCsvReaderDelegate):
        def __init__(self) -> None:
            self.nonnull_value_counts_by_col: Optional[pd.Series] = None

        def on_dataframe(self, encoding: str, chunk_num: int, df: pd.DataFrame) -> bool:
            # Note: the "is None" check is required here (vs "if self.counts:"), otherwise we see
            # "ValueError: The truth value of a Series is ambiguous."
            if self.nonnull_value_counts_by_col is None:
                self.nonnull_value_counts_by_col = df.count()
            else:
                self.nonnull_value_counts_by_col += df.count()

            # If all values are non-empty, no need to continue reading
            return not self.nonnull_value_counts_by_col.all()

    def __init__(self, fs: GCSFileSystem, contains_headers: bool):
        super().__init__(fs)
        self.contains_headers = contains_headers

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(contains_headers={self.contains_headers})"

    def supports_output_type(self, output_type: ExportOutputFormatType) -> bool:
        return (
            output_type == ExportOutputFormatType.CSV and self.contains_headers
        ) or (
            output_type
            in [
                ExportOutputFormatType.HEADERLESS_CSV,
                ExportOutputFormatType.HEADERLESS_CSV_WITH_METADATA,
            ]
            and not self.contains_headers
        )

    def validate(self, path: GcsfsFilePath, allow_empty: bool) -> bool:
        csv_reader = GcsfsCsvReader(self.fs)
        delegate = self.NonEmptyColumnsGcsfsCsvReaderDelegate()
        header = 0 if self.contains_headers else None
        csv_reader.streaming_read(path, delegate, chunk_size=1000, header=header)

        if delegate.nonnull_value_counts_by_col is None:
            logging.warning("Found empty exported file: %s", path)
            return allow_empty
        # If .all() returns true, that means that every column is nonzero
        return delegate.nonnull_value_counts_by_col.all()
