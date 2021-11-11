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
"""Shared implementations of the GcsfsCsvReaderDelegate."""

import abc
import csv
import logging
from typing import List, Optional, Tuple

import pandas as pd

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_csv_reader import GcsfsCsvReaderDelegate
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


class SimpleGcsfsCsvReaderDelegate(GcsfsCsvReaderDelegate):
    """A simple, base implementation of the GcsfsCsvReaderDelegate that allows the GcsfsCsvReader to cycle through all
    CSV chunks until we find a valid encoding, but does nothing with the data.
    """

    def on_start_read_with_encoding(self, encoding: str) -> None:
        pass

    def on_file_stream_normalization(
        self, old_encoding: str, new_encoding: str
    ) -> None:
        pass

    def on_dataframe(self, encoding: str, chunk_num: int, df: pd.DataFrame) -> bool:
        return True

    def on_unicode_decode_error(self, encoding: str, e: UnicodeError) -> bool:
        return False

    def on_exception(self, encoding: str, e: Exception) -> bool:
        return True

    def on_file_read_success(self, encoding: str) -> None:
        pass


class ReadOneGcsfsCsvReaderDelegate(SimpleGcsfsCsvReaderDelegate):
    """An implementation of the GcsfsCsvReaderDelegate that reads and stores at most one data chunk."""

    def __init__(self) -> None:
        self.df: Optional[pd.DataFrame] = None

    def on_dataframe(self, encoding: str, chunk_num: int, df: pd.DataFrame) -> bool:
        if self.df is not None:
            raise ValueError("Expected only one DataFrame chunk, found multiple.")
        self.df = df

        # Stop iteration after this one chunk
        return False


class SplittingGcsfsCsvReaderDelegate(GcsfsCsvReaderDelegate):
    """An implementation of the GcsfsCsvReaderDelegate that uploads each CSV chunk to a separate Google Cloud Storage
    path.
    """

    def __init__(self, path: GcsfsFilePath, fs: GCSFileSystem, include_header: bool):
        self.path = path
        self.fs = fs
        self.include_header = include_header

        self.output_paths_with_columns: List[Tuple[GcsfsFilePath, List[str]]] = []

    def on_start_read_with_encoding(self, encoding: str) -> None:
        logging.info(
            "Attempting to do chunked upload of [%s] with encoding [%s]",
            self.path.abs_path(),
            encoding,
        )

    def on_file_stream_normalization(
        self, old_encoding: str, new_encoding: str
    ) -> None:
        logging.info(
            "Stream requires normalization. Old encoding: [%s]. New encoding: [%s]",
            old_encoding,
            new_encoding,
        )

    def on_dataframe(self, encoding: str, chunk_num: int, df: pd.DataFrame) -> bool:
        logging.info(
            "Loaded DataFrame chunk [%d] has [%d] rows", chunk_num, df.shape[0]
        )

        transformed_df = self.transform_dataframe(df)

        logging.info(
            "Transformed DataFrame chunk [%d] has [%d] rows",
            chunk_num,
            transformed_df.shape[0],
        )
        output_path = self.get_output_path(chunk_num=chunk_num)

        logging.info(
            "Writing DataFrame chunk [%d] to output path [%s]",
            chunk_num,
            output_path.abs_path(),
        )

        # We cannot use QUOTE_ALL as it results in empty values being written as "" in our temp file csv.
        # When uploading the temp file to BQ this results in empty strings being uploaded instead of NULLs.
        quoting = csv.QUOTE_MINIMAL
        self.fs.upload_from_string(
            output_path,
            transformed_df.to_csv(
                header=self.include_header, index=False, quoting=quoting
            ),
            "text/csv",
        )
        logging.info("Done writing to output path")

        self.output_paths_with_columns.append((output_path, transformed_df.columns))
        return True

    def on_unicode_decode_error(self, encoding: str, e: UnicodeError) -> bool:
        logging.info(
            "Unable to read file [%s] with encoding [%s]",
            self.path.abs_path(),
            encoding,
        )
        logging.exception(e)
        self._delete_temp_output_paths()
        return False

    def on_exception(self, encoding: str, e: Exception) -> bool:
        logging.error("Failed to upload to GCS - cleaning up temp paths")
        self._delete_temp_output_paths()
        return True

    def on_file_read_success(self, encoding: str) -> None:
        logging.info(
            "Successfully read file [%s] with encoding [%s]",
            self.path.abs_path(),
            encoding,
        )

    def _delete_temp_output_paths(self) -> None:
        for temp_output_path in [path for path, _ in self.output_paths_with_columns]:
            logging.info("Deleting temp file [%s].", temp_output_path.abs_path())
            self.fs.delete(temp_output_path)
        self.output_paths_with_columns.clear()

    @abc.abstractmethod
    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    @abc.abstractmethod
    def get_output_path(self, chunk_num: int) -> GcsfsFilePath:
        pass
