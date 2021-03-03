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
"""Streaming read functionality for Google Cloud Storage CSV files."""
import abc
from typing import Iterator, List, Optional, Any, TextIO

import gcsfs
import pandas as pd

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.utils import environment

COMMON_RAW_FILE_ENCODINGS = [
    "UTF-8",
    "ISO-8859-1",  # Also known as 'latin-1', used in the census and lots of other government data
]


class GcsfsCsvReaderDelegate:
    """A delegate for handling various events that happen during a GcsfsCsvReader streaming_read() call."""

    @abc.abstractmethod
    def on_start_read_with_encoding(self, encoding: str) -> None:
        """Called when we attempt to start reading the file with a particular encoding. This may get called multiple
        times during the course of a single streaming_read() call if one of the encodings fails.
        """

    @abc.abstractmethod
    def on_dataframe(self, encoding: str, chunk_num: int, df: pd.DataFrame) -> bool:
        """Called once for each dataframe chunk we read for a given encoding. May be called successfully multiple times
        before we hit a unicode decode error - this function should record any necessary state required for clean up in
        the case that we have to restart the read with a different encoding.

        Implementations should return True if iteration should continue to the next chunk (if there is one), or False if
        we can successfully terminate the read.
        """

    @abc.abstractmethod
    def on_unicode_decode_error(self, encoding: str, e: UnicodeError) -> bool:
        """Called when the file read hits a decode error for a given encoding. Any necessary clean up for the partially
        processed file should happen here.

        Implementations should return True if the exception should be re-raised, otherwise False if we should attempt
        the next encoding type.
        """

    @abc.abstractmethod
    def on_exception(self, encoding: str, e: Exception) -> bool:
        """Called when the file read hits any error that is not a UnicodeDecodeError. Any necessary clean up for the
        partially processed file should happen here.

        Implementations should return True if the exception should be re-raised, otherwise False if we should attempt
        the next encoding type.
        """

    @abc.abstractmethod
    def on_file_read_success(self, encoding: str) -> None:
        """Called when the streaming read has successfully completed."""


class GcsfsCsvReader:
    """Class providing streaming read functionality for Google Cloud Storage CSV files."""

    def __init__(self, fs: gcsfs.GCSFileSystem):
        self.gcs_file_system = fs

    def _file_pointer_for_path(self, path: GcsfsFilePath, encoding: str) -> TextIO:
        """Returns a file pointer for the given path."""

        # From the GCSFileSystem docs (https://gcsfs.readthedocs.io/en/latest/api.html#gcsfs.core.GCSFileSystem),
        # 'google_default' means we should look for local credentials set up via `gcloud login`. The project this is
        # reading from may have to match the project default you have set locally (check via `gcloud info` and set via
        # `gcloud config set project [PROJECT_ID]`. If we are running in the GCP environment, we should be able to query
        # the internal metadata for credentials.
        token = "google_default" if not environment.in_gcp() else "cloud"
        return self.gcs_file_system.open(path.uri(), encoding=encoding, token=token)

    def streaming_read(
        self,
        path: GcsfsFilePath,
        delegate: GcsfsCsvReaderDelegate,
        chunk_size: int,
        encodings_to_try: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> None:
        """
        Performs a streaming read of the CSV at the provided path. Will attempt to decode file with multiple encoding
        types. For large files, this allows us to read and process the whole file without ever storing the whole file in
        local memory/disk.

        Args:
            path: The GCS path to read.
            delegate: A delegate for handling read chunks one by one.
            chunk_size: The max number of rows each chunk of the CSV should have.
            encodings_to_try: If provided, the ordered list of file encodings we should try for the given file.
            wrapper: If provided and true, use wrapper function when calling the helper function in direct_ingest_utils
            kwargs: Key-value args passed through to the pandas read_csv() call.
        """

        if not encodings_to_try:
            encodings_to_try = COMMON_RAW_FILE_ENCODINGS

        for encoding in encodings_to_try:
            delegate.on_start_read_with_encoding(encoding)
            try:
                with self._file_pointer_for_path(path, encoding=encoding) as fp:
                    try:
                        reader: Iterator[pd.DataFrame] = pd.read_csv(
                            # Note: Pandas read_csv() also accepts GCS gs:// URIs directly, but it does not properly
                            # close the file stream in the case of an EmptyDataError, which we catch below, so we are
                            # creating and passing in a file pointer instead so that we have control over the scope.
                            fp,
                            encoding=encoding,
                            dtype=str,
                            chunksize=chunk_size,
                            **kwargs,
                        )
                    except pd.errors.EmptyDataError:
                        reader = iter([])

                    for i, df in enumerate(reader):
                        continue_iteration = delegate.on_dataframe(
                            encoding=encoding, chunk_num=i, df=df
                        )
                        if not continue_iteration:
                            break

                    delegate.on_file_read_success(encoding)
                    return
            except UnicodeError as e:
                should_throw = delegate.on_unicode_decode_error(encoding, e)
                if should_throw:
                    raise e
                continue
            except Exception as e:
                should_throw = delegate.on_exception(encoding, e)
                if should_throw:
                    raise e

        raise ValueError(
            f"Unable to read path [{path.abs_path()}] for any of these encodings: {encodings_to_try}"
        )
