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
""" Functionality for converting CSVs to Parquet format and storing in Redis
"""
from typing import Generator
from io import BytesIO

import pandas
import pandas as pd
import redis

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.controllers.gcsfs_csv_reader import (
    GcsfsCsvReaderDelegate,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    filename_parts_from_path,
)


class SingleIngestFileParquetCache:
    """Class responsible for converting dataframes to Parquet and storing to Redis.
    A single ingest file may be broken into multiple chunks, so we store the Parquet files as a Redis list
    """

    def __init__(
        self, cache: redis.Redis, file: GcsfsFilePath, *, expiry: int = 0
    ) -> None:
        self.cache = cache
        self.expiry = expiry
        self.cache_key = self.parquet_cache_key(file)
        self.staged_cache_key = f"{self.cache_key}.tmp"

    @classmethod
    def parquet_cache_key(cls, file: GcsfsFilePath) -> str:
        return f"{file.uri()}.pq"

    def clear(self) -> None:
        """ Removes the entry from the cache """
        self.cache.delete(self.cache_key)
        self.cache.delete(self.staged_cache_key)

    def add_chunk(self, df: pandas.DataFrame) -> None:
        """ Serializes a dataframe to parquet format; pushes to redis """
        self.cache.rpush(self.staged_cache_key, df.to_parquet(compression="GZIP"))
        self.cache.expire(self.staged_cache_key, self.expiry)

    def move_staged_files_to_completed(self) -> None:
        """ Once a file has been fully cached, we can move it to the main cache key"""
        self.cache.rename(self.staged_cache_key, self.cache_key)

    def get_parquet_files(self) -> Generator[BytesIO, None, None]:
        file_count = self.cache.llen(self.cache_key)

        for i in range(file_count):
            parquet_file = self.cache.lindex(self.cache_key, i)

            if not isinstance(parquet_file, bytes):
                raise ValueError(
                    f"Expected Parquet file at {self.cache_key} index {i} to be bytes type"
                )

            yield BytesIO(parquet_file)


class CacheIngestFileAsParquetDelegate(GcsfsCsvReaderDelegate):
    """ GcsfsCsvReaderDelegate for streaming reads to Redis"""

    def __init__(
        self, parquet_cache: SingleIngestFileParquetCache, path: GcsfsFilePath
    ):
        self.path = path
        self.file_parts = filename_parts_from_path(path)
        self.parquet_cache = parquet_cache
        self.parquet_cache.clear()

    def on_dataframe(self, encoding: str, chunk_num: int, df: pd.DataFrame) -> bool:
        """ For each chunked dataframe, append the processing date column, and push to Redis """
        df["ingest_processing_date"] = self.file_parts.utc_upload_datetime.strftime(
            "%D"
        )

        self.parquet_cache.add_chunk(df)

        # Continue to the next chunk
        return True

    def on_unicode_decode_error(self, encoding: str, e: UnicodeError) -> bool:
        # The file cannot be decoded; delete cache key and don't retry
        self.parquet_cache.clear()

        return False

    def on_exception(self, encoding: str, e: Exception) -> bool:
        # If the file cannot be successfully parsed, remove from cache
        if isinstance(e, pandas.errors.ParserError):
            self.parquet_cache.clear()

        # Re-raise exception
        return True

    def on_file_read_success(self, encoding: str) -> None:
        self.parquet_cache.move_staged_files_to_completed()

    def on_start_read_with_encoding(self, encoding: str) -> None:
        pass
