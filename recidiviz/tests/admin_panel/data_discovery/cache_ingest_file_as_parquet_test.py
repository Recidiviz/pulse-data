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
""" Tests for cache_ingest_file_as_parquet """
from unittest import TestCase, mock

import fakeredis
import pandas
from freezegun import freeze_time

from recidiviz.admin_panel.data_discovery.cache_ingest_file_as_parquet import (
    CacheIngestFileAsParquetDelegate,
    SingleIngestFileParquetCache,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


class TestSingleIngestFileParquetCache(TestCase):
    """Tests for SingleIngestFileParquetCache"""

    def setUp(self) -> None:
        self.fakeredis = fakeredis.FakeRedis()

    @freeze_time("2021-04-13 23:00:00")
    def test_get_parquet_files(self) -> None:
        """Dataframes can be added to the cache; read from the cache"""
        file_path = GcsfsFilePath.from_absolute_path(
            "gs://recidiviz-staging-bucket/file.csv"
        )
        expiry = 999
        parquet_file_cache = SingleIngestFileParquetCache(
            self.fakeredis, file_path, expiry=expiry
        )
        first_chunk = pandas.DataFrame(data=[[1], [2]], columns=["column"])
        second_chunk = pandas.DataFrame(data=[[3], [4]], columns=["column"])

        parquet_file_cache.add_chunk(first_chunk)
        parquet_file_cache.add_chunk(second_chunk)
        parquet_file_cache.move_staged_files_to_completed()
        self.assertEqual(self.fakeredis.ttl(parquet_file_cache.cache_key), expiry)

        dataframes = [
            pandas.read_parquet(parquet_file)
            for parquet_file in parquet_file_cache.get_parquet_files()
        ]
        self.assertEqual(len(dataframes), 2)
        self.assertTrue(first_chunk.equals(dataframes[0]))
        self.assertTrue(second_chunk.equals(dataframes[1]))


class TestCacheIngestFileAsParquetDelegate(TestCase):
    """Tests for the CacheIngestFileAsParquetDelegate"""

    def setUp(self) -> None:
        self.redis = fakeredis.FakeRedis()
        path = GcsfsFilePath.from_absolute_path(
            "gs://test_bucket/us_id/ingest_view/2021/01/11/unprocessed_2021-01-11T00:00:00:000000_ingest_view_test_view.csv"
        )
        self.cache = mock.MagicMock()
        self.delegate = CacheIngestFileAsParquetDelegate(self.cache, path)

    def test_initialize(self) -> None:
        df = pandas.DataFrame(data=[1], columns=["x"])
        self.assertEqual(self.delegate.on_dataframe("UTF-8", 0, df), True)
        self.assertEqual(df.iloc[0]["ingest_processing_date"], "01/11/21")
        self.cache.add_chunk.assert_called()

    def test_on_exception(self) -> None:
        # Clear is called upon instantiation
        self.cache.reset_mock()

        self.assertTrue(self.delegate.on_exception("UTF-8", ValueError()))
        self.cache.clear.assert_not_called()

        # Cache is cleared upon pandas parsing exception
        self.assertTrue(
            self.delegate.on_exception("UTF-8", pandas.errors.ParserError())
        )
        self.cache.clear.assert_called()

    def test_on_unicode_decode_error(self) -> None:
        # Don't retry processing with a different data type on decode errors
        self.assertFalse(self.delegate.on_unicode_decode_error("UTF-8", UnicodeError()))

        # Cache is cleared upon decode error
        self.cache.clear.assert_called()
