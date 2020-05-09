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
"""Tests for DirectIngestFileMetadataManager."""
import datetime
import unittest
from typing import List, Dict

from google.cloud import bigquery
from mock import create_autospec

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.ingest.direct.controllers.direct_ingest_file_metadata_manager import \
    BigQueryDirectIngestFileMetadataManager, RawFileMetadataRow
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import to_normalized_unprocessed_file_path, \
    DirectIngestGCSFileSystem
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsFilePath


class DirectIngestFileMetadataManagerTest(unittest.TestCase):
    """Tests for DirectIngestFileMetadataManager."""

    def setUp(self) -> None:
        self.mock_big_query_client = create_autospec(BigQueryClient)
        self.metadata_manager = BigQueryDirectIngestFileMetadataManager(
            region_code='US_XX',
            big_query_client=self.mock_big_query_client
        )

    def _make_unprocessed_path(self, path_str: str, file_type: GcsfsDirectIngestFileType) -> GcsfsFilePath:
        normalized_path_str = \
            to_normalized_unprocessed_file_path(
                original_file_path=path_str,
                file_type=file_type,
                dt=datetime.datetime(2015, 1, 2, 3, 4, 5, 6))
        return GcsfsFilePath.from_absolute_path(normalized_path_str)

    def _make_processed_path(self, path_str: str, file_type: GcsfsDirectIngestFileType) -> GcsfsFilePath:
        path = self._make_unprocessed_path(path_str, file_type)
        # pylint:disable=protected-access
        return DirectIngestGCSFileSystem._to_processed_file_path(path)

    def test_register_processed_path_crashes(self):
        raw_processed_path = self._make_processed_path('bucket/file_tag.csv',
                                                       GcsfsDirectIngestFileType.RAW_DATA)

        with self.assertRaises(ValueError):
            self.metadata_manager.register_new_file(raw_processed_path)

        ingest_view_processed_path = self._make_processed_path('bucket/file_tag.csv',
                                                               GcsfsDirectIngestFileType.INGEST_VIEW)

        with self.assertRaises(ValueError):
            self.metadata_manager.register_new_file(ingest_view_processed_path)

    class FakeRowIterator(list):
        def __init__(self, results: List[Dict]):
            super().__init__(results)
            self.total_rows = len(self)

    def fake_query_results(self, query_str: str) -> bigquery.QueryJob:
        query_job = create_autospec(bigquery.QueryJob)
        if query_str.startswith('SELECT MAX'):
            query_job.result.return_value = self.FakeRowIterator([{'max_file_id': 6}])
        else:
            query_job.result.return_value = self.FakeRowIterator([])
        return query_job

    # TODO(3020): Once register_new_file actually writes a file, update this test to expect that we throw
    def test_get_raw_file_metadata_when_not_yet_registered(self):
        # Arrange

        self.mock_big_query_client.run_query_async.side_effect = self.fake_query_results

        raw_unprocessed_path = self._make_unprocessed_path('bucket/file_tag.csv',
                                                           GcsfsDirectIngestFileType.RAW_DATA)

        # Act
        metadata = self.metadata_manager.get_file_metadata(raw_unprocessed_path)

        # Assert
        self.assertIsInstance(metadata, RawFileMetadataRow)
        self.assertEqual(
            metadata,
            RawFileMetadataRow(
                region_code='US_XX',
                file_id=7,
                file_tag='file_tag',
                import_time=datetime.datetime(2015, 1, 2, 3, 4, 5, 6),
                normalized_file_name='unprocessed_2015-01-02T03:04:05:000006_raw_file_tag.csv',
                processed_time=None,
                datetimes_contained_lower_bound_inclusive=None,
                datetimes_contained_upper_bound_inclusive=datetime.datetime(2015, 1, 2, 3, 4, 5, 6)
            ))

    def test_mark_file_as_processed(self):
        # Arrange
        raw_unprocessed_path = self._make_unprocessed_path('bucket/file_tag.csv',
                                                           GcsfsDirectIngestFileType.RAW_DATA)
        metadata = RawFileMetadataRow(
            region_code='US_XX',
            file_id=7,
            file_tag='file_tag',
            import_time=datetime.datetime(2015, 1, 2, 3, 4, 5, 6),
            normalized_file_name='unprocessed_2015-01-02T03:04:05:000006_raw_file_tag.csv',
            processed_time=None,
            datetimes_contained_lower_bound_inclusive=None,
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2015, 1, 2, 3, 4, 5, 6)
        )

        self.mock_big_query_client.run_query_async.side_effect = self.fake_query_results

        # Act
        self.metadata_manager.mark_file_as_processed(raw_unprocessed_path, metadata,
                                                     datetime.datetime(2015, 1, 2, 3, 5, 6, 7))

        # Assert
        self.mock_big_query_client.run_query_async.assert_called_once()
        self.mock_big_query_client.insert_rows_into_table.assert_called_once()
