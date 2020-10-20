# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for the Path Normalization in DirectIngestGCSFileSystem."""
import os
import datetime
from unittest import TestCase

from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import \
    to_normalized_unprocessed_file_path_from_normalized_path, to_normalized_processed_file_path_from_normalized_path, \
    to_normalized_processed_file_name, to_normalized_unprocessed_file_name, DirectIngestGCSFileSystem
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsDirectIngestFileType, \
    filename_parts_from_path
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class TestDirectIngestGcsFileSystem(TestCase):
    """Tests for the FakeGCSFileSystem."""

    STORAGE_DIR_PATH = GcsfsDirectoryPath(bucket_name='storage_bucket',
                                          relative_path='region_subdir')

    INGEST_DIR_PATH = GcsfsDirectoryPath(bucket_name='my_bucket')

    def setUp(self) -> None:
        self.fs = DirectIngestGCSFileSystem(FakeGCSFileSystem())

    def fully_process_file(self,
                           dt: datetime.datetime,
                           path: GcsfsFilePath,
                           file_type_differentiation_on: bool = False) -> None:
        """Mimics all the file system calls for a single file in the direct
        ingest system, from getting added to the ingest bucket, turning to a
        processed file, then getting moved to storage."""

        self.fs.gcs_file_system.test_add_path(path)

        start_num_total_files = len(self.fs.gcs_file_system.all_paths)
        # pylint: disable=protected-access
        start_ingest_paths = self.fs._ls_with_file_prefix(self.INGEST_DIR_PATH, '', None)
        start_storage_paths = self.fs._ls_with_file_prefix(self.STORAGE_DIR_PATH, '', None)
        if file_type_differentiation_on:
            start_raw_storage_paths = self.fs._ls_with_file_prefix(
                self.STORAGE_DIR_PATH, '', file_type_filter=GcsfsDirectIngestFileType.RAW_DATA)
            start_ingest_view_storage_paths = self.fs._ls_with_file_prefix(
                self.STORAGE_DIR_PATH, '', file_type_filter=GcsfsDirectIngestFileType.INGEST_VIEW)
        else:
            start_raw_storage_paths = []
            start_ingest_view_storage_paths = []

        # File is renamed to normalized path
        file_type = GcsfsDirectIngestFileType.RAW_DATA \
            if file_type_differentiation_on else GcsfsDirectIngestFileType.UNSPECIFIED

        self.fs.mv_path_to_normalized_path(path, file_type, dt)

        if file_type_differentiation_on:
            raw_unprocessed = self.fs.get_unprocessed_file_paths(self.INGEST_DIR_PATH,
                                                                 file_type_filter=GcsfsDirectIngestFileType.RAW_DATA)
            self.assertEqual(len(raw_unprocessed), 1)
            self.assertTrue(self.fs.is_seen_unprocessed_file(raw_unprocessed[0]))

            # ... raw file imported to BQ

            processed_path = self.fs.mv_path_to_processed_path(raw_unprocessed[0])

            processed = self.fs.get_processed_file_paths(self.INGEST_DIR_PATH, None)
            self.assertEqual(len(processed), 1)

            self.fs.copy(processed_path,
                         GcsfsFilePath.from_absolute_path(
                             to_normalized_unprocessed_file_path_from_normalized_path(
                                 processed_path.abs_path(),
                                 file_type_override=GcsfsDirectIngestFileType.INGEST_VIEW)))
            self.fs.mv_path_to_storage(processed_path, self.STORAGE_DIR_PATH)

        ingest_unprocessed_filter = GcsfsDirectIngestFileType.INGEST_VIEW if file_type_differentiation_on else None

        ingest_unprocessed = self.fs.get_unprocessed_file_paths(self.INGEST_DIR_PATH,
                                                                file_type_filter=ingest_unprocessed_filter)
        self.assertEqual(len(ingest_unprocessed), 1)
        self.assertTrue(self.fs.is_seen_unprocessed_file(ingest_unprocessed[0]))

        # ... file is ingested

        # File is moved to processed path
        self.fs.mv_path_to_processed_path(ingest_unprocessed[0])
        processed = self.fs.get_processed_file_paths(self.INGEST_DIR_PATH, None)
        self.assertEqual(len(processed), 1)
        self.assertTrue(self.fs.is_processed_file(processed[0]))

        unprocessed = self.fs.get_unprocessed_file_paths(self.INGEST_DIR_PATH, None)
        self.assertEqual(len(unprocessed), 0)

        # File is moved to storage
        ingest_move_type_filter = GcsfsDirectIngestFileType.INGEST_VIEW \
            if file_type_differentiation_on else None

        self.fs.mv_processed_paths_before_date_to_storage(
            self.INGEST_DIR_PATH, self.STORAGE_DIR_PATH, date_str_bound=dt.date().isoformat(),
            include_bound=True, file_type_filter=ingest_move_type_filter)

        end_ingest_paths = self.fs._ls_with_file_prefix(self.INGEST_DIR_PATH, '', file_type_filter=None)
        end_storage_paths = self.fs._ls_with_file_prefix(self.STORAGE_DIR_PATH, '', file_type_filter=None)
        if file_type_differentiation_on:
            end_raw_storage_paths = self.fs._ls_with_file_prefix(
                self.STORAGE_DIR_PATH, '', file_type_filter=GcsfsDirectIngestFileType.RAW_DATA)
            end_ingest_view_storage_paths = self.fs._ls_with_file_prefix(
                self.STORAGE_DIR_PATH, '', file_type_filter=GcsfsDirectIngestFileType.INGEST_VIEW)
        else:
            end_raw_storage_paths = []
            end_ingest_view_storage_paths = []

        # Each file gets re-exported as ingest view
        splitting_factor = 2 if file_type_differentiation_on else 1

        expected_final_total_files = start_num_total_files + splitting_factor - 1
        self.assertEqual(len(self.fs.gcs_file_system.all_paths), expected_final_total_files)
        self.assertEqual(len(end_ingest_paths), len(start_ingest_paths) - 1)
        self.assertEqual(len(end_storage_paths), len(start_storage_paths) + 1 * splitting_factor)
        if file_type_differentiation_on:
            self.assertEqual(len(end_raw_storage_paths) + len(end_ingest_view_storage_paths), len(end_storage_paths))
            self.assertEqual(len(end_raw_storage_paths), len(start_raw_storage_paths) + 1)
            self.assertEqual(len(end_ingest_view_storage_paths), len(start_ingest_view_storage_paths) + 1)

        for sp in end_storage_paths:
            parts = filename_parts_from_path(sp)
            if sp.abs_path() not in {p.abs_path() for p in start_storage_paths}:
                self.assertTrue(sp.abs_path().startswith(self.STORAGE_DIR_PATH.abs_path()))
                dir_path, storage_file_name = os.path.split(sp.abs_path())
                if parts.file_type != GcsfsDirectIngestFileType.UNSPECIFIED:
                    self.assertTrue(parts.file_type.value in dir_path)
                name, _ = path.file_name.split('.')
                self.assertTrue(name in storage_file_name)

    def test_direct_ingest_file_moves(self) -> None:
        self.fully_process_file(datetime.datetime.now(),
                                GcsfsFilePath(bucket_name='my_bucket',
                                              blob_name='test_file.csv'))

    def test_direct_ingest_multiple_file_moves(self) -> None:
        self.fully_process_file(datetime.datetime.now(),
                                GcsfsFilePath(bucket_name='my_bucket',
                                              blob_name='test_file.csv'))

        self.fully_process_file(datetime.datetime.now(),
                                GcsfsFilePath(bucket_name='my_bucket',
                                              blob_name='test_file_2.csv'))

    def test_move_to_storage_with_conflict(self) -> None:
        dt = datetime.datetime.now()
        self.fully_process_file(dt,
                                GcsfsFilePath(bucket_name='my_bucket',
                                              blob_name='test_file.csv'))

        # Try uploading a file with a duplicate name that has already been
        # moved to storage
        self.fully_process_file(dt,
                                GcsfsFilePath(bucket_name='my_bucket',
                                              blob_name='test_file.csv'))

        # pylint: disable=protected-access
        storage_paths = self.fs._ls_with_file_prefix(self.STORAGE_DIR_PATH, '', file_type_filter=None)
        self.assertEqual(len(storage_paths), 2)

        found_first_file = False
        found_second_file = False
        for path in storage_paths:
            self.assertTrue(filename_parts_from_path(path))
            if path.abs_path().endswith('test_file.csv'):
                found_first_file = True
            if path.abs_path().endswith('test_file-(1).csv'):
                found_second_file = True

        self.assertTrue(found_first_file)
        self.assertTrue(found_second_file)

    def test_direct_ingest_file_moves_with_file_types(self) -> None:
        self.fully_process_file(datetime.datetime.now(),
                                GcsfsFilePath(bucket_name='my_bucket',
                                              blob_name='test_file.csv'),
                                file_type_differentiation_on=True)

    def test_direct_ingest_multiple_file_moves_with_file_types(self) -> None:
        self.fully_process_file(datetime.datetime.now(),
                                GcsfsFilePath(bucket_name='my_bucket',
                                              blob_name='test_file.csv'),
                                file_type_differentiation_on=True)

        self.fully_process_file(datetime.datetime.now(),
                                GcsfsFilePath(bucket_name='my_bucket',
                                              blob_name='test_file_2.csv'),
                                file_type_differentiation_on=True)

    def test_move_to_storage_with_conflict_with_file_types(self) -> None:
        dt = datetime.datetime.now()
        self.fully_process_file(dt,
                                GcsfsFilePath(bucket_name='my_bucket',
                                              blob_name='test_file.csv'),
                                file_type_differentiation_on=True)

        # Try uploading a file with a duplicate name that has already been
        # moved to storage
        self.fully_process_file(dt,
                                GcsfsFilePath(bucket_name='my_bucket',
                                              blob_name='test_file.csv'),
                                file_type_differentiation_on=True)

        # pylint: disable=protected-access
        storage_paths = self.fs._ls_with_file_prefix(self.STORAGE_DIR_PATH, '', file_type_filter=None)
        self.assertEqual(len(storage_paths), 4)

        found_first_file = False
        found_second_file = False
        for path in storage_paths:
            if path.abs_path().endswith('test_file.csv'):
                found_first_file = True
            if path.abs_path().endswith('test_file-(1).csv'):
                found_second_file = True

        self.assertTrue(found_first_file)
        self.assertTrue(found_second_file)


class TestPathNormalization(TestCase):
    """Class that tests path normalization functions created for both processed and unprocessed file paths"""

    def test_to_normalized_processed_file_path_from_normalized_path(self) -> None:
        original_file_path = 'gs://test-bucket-direct-ingest-state-storage/us_nd/2019/08/12/' \
                             'unprocessed_2019-08-12T00:00:00:000000_test_file_tag.csv'
        expected_file_path = 'gs://test-bucket-direct-ingest-state-storage/us_nd/2019/08/12/' \
                             'processed_2019-08-12T00:00:00:000000_raw_test_file_tag.csv'
        self.assertEqual(expected_file_path,
                         to_normalized_processed_file_path_from_normalized_path(original_file_path,
                                                                                GcsfsDirectIngestFileType.RAW_DATA))

    def test_to_normalized_unprocessed_file_path_from_normalized_path(self) -> None:
        original_file_path = 'gs://test-bucket-direct-ingest-state-storage/us_nd/2019/08/12/' \
                             'processed_2019-08-12T00:00:00:000000_test_file_tag.csv'
        expected_file_path = 'gs://test-bucket-direct-ingest-state-storage/us_nd/2019/08/12/' \
                             'unprocessed_2019-08-12T00:00:00:000000_raw_test_file_tag.csv'
        self.assertEqual(expected_file_path,
                         to_normalized_unprocessed_file_path_from_normalized_path(original_file_path,
                                                                                  GcsfsDirectIngestFileType.RAW_DATA))

    def test_to_normalized_processed_file_name(self) -> None:
        original_file_name = 'test_file_tag.csv'
        expected_file_name = 'processed_2019-08-12T00:00:00:000000_raw_test_file_tag.csv'
        self.assertEqual(expected_file_name,
                         to_normalized_processed_file_name(original_file_name,
                                                           GcsfsDirectIngestFileType.RAW_DATA,
                                                           datetime.datetime(2019, 8, 12, 0, 0, 0)))

    def test_to_normalized_unprocessed_file_name(self) -> None:
        original_file_name = 'test_file_tag.csv'
        expected_file_name = 'unprocessed_2019-08-12T00:00:00:000000_raw_test_file_tag.csv'
        self.assertEqual(expected_file_name,
                         to_normalized_unprocessed_file_name(original_file_name,
                                                             GcsfsDirectIngestFileType.RAW_DATA,
                                                             datetime.datetime(2019, 8, 12, 0, 0, 0)))
