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
"""Tests for the DirectIngestGCSFileSystem."""
import datetime
import os
from unittest import TestCase

from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsFilePath, \
    GcsfsDirectoryPath
from recidiviz.tests.ingest.direct.direct_ingest_util import \
    FakeDirectIngestGCSFileSystem


class TestDirectIngestGcsFileSystem(TestCase):
    """Tests for the DirectIngestGCSFileSystem."""

    STORAGE_DIR_PATH = GcsfsDirectoryPath(bucket_name='storage_bucket',
                                          relative_path='region_subdir')

    INGEST_DIR_PATH = GcsfsDirectoryPath(bucket_name='my_bucket')

    def fully_process_file(self,
                           test_fs: FakeDirectIngestGCSFileSystem,
                           dt: datetime.datetime,
                           path: GcsfsFilePath):
        """Mimics all the file system calls for a single file in the direct
        ingest system, from getting added to the ingest bucket, turning to a
        processed file, then getting moved to storage."""

        test_fs.test_add_path(path)

        start_num_total_files = len(test_fs.all_paths)
        # pylint: disable=protected-access
        start_ingest_paths = test_fs._ls_with_file_prefix(
            self.INGEST_DIR_PATH, '')
        start_storage_paths = test_fs._ls_with_file_prefix(
            self.STORAGE_DIR_PATH, '')

        # File is renamed to normalized path
        test_fs.mv_path_to_normalized_path(path, dt)

        unprocessed = test_fs.get_unprocessed_file_paths(self.INGEST_DIR_PATH)
        self.assertEqual(len(unprocessed), 1)
        self.assertTrue(test_fs.is_seen_unprocessed_file(unprocessed[0]))

        # ... file is processed

        # File is moved to processed path
        test_fs.mv_path_to_processed_path(unprocessed[0])
        processed = test_fs.get_processed_file_paths(self.INGEST_DIR_PATH)
        self.assertEqual(len(processed), 1)
        self.assertTrue(test_fs.is_processed_file(processed[0]))

        unprocessed = test_fs.get_unprocessed_file_paths(self.INGEST_DIR_PATH)
        self.assertEqual(len(unprocessed), 0)

        # File is moved to storage
        test_fs.mv_processed_paths_before_date_to_storage(
            self.INGEST_DIR_PATH, self.STORAGE_DIR_PATH, dt.date().isoformat(),
            include_bound=True)

        end_ingest_paths = test_fs._ls_with_file_prefix(
            self.INGEST_DIR_PATH, '')
        end_storage_paths = test_fs._ls_with_file_prefix(
            self.STORAGE_DIR_PATH, '')

        self.assertEqual(len(test_fs.all_paths), start_num_total_files)
        self.assertEqual(len(end_ingest_paths), len(start_ingest_paths) - 1)
        self.assertEqual(len(end_storage_paths), len(start_storage_paths) + 1)

        for sp in end_storage_paths:
            if sp.abs_path() not in \
                    {p.abs_path() for p in start_storage_paths}:
                self.assertTrue(
                    sp.abs_path().startswith(
                        self.STORAGE_DIR_PATH.abs_path()))
                _, storage_file_name = \
                    os.path.split(sp.abs_path())
                name, _ = path.file_name.split('.')
                self.assertTrue(name in storage_file_name)

    def test_direct_ingest_file_moves(self):
        test_fs = FakeDirectIngestGCSFileSystem()
        self.fully_process_file(test_fs,
                                datetime.datetime.now(),
                                GcsfsFilePath(bucket_name='my_bucket',
                                              blob_name='test_file.csv'))

    def test_direct_ingest_multiple_file_moves(self):
        test_fs = FakeDirectIngestGCSFileSystem()
        self.fully_process_file(test_fs,
                                datetime.datetime.now(),
                                GcsfsFilePath(bucket_name='my_bucket',
                                              blob_name='test_file.csv'))

        self.fully_process_file(test_fs,
                                datetime.datetime.now(),
                                GcsfsFilePath(bucket_name='my_bucket',
                                              blob_name='test_file2.csv'))

    def test_move_to_storage_with_conflict(self):
        test_fs = FakeDirectIngestGCSFileSystem()
        dt = datetime.datetime.now()
        self.fully_process_file(test_fs, dt,
                                GcsfsFilePath(bucket_name='my_bucket',
                                              blob_name='test_file.csv'))

        # Try uploading a file with a duplicate name that has already been
        # moved to storage
        self.fully_process_file(test_fs, dt,
                                GcsfsFilePath(bucket_name='my_bucket',
                                              blob_name='test_file.csv'))

        # pylint: disable=protected-access
        storage_paths = test_fs._ls_with_file_prefix(self.STORAGE_DIR_PATH, '')
        self.assertEqual(len(storage_paths), 2)

        found_first_file = False
        found_second_file = False
        for path in storage_paths:
            if path.abs_path().endswith('test_file.csv'):
                found_first_file = True
            if path.abs_path().endswith('test_file-(1).csv'):
                found_second_file = True

        self.assertTrue(found_first_file)
        self.assertTrue(found_second_file)
