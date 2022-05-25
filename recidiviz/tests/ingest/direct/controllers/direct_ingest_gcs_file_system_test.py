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
import datetime
import os
from unittest import TestCase

from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
    to_normalized_processed_raw_file_name,
    to_normalized_unprocessed_file_path_from_normalized_path,
    to_normalized_unprocessed_raw_file_name,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest.direct import fixture_util
from recidiviz.tests.utils.fake_region import TEST_STATE_REGION


class TestDirectIngestGcsFileSystem(TestCase):
    """Tests for the FakeGCSFileSystem."""

    STORAGE_DIR_PATH = GcsfsDirectoryPath(
        bucket_name="storage_bucket", relative_path="region_subdir"
    )

    INGEST_DIR_PATH = GcsfsDirectoryPath(bucket_name="my_bucket")

    def setUp(self) -> None:
        self.fs = DirectIngestGCSFileSystem(FakeGCSFileSystem())

    def fully_process_file(self, dt: datetime.datetime, path: GcsfsFilePath) -> None:
        """Mimics all the file system calls for a single file in the direct
        ingest system, from getting added to the ingest bucket, turning to a
        processed file, then getting moved to storage."""

        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system,
            path,
            region_code=TEST_STATE_REGION.region_code,
            has_fixture=False,
        )

        start_num_total_files = len(self.fs.gcs_file_system.all_paths)
        # pylint: disable=protected-access
        start_ingest_paths = self.fs._ls_with_file_prefix(
            self.INGEST_DIR_PATH, "", filter_type=self.fs._FilterType.NO_FILTER
        )
        start_storage_paths = self.fs._ls_with_file_prefix(
            self.STORAGE_DIR_PATH, "", filter_type=self.fs._FilterType.NO_FILTER
        )

        start_raw_storage_paths = self.fs._ls_with_file_prefix(
            self.STORAGE_DIR_PATH, "", filter_type=self.fs._FilterType.NORMALIZED_ONLY
        )

        # File is renamed to normalized path
        self.fs.mv_raw_file_to_normalized_path(path, dt)

        raw_unprocessed = self.fs.get_unprocessed_raw_file_paths(self.INGEST_DIR_PATH)
        self.assertEqual(len(raw_unprocessed), 1)
        self.assertTrue(self.fs.is_seen_unprocessed_file(raw_unprocessed[0]))

        # ... raw file imported to BQ

        processed_path = self.fs.mv_path_to_processed_path(raw_unprocessed[0])

        processed = self.fs.get_processed_file_paths(self.INGEST_DIR_PATH)
        self.assertEqual(len(processed), 1)
        unprocessed = self.fs.get_unprocessed_raw_file_paths(self.INGEST_DIR_PATH)
        self.assertEqual(len(unprocessed), 0)

        self.fs.mv_raw_file_to_storage(processed_path, self.STORAGE_DIR_PATH)

        processed = self.fs.get_processed_file_paths(self.INGEST_DIR_PATH)
        self.assertEqual(len(processed), 0)

        end_ingest_paths = self.fs._ls_with_file_prefix(
            self.INGEST_DIR_PATH, "", filter_type=self.fs._FilterType.NO_FILTER
        )
        end_storage_paths = self.fs._ls_with_file_prefix(
            self.STORAGE_DIR_PATH, "", filter_type=self.fs._FilterType.NO_FILTER
        )
        end_raw_storage_paths = self.fs._ls_with_file_prefix(
            self.STORAGE_DIR_PATH, "", filter_type=self.fs._FilterType.NORMALIZED_ONLY
        )

        expected_final_total_files = start_num_total_files
        self.assertEqual(
            len(self.fs.gcs_file_system.all_paths), expected_final_total_files
        )
        self.assertEqual(len(end_ingest_paths), len(start_ingest_paths) - 1)
        self.assertEqual(len(end_storage_paths), len(start_storage_paths) + 1)
        self.assertEqual(len(end_raw_storage_paths), len(end_storage_paths))
        self.assertEqual(len(end_raw_storage_paths), len(start_raw_storage_paths) + 1)

        for sp in end_storage_paths:
            parts = filename_parts_from_path(sp)
            if sp.abs_path() not in {p.abs_path() for p in start_storage_paths}:
                self.assertTrue(
                    sp.abs_path().startswith(self.STORAGE_DIR_PATH.abs_path())
                )
                dir_path, storage_file_name = os.path.split(sp.abs_path())

                self.assertTrue(parts.file_type.value in dir_path)
                name, _ = path.file_name.split(".")
                self.assertTrue(name in storage_file_name)

    def test_direct_ingest_file_moves(self) -> None:
        self.fully_process_file(
            datetime.datetime.now(),
            GcsfsFilePath(bucket_name="my_bucket", blob_name="test_file.csv"),
        )

    def test_direct_ingest_multiple_file_moves(self) -> None:
        self.fully_process_file(
            datetime.datetime.now(),
            GcsfsFilePath(bucket_name="my_bucket", blob_name="test_file.csv"),
        )

        self.fully_process_file(
            datetime.datetime.now(),
            GcsfsFilePath(bucket_name="my_bucket", blob_name="test_file_2.csv"),
        )

    def test_direct_ingest_file_moves_with_file_types(self) -> None:
        self.fully_process_file(
            datetime.datetime.now(),
            GcsfsFilePath(bucket_name="my_bucket", blob_name="test_file.csv"),
        )

    def test_direct_ingest_multiple_file_moves_with_file_types(self) -> None:
        self.fully_process_file(
            datetime.datetime.now(),
            GcsfsFilePath(bucket_name="my_bucket", blob_name="test_file.csv"),
        )

        self.fully_process_file(
            datetime.datetime.now(),
            GcsfsFilePath(bucket_name="my_bucket", blob_name="test_file_2.csv"),
        )

    def test_move_to_storage_with_conflict_with_file_types(self) -> None:
        dt = datetime.datetime.now()
        self.fully_process_file(
            dt, GcsfsFilePath(bucket_name="my_bucket", blob_name="test_file.csv")
        )

        # Try uploading a file with a duplicate name that has already been
        # moved to storage
        self.fully_process_file(
            dt, GcsfsFilePath(bucket_name="my_bucket", blob_name="test_file.csv")
        )

        # pylint: disable=protected-access
        storage_paths = self.fs._ls_with_file_prefix(
            self.STORAGE_DIR_PATH, "", filter_type=self.fs._FilterType.NO_FILTER
        )
        self.assertEqual(len(storage_paths), 2)

        found_first_file = False
        found_second_file = False
        for path in storage_paths:
            if path.abs_path().endswith("test_file.csv"):
                found_first_file = True
            if path.abs_path().endswith("test_file-(1).csv"):
                found_second_file = True

        self.assertTrue(found_first_file)
        self.assertTrue(found_second_file)


class TestPathNormalization(TestCase):
    """Class that tests path normalization functions created for both processed and unprocessed file paths"""

    def test_to_normalized_unprocessed_file_path_from_normalized_path(self) -> None:
        original_file_path = (
            "gs://test-bucket-direct-ingest-state-storage/us_nd/2019/08/12/"
            "processed_2019-08-12T00:00:00:000000_raw_test_file_tag.csv"
        )
        expected_file_path = (
            "gs://test-bucket-direct-ingest-state-storage/us_nd/2019/08/12/"
            "unprocessed_2019-08-12T00:00:00:000000_raw_test_file_tag.csv"
        )
        self.assertEqual(
            expected_file_path,
            to_normalized_unprocessed_file_path_from_normalized_path(
                original_file_path
            ),
        )

    def test_to_normalized_processed_file_name(self) -> None:
        original_file_name = "test_file_tag.csv"
        expected_file_name = (
            "processed_2019-08-12T00:00:00:000000_raw_test_file_tag.csv"
        )
        self.assertEqual(
            expected_file_name,
            to_normalized_processed_raw_file_name(
                original_file_name,
                datetime.datetime(2019, 8, 12, 0, 0, 0),
            ),
        )

    def test_to_normalized_unprocessed_file_name(self) -> None:
        original_file_name = "test_file_tag.csv"
        expected_file_name = (
            "unprocessed_2019-08-12T00:00:00:000000_raw_test_file_tag.csv"
        )
        self.assertEqual(
            expected_file_name,
            to_normalized_unprocessed_raw_file_name(
                original_file_name,
                datetime.datetime(2019, 8, 12, 0, 0, 0),
            ),
        )
