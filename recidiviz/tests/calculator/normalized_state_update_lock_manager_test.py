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
"""Tests for NormalizedStateUpdateLockManager."""

import unittest
from unittest.mock import Mock, patch

from recidiviz.calculator.normalized_state_update_lock_manager import (
    NORMALIZED_STATE_UPDATE_LOCK_NAME,
    NormalizedStateUpdateLockManager,
)
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import (
    GCSPseudoLockAlreadyExists,
    GCSPseudoLockDoesNotExist,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_lock_manager import (
    CloudSqlToBQLockManager,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class NormalizedStateUpdateLockManagerTest(unittest.TestCase):
    """Tests for NormalizedStateUpdateLockManager."""

    def setUp(self) -> None:
        self.fake_fs = FakeGCSFileSystem()
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"
        with patch(
            "recidiviz.cloud_storage.gcs_pseudo_lock_manager.GcsfsFactory.build",
            Mock(return_value=self.fake_fs),
        ):
            self.lock_manager = NormalizedStateUpdateLockManager()
            self.lock_bucket = self.lock_manager.lock_manager.bucket_name
            self.cloudsql_lock_manager = CloudSqlToBQLockManager()

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    def test_acquire_release_new_lock(self) -> None:
        self.lock_manager.acquire_lock(lock_id="lock1")
        expected_paths = [
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name=NORMALIZED_STATE_UPDATE_LOCK_NAME,
            )
        ]
        self.assertEqual(expected_paths, self.fake_fs.all_paths)

        self.lock_manager.release_lock()
        self.assertEqual([], self.fake_fs.all_paths)

    def test_release_without_acquiring(self) -> None:
        with self.assertRaises(GCSPseudoLockDoesNotExist):
            self.lock_manager.release_lock()

    def test_acquire_existing(self) -> None:
        self.lock_manager.acquire_lock(lock_id="lock1")
        with self.assertRaises(GCSPseudoLockAlreadyExists):
            self.lock_manager.acquire_lock(lock_id="lock2")

    def test_acquire_state_cannot_proceed(self) -> None:
        self.lock_manager.acquire_lock(lock_id="lock1")
        self.cloudsql_lock_manager.acquire_lock(
            lock_id="lock1", schema_type=(SchemaType.STATE)
        )

        self.assertFalse(self.lock_manager.can_proceed())

        # Acquiring the same lock again does not crash
        self.lock_manager.acquire_lock(lock_id="lock1")

    def test_acquire_non_state_can_proceed(self) -> None:
        self.lock_manager.acquire_lock(lock_id="lock1")
        for schema_type in SchemaType:
            if schema_type == SchemaType.STATE:
                continue
            self.cloudsql_lock_manager.acquire_lock(
                lock_id="lock1", schema_type=schema_type
            )

        self.assertTrue(self.lock_manager.can_proceed())

    def test_can_proceed_without_acquiring(self) -> None:
        with self.assertRaises(GCSPseudoLockDoesNotExist):
            self.lock_manager.can_proceed()
