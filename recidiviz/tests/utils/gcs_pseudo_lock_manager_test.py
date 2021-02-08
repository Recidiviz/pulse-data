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
"""Tests for the GCS Pseudo Lock Manager class"""

import unittest
from datetime import datetime
from unittest import mock
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.utils.gcs_pseudo_lock_manager import GCSPseudoLockManager, GCSPseudoLockAlreadyExists, \
    GCSPseudoLockDoesNotExist
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class GCSPseudoLockManagerTest(unittest.TestCase):
    """Class to test GCS Pseudo Lock Manager"""
    LOCK_NAME = 'LOCK_NAME'
    LOCK_NAME2 = 'LOCK_NAME2'
    PROJECT_ID = 'recidiviz-staging'
    TIME_FORMAT = "%m/%d/%Y, %H:%M:%S"
    CONTENTS = "CONTENTS"
    CONTENTS2 = "CONTENTS2"

    def setUp(self) -> None:
        self.project_id_patcher = mock.patch('recidiviz.utils.gcs_pseudo_lock_manager.metadata')
        self.project_id_patcher.start().return_value = 'recidiviz-123'
        self.gcs_factory_patcher = mock.patch('recidiviz.utils.gcs_pseudo_lock_manager.GcsfsFactory.build')
        fake_gcs = FakeGCSFileSystem()
        self.gcs_factory_patcher.start().return_value = fake_gcs
        self.fs = fake_gcs

    def tearDown(self) -> None:
        self.gcs_factory_patcher.stop()
        self.project_id_patcher.stop()

    def test_lock(self) -> None:
        """Locks temp and then checks if locked"""
        lock_manager = GCSPseudoLockManager(self.PROJECT_ID)
        lock_manager.lock(self.LOCK_NAME)
        self.assertTrue(lock_manager.is_locked(self.LOCK_NAME))

    def test_lock_unlock(self) -> None:
        """Locks then unlocks temp, checks if still locked"""
        lock_manager = GCSPseudoLockManager(self.PROJECT_ID)
        lock_manager.lock(self.LOCK_NAME)
        lock_manager.unlock(self.LOCK_NAME)
        self.assertFalse(lock_manager.is_locked(self.LOCK_NAME))

    def test_double_lock(self) -> None:
        """Locks and then locks again, asserts its still locked and an error is raised"""
        lock_manager = GCSPseudoLockManager(self.PROJECT_ID)
        lock_manager.lock(self.LOCK_NAME)
        with self.assertRaises(GCSPseudoLockAlreadyExists):
            lock_manager.lock(self.LOCK_NAME)
        self.assertTrue(lock_manager.is_locked(self.LOCK_NAME))

    def test_double_unlock(self) -> None:
        """Unlocks and then unlocks gain, asserts its still unlocked and raises an error"""
        lock_manager = GCSPseudoLockManager(self.PROJECT_ID)
        self.assertFalse(lock_manager.is_locked(self.LOCK_NAME))
        with self.assertRaises(GCSPseudoLockDoesNotExist):
            lock_manager.unlock(self.LOCK_NAME)
        with self.assertRaises(GCSPseudoLockDoesNotExist):
            lock_manager.unlock(self.LOCK_NAME)
        self.assertFalse(lock_manager.is_locked(self.LOCK_NAME))

    def test_lock_two_diff(self) -> None:
        """Locks two different locks, asserts both locked"""
        lock_manager = GCSPseudoLockManager(self.PROJECT_ID)
        lock_manager.lock(self.LOCK_NAME)
        lock_manager.lock(self.LOCK_NAME2)
        self.assertTrue(lock_manager.is_locked(self.LOCK_NAME))
        self.assertTrue(lock_manager.is_locked(self.LOCK_NAME2))

    def test_lock_unlock_two_diff(self) -> None:
        """Locks two different locks, unlocks both, asserts both unlocked"""
        lock_manager = GCSPseudoLockManager(self.PROJECT_ID)
        lock_manager.lock(self.LOCK_NAME)
        lock_manager.lock(self.LOCK_NAME2)
        lock_manager.unlock(self.LOCK_NAME)
        lock_manager.unlock(self.LOCK_NAME2)
        self.assertFalse(lock_manager.is_locked(self.LOCK_NAME))
        self.assertFalse(lock_manager.is_locked(self.LOCK_NAME2))

    def test_lock_two_diff_unlock_one(self) -> None:
        """Locks two different locks, unlocks one, asserts both in correct place"""
        lock_manager = GCSPseudoLockManager(self.PROJECT_ID)
        lock_manager.lock(self.LOCK_NAME)
        lock_manager.lock(self.LOCK_NAME2)
        lock_manager.unlock(self.LOCK_NAME)
        self.assertFalse(lock_manager.is_locked(self.LOCK_NAME))
        self.assertTrue(lock_manager.is_locked(self.LOCK_NAME2))

    def test_lock_one_unlock_other(self) -> None:
        """Locks one lock and unlocks another, asserts both have correct status"""
        lock_manager = GCSPseudoLockManager(self.PROJECT_ID)
        lock_manager.lock(self.LOCK_NAME)
        with self.assertRaises(GCSPseudoLockDoesNotExist):
            lock_manager.unlock(self.LOCK_NAME2)
        self.assertTrue(lock_manager.is_locked(self.LOCK_NAME))
        self.assertFalse(lock_manager.is_locked(self.LOCK_NAME2))

    def test_lock_twice_unlock_once(self) -> None:
        """Locks one lock and unlocks another, asserts both have correct status/raise errors"""
        lock_manager = GCSPseudoLockManager(self.PROJECT_ID)
        lock_manager.lock(self.LOCK_NAME)
        with self.assertRaises(GCSPseudoLockAlreadyExists):
            lock_manager.lock(self.LOCK_NAME)
        lock_manager.unlock(self.LOCK_NAME)
        self.assertFalse(lock_manager.is_locked(self.LOCK_NAME))

    def test_unlock_never_locked(self) -> None:
        """Unlocks a lock that has never been locked before, check it raises an error and remains locked"""
        lock_manager = GCSPseudoLockManager(self.PROJECT_ID)
        with self.assertRaises(GCSPseudoLockDoesNotExist):
            lock_manager.unlock(self.LOCK_NAME2)
        self.assertFalse(lock_manager.is_locked(self.LOCK_NAME))

    def test_contents_of_lock_default(self) -> None:
        """Locks with default contents and asserts the lockfile contains correct time"""
        lock_manager = GCSPseudoLockManager(self.PROJECT_ID)
        lock_manager.lock(self.LOCK_NAME)
        correct_contents = datetime.now().strftime(self.TIME_FORMAT)
        path = GcsfsFilePath(bucket_name=lock_manager.bucket_name, blob_name=self.LOCK_NAME)
        actual_contents = self.fs.download_as_string(path)
        self.assertEqual(correct_contents, actual_contents)

    def test_contents_of_lock_set(self) -> None:
        """Locks with pre-specified contents and asserts the lockfile contains those contents"""
        lock_manager = GCSPseudoLockManager(self.PROJECT_ID)
        lock_manager.lock(self.LOCK_NAME, self.CONTENTS)
        path = GcsfsFilePath(bucket_name=lock_manager.bucket_name, blob_name=self.LOCK_NAME)
        actual_contents = self.fs.download_as_string(path)
        self.assertEqual(self.CONTENTS, actual_contents)

    def test_contents_of_unlocked_and_relocked(self) -> None:
        """Locks with pre-specified contents and asserts the lockfile contains those contents"""
        lock_manager = GCSPseudoLockManager(self.PROJECT_ID)
        lock_manager.lock(self.LOCK_NAME, self.CONTENTS)
        lock_manager.unlock(self.LOCK_NAME)
        lock_manager.lock(self.LOCK_NAME, self.CONTENTS2)
        path = GcsfsFilePath(bucket_name=lock_manager.bucket_name, blob_name=self.LOCK_NAME)
        actual_contents = self.fs.download_as_string(path)
        self.assertEqual(self.CONTENTS2, actual_contents)
