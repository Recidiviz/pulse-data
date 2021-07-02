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
"""Tests for DirectIngestRegionLockManager."""
import time
import unittest
from unittest.mock import Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.ingest.direct.controllers.direct_ingest_region_lock_manager import (
    DirectIngestRegionLockManager,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class DirectIngestRegionLockManagerTest(unittest.TestCase):
    """Tests for DirectIngestRegionLockManager."""

    def setUp(self) -> None:
        self.fake_fs = FakeGCSFileSystem()
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"

        self.blocking_locks = ["blocking_lock1", "blocking_lock2"]
        with patch(
            "recidiviz.cloud_storage.gcs_pseudo_lock_manager.GcsfsFactory.build",
            Mock(return_value=self.fake_fs),
        ):
            self.lock_manager = DirectIngestRegionLockManager(
                region_code=StateCode.US_XX.value,
                blocking_locks=self.blocking_locks,
                ingest_instance=DirectIngestInstance.PRIMARY,
            )

            self.lock_manager_secondary = DirectIngestRegionLockManager(
                region_code=StateCode.US_XX.value,
                blocking_locks=self.blocking_locks,
                ingest_instance=DirectIngestInstance.SECONDARY,
            )

            self.lock_manager_other_region = DirectIngestRegionLockManager(
                region_code=StateCode.US_WW.value,
                blocking_locks=[],
                ingest_instance=DirectIngestInstance.PRIMARY,
            )

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    def test_locking(self) -> None:
        self.assertFalse(self.lock_manager.is_locked())
        with self.lock_manager.using_region_lock(expiration_in_seconds=10):
            self.assertTrue(self.lock_manager.is_locked())
            self.assertFalse(self.lock_manager_other_region.is_locked())
            with self.lock_manager_other_region.using_region_lock(
                expiration_in_seconds=10
            ):
                self.assertTrue(self.lock_manager_other_region.is_locked())
            self.assertFalse(self.lock_manager_other_region.is_locked())
        self.assertFalse(self.lock_manager.is_locked())

    def test_locking_different_instances(self) -> None:
        self.assertFalse(self.lock_manager.is_locked())
        with self.lock_manager.using_region_lock(expiration_in_seconds=10):
            self.assertTrue(self.lock_manager.is_locked())
            self.assertFalse(self.lock_manager_secondary.is_locked())
            with self.lock_manager_secondary.using_region_lock(
                expiration_in_seconds=10
            ):
                self.assertTrue(self.lock_manager_secondary.is_locked())
            self.assertFalse(self.lock_manager_secondary.is_locked())
            self.assertTrue(self.lock_manager.is_locked())
        self.assertFalse(self.lock_manager.is_locked())

    def test_locking_expiration(self) -> None:
        self.assertFalse(self.lock_manager.is_locked())
        with self.lock_manager.using_region_lock(expiration_in_seconds=1):
            self.assertTrue(self.lock_manager.is_locked())
            time.sleep(1.5)
            self.assertFalse(self.lock_manager.is_locked())
        self.assertFalse(self.lock_manager.is_locked())

    def test_can_proceed(self) -> None:
        self.assertTrue(self.lock_manager.can_proceed())
        self.lock_manager.lock_manager.lock(self.blocking_locks[0])
        self.assertFalse(self.lock_manager.can_proceed())

        self.lock_manager.lock_manager.unlock(self.blocking_locks[0])
        self.lock_manager.lock_manager.lock(self.blocking_locks[1])

        self.assertFalse(self.lock_manager.can_proceed())

        self.lock_manager.lock_manager.unlock(self.blocking_locks[1])
        self.assertTrue(self.lock_manager.can_proceed())
