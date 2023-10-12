# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Manages acquiring and releasing the lock for the state update processes."""

import unittest
from unittest.mock import Mock, patch

from recidiviz.cloud_storage.gcs_pseudo_lock_manager import GCSPseudoLockDoesNotExist
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.state_update_lock_manager import (
    StateUpdateLockManager,
    state_update_lock_name,
)


class StateUpdateLockManagerTest(unittest.TestCase):
    """Tests for StateUpdateLockManager."""

    def setUp(self) -> None:
        self.fake_fs = FakeGCSFileSystem()
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"

        self.sleep_patcher = patch("time.sleep", side_effect=lambda _: None)
        self.mock_sleep = self.sleep_patcher.start()
        with patch(
            "recidiviz.cloud_storage.gcs_pseudo_lock_manager.GcsfsFactory.build",
            Mock(return_value=self.fake_fs),
        ):
            self.us_xx_primary_lock_manager = StateUpdateLockManager(
                DirectIngestInstance.PRIMARY, StateCode.US_XX
            )
            self.us_xx_secondary_lock_manager = StateUpdateLockManager(
                DirectIngestInstance.SECONDARY, StateCode.US_XX
            )
            self.us_yy_primary_lock_manager = StateUpdateLockManager(
                DirectIngestInstance.PRIMARY, StateCode.US_YY
            )
            self.state_agnostic_primary_lock_manager = StateUpdateLockManager(
                DirectIngestInstance.PRIMARY
            )
            self.lock_bucket = self.us_xx_primary_lock_manager.lock_manager.bucket_name

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        self.sleep_patcher.stop()

    def test_acquire_release_new_lock(self) -> None:
        self.us_xx_primary_lock_manager.acquire_lock(lock_id="lock_id1")
        expected_paths = [
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name=state_update_lock_name(
                    DirectIngestInstance.PRIMARY, StateCode.US_XX
                ),
            )
        ]
        self.assertEqual(expected_paths, self.fake_fs.all_paths)

        self.us_xx_primary_lock_manager.release_lock(lock_id="lock_id1")
        self.assertEqual([], self.fake_fs.all_paths)

    def test_release_lock_with_wrong_lock_id(self) -> None:
        self.us_xx_primary_lock_manager.acquire_lock(lock_id="lock_id1")
        expected_paths = [
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name=state_update_lock_name(
                    DirectIngestInstance.PRIMARY, StateCode.US_XX
                ),
            )
        ]
        self.assertEqual(expected_paths, self.fake_fs.all_paths)

        with self.assertRaisesRegex(
            ValueError, "Lock id.* does not match existing lock's lock id .*"
        ):
            self.us_xx_primary_lock_manager.release_lock(lock_id="lock_id2")
        self.assertEqual(expected_paths, self.fake_fs.all_paths)
        self.us_xx_primary_lock_manager.release_lock(lock_id="lock_id1")

    def test_release_without_acquiring(self) -> None:
        with self.assertRaises(GCSPseudoLockDoesNotExist):
            self.us_xx_primary_lock_manager.release_lock(lock_id="lock_id1")

    def test_acquire_existing_timeout(self) -> None:
        self.us_xx_primary_lock_manager.acquire_lock(lock_id="lock_id1")
        with self.assertRaisesRegex(
            ValueError, "Could not acquire lock after waiting.*"
        ):
            self.us_xx_primary_lock_manager.acquire_lock(lock_id="lock_id2")
        self.us_xx_primary_lock_manager.release_lock(lock_id="lock_id1")

    def test_general_lock_held_state_specific_attempted(self) -> None:
        self.mock_sleep.side_effect = (
            lambda _: self.state_agnostic_primary_lock_manager.release_lock(
                lock_id="lock_id1"
            )
            if self.state_agnostic_primary_lock_manager.is_locked()
            else None
        )
        self.state_agnostic_primary_lock_manager.acquire_lock("lock_id1")
        self.us_xx_primary_lock_manager.acquire_lock("lock_id2")
        expected_paths = [
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name=state_update_lock_name(
                    DirectIngestInstance.PRIMARY, StateCode.US_XX
                ),
            )
        ]
        self.assertEqual(expected_paths, self.fake_fs.all_paths)
        self.mock_sleep.assert_called()
        self.us_xx_primary_lock_manager.release_lock(lock_id="lock_id2")

    def test_state_specific_acquired_general_attempted(self) -> None:
        self.mock_sleep.side_effect = (
            lambda _: self.us_xx_primary_lock_manager.release_lock(lock_id="lock_id1")
            if self.us_xx_primary_lock_manager.is_locked()
            else None
        )
        self.us_xx_primary_lock_manager.acquire_lock("lock_id1")
        self.state_agnostic_primary_lock_manager.acquire_lock("lock_id2")
        expected_paths = [
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name=state_update_lock_name(DirectIngestInstance.PRIMARY, None),
            )
        ]
        self.assertEqual(expected_paths, self.fake_fs.all_paths)
        self.mock_sleep.assert_called()
        self.state_agnostic_primary_lock_manager.release_lock(lock_id="lock_id2")

    def test_state_specific_acquired_another_state_specific_attempted(self) -> None:
        self.us_xx_primary_lock_manager.acquire_lock("lock_id1")
        self.us_yy_primary_lock_manager.acquire_lock("lock_id2")
        expected_paths = [
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name=state_update_lock_name(
                    DirectIngestInstance.PRIMARY, StateCode.US_XX
                ),
            ),
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name=state_update_lock_name(
                    DirectIngestInstance.PRIMARY, StateCode.US_YY
                ),
            ),
        ]
        self.assertEqual(expected_paths, self.fake_fs.all_paths)
        self.mock_sleep.assert_not_called()
        self.us_xx_primary_lock_manager.release_lock(lock_id="lock_id1")
        self.us_yy_primary_lock_manager.release_lock(lock_id="lock_id2")

    def test_state_specific_acquired_secondary_state_specific_attempted(self) -> None:
        self.us_xx_primary_lock_manager.acquire_lock("lock_id1")
        self.us_xx_secondary_lock_manager.acquire_lock("lock_id2")
        expected_paths = [
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name=state_update_lock_name(
                    DirectIngestInstance.PRIMARY, StateCode.US_XX
                ),
            ),
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name=state_update_lock_name(
                    DirectIngestInstance.SECONDARY, StateCode.US_XX
                ),
            ),
        ]
        self.assertEqual(expected_paths, self.fake_fs.all_paths)
        self.mock_sleep.assert_not_called()
        self.us_xx_primary_lock_manager.release_lock(lock_id="lock_id1")
        self.us_xx_secondary_lock_manager.release_lock(lock_id="lock_id2")

    def test_secondary_state_specific_acquired_primary_agnostic_state_attempted(
        self,
    ) -> None:
        self.us_xx_secondary_lock_manager.acquire_lock("lock_id1")
        self.state_agnostic_primary_lock_manager.acquire_lock("lock_id2")
        expected_paths = [
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name=state_update_lock_name(
                    DirectIngestInstance.SECONDARY, StateCode.US_XX
                ),
            ),
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name=state_update_lock_name(DirectIngestInstance.PRIMARY, None),
            ),
        ]
        self.assertEqual(expected_paths, self.fake_fs.all_paths)
        self.mock_sleep.assert_not_called()
        self.us_xx_secondary_lock_manager.release_lock(lock_id="lock_id1")
        self.state_agnostic_primary_lock_manager.release_lock(lock_id="lock_id2")

    def test_primary_agnostic_state_acquired_secondary_state_specific_attempted(
        self,
    ) -> None:
        self.state_agnostic_primary_lock_manager.acquire_lock("lock_id1")
        self.us_xx_secondary_lock_manager.acquire_lock("lock_id2")
        expected_paths = [
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name=state_update_lock_name(DirectIngestInstance.PRIMARY, None),
            ),
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name=state_update_lock_name(
                    DirectIngestInstance.SECONDARY, StateCode.US_XX
                ),
            ),
        ]
        self.assertEqual(expected_paths, self.fake_fs.all_paths)
        self.mock_sleep.assert_not_called()
        self.state_agnostic_primary_lock_manager.release_lock(lock_id="lock_id1")
        self.us_xx_secondary_lock_manager.release_lock(lock_id="lock_id2")
