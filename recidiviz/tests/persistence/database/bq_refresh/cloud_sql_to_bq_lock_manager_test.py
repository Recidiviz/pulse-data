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
"""Tests for CloudSqlToBQLockManager."""

import unittest
from unittest.mock import Mock, patch

from recidiviz.cloud_storage.gcs_pseudo_lock_manager import (
    GCSPseudoLockAlreadyExists,
    GCSPseudoLockDoesNotExist,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct.controllers.direct_ingest_region_lock_manager import (
    DirectIngestRegionLockManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_lock_manager import (
    CloudSqlToBQLockManager,
)
from recidiviz.persistence.database.schema_type import SchemaType


class CloudSqlToBQLockManagerTest(unittest.TestCase):
    """Tests for CloudSqlToBQLockManager."""

    def setUp(self) -> None:
        self.fake_fs = FakeGCSFileSystem()
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"
        with patch(
            "recidiviz.cloud_storage.gcs_pseudo_lock_manager.GcsfsFactory.build",
            Mock(return_value=self.fake_fs),
        ):
            self.lock_manager = CloudSqlToBQLockManager()
            self.lock_bucket = self.lock_manager.lock_manager.bucket_name
            self.state_ingest_lock_manager = DirectIngestRegionLockManager(
                region_code=StateCode.US_XX.value,
                blocking_locks=[],
                ingest_instance=DirectIngestInstance.PRIMARY,
            )
            self.state_ingest_lock_manager_secondary = DirectIngestRegionLockManager(
                region_code=StateCode.US_XX.value,
                blocking_locks=[],
                ingest_instance=DirectIngestInstance.SECONDARY,
            )

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    def test_acquire_release_new_lock(self) -> None:
        self.lock_manager.acquire_lock(
            lock_id="lock1",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )
        expected_paths = [
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name="EXPORT_PROCESS_RUNNING_STATE_PRIMARY",
            )
        ]
        self.assertEqual(expected_paths, self.fake_fs.all_paths)

        self.lock_manager.release_lock(
            schema_type=SchemaType.STATE, ingest_instance=DirectIngestInstance.PRIMARY
        )
        self.assertEqual([], self.fake_fs.all_paths)

    def test_acquire_release_new_lock_secondary(self) -> None:
        self.lock_manager.acquire_lock(
            lock_id="lock1",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.SECONDARY,
        )
        expected_paths = [
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name="EXPORT_PROCESS_RUNNING_STATE_SECONDARY",
            )
        ]
        self.assertEqual(expected_paths, self.fake_fs.all_paths)

        self.lock_manager.release_lock(
            schema_type=SchemaType.STATE, ingest_instance=DirectIngestInstance.SECONDARY
        )
        self.assertEqual([], self.fake_fs.all_paths)

    def test_acquire_two_locks_different_schemas(self) -> None:
        self.lock_manager.acquire_lock(
            lock_id="lock1",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )
        expected_paths = [
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name="EXPORT_PROCESS_RUNNING_STATE_PRIMARY",
            )
        ]
        self.assertEqual(expected_paths, self.fake_fs.all_paths)

        self.lock_manager.acquire_lock(
            lock_id="lock1",
            schema_type=SchemaType.OPERATIONS,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )
        expected_paths.append(
            GcsfsFilePath(
                bucket_name=self.lock_bucket,
                blob_name="EXPORT_PROCESS_RUNNING_OPERATIONS_PRIMARY",
            )
        )
        self.assertEqual(expected_paths, self.fake_fs.all_paths)

        self.lock_manager.release_lock(
            schema_type=SchemaType.STATE, ingest_instance=DirectIngestInstance.PRIMARY
        )
        self.lock_manager.release_lock(
            schema_type=SchemaType.OPERATIONS,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )
        self.assertEqual([], self.fake_fs.all_paths)

    def test_release_without_acquiring(self) -> None:
        with self.assertRaises(GCSPseudoLockDoesNotExist):
            self.lock_manager.release_lock(
                schema_type=SchemaType.STATE,
                ingest_instance=DirectIngestInstance.PRIMARY,
            )

    def test_acquire_existing(self) -> None:
        self.lock_manager.acquire_lock(
            lock_id="lock1",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )
        with self.assertRaises(GCSPseudoLockAlreadyExists):
            self.lock_manager.acquire_lock(
                lock_id="lock2",
                schema_type=SchemaType.STATE,
                ingest_instance=DirectIngestInstance.PRIMARY,
            )

    def test_acquire_state_cannot_proceed(self) -> None:
        with self.state_ingest_lock_manager.using_region_lock(expiration_in_seconds=10):
            for schema_type in SchemaType:
                self.lock_manager.acquire_lock(
                    lock_id="lock1",
                    schema_type=schema_type,
                    ingest_instance=DirectIngestInstance.PRIMARY,
                )
            self.assertFalse(
                self.lock_manager.can_proceed(
                    SchemaType.STATE, DirectIngestInstance.PRIMARY
                )
            )
            self.assertFalse(
                self.lock_manager.can_proceed(
                    SchemaType.OPERATIONS, DirectIngestInstance.PRIMARY
                )
            )
            # State ingest does not block PATHWAYS export
            self.assertTrue(
                self.lock_manager.can_proceed(
                    SchemaType.PATHWAYS, DirectIngestInstance.PRIMARY
                )
            )
            self.assertTrue(
                self.lock_manager.can_proceed(
                    SchemaType.CASE_TRIAGE, DirectIngestInstance.PRIMARY
                )
            )

        # SECONDARY ingest should not block a primary STATE export
        with self.state_ingest_lock_manager_secondary.using_region_lock(
            expiration_in_seconds=10
        ):
            self.lock_manager.acquire_lock(
                lock_id="lock1",
                schema_type=SchemaType.STATE,
                ingest_instance=DirectIngestInstance.PRIMARY,
            )
            self.assertTrue(
                self.lock_manager.can_proceed(
                    SchemaType.STATE, DirectIngestInstance.PRIMARY
                )
            )

        # Acquiring the same state export lock again does not crash
        self.lock_manager.acquire_lock(
            lock_id="lock1",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        # Now that the ingest lock has been released, all export jobs can proceed
        for schema_type in SchemaType:
            self.assertTrue(
                self.lock_manager.can_proceed(schema_type, DirectIngestInstance.PRIMARY)
            )

    def test_acquire_primary_can_proceed_while_secondary_ingest_locks_active(
        self,
    ) -> None:
        with self.state_ingest_lock_manager_secondary.using_region_lock(
            expiration_in_seconds=10
        ):
            for schema_type in SchemaType:
                self.lock_manager.acquire_lock(
                    lock_id="lock1",
                    schema_type=schema_type,
                    ingest_instance=DirectIngestInstance.PRIMARY,
                )
            self.assertTrue(
                self.lock_manager.can_proceed(
                    SchemaType.STATE, DirectIngestInstance.PRIMARY
                )
            )
            self.assertTrue(
                self.lock_manager.can_proceed(
                    SchemaType.OPERATIONS, DirectIngestInstance.PRIMARY
                )
            )
            # State ingest does not block PATHWAYS export
            self.assertTrue(
                self.lock_manager.can_proceed(
                    SchemaType.PATHWAYS, DirectIngestInstance.PRIMARY
                )
            )
            self.assertTrue(
                self.lock_manager.can_proceed(
                    SchemaType.CASE_TRIAGE, DirectIngestInstance.PRIMARY
                )
            )

        # Acquiring the same state export lock again does not crash
        self.lock_manager.acquire_lock(
            lock_id="lock1",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

    def test_acquire_secondary_can_proceed_while_primary_ingest_locks_active(
        self,
    ) -> None:
        with self.state_ingest_lock_manager.using_region_lock(expiration_in_seconds=10):
            for schema_type in SchemaType:
                self.lock_manager.acquire_lock(
                    lock_id="lock1",
                    schema_type=schema_type,
                    ingest_instance=DirectIngestInstance.SECONDARY,
                )
            # SECONDARY state export should not be blocked by a primary ingest lock
            self.assertTrue(
                self.lock_manager.can_proceed(
                    SchemaType.STATE, DirectIngestInstance.SECONDARY
                )
            )
            self.assertTrue(
                self.lock_manager.can_proceed(
                    SchemaType.OPERATIONS, DirectIngestInstance.SECONDARY
                )
            )
            # State ingest does not block PATHWAYS export
            self.assertTrue(
                self.lock_manager.can_proceed(
                    SchemaType.PATHWAYS, DirectIngestInstance.SECONDARY
                )
            )
            self.assertTrue(
                self.lock_manager.can_proceed(
                    SchemaType.CASE_TRIAGE, DirectIngestInstance.SECONDARY
                )
            )

        # Acquiring the same state export lock again does not crash
        self.lock_manager.acquire_lock(
            lock_id="lock1",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.SECONDARY,
        )

        # Now that the ingest lock has been released, all export jobs can proceed
        for schema_type in SchemaType:
            self.assertTrue(
                self.lock_manager.can_proceed(
                    schema_type, DirectIngestInstance.SECONDARY
                )
            )

    def test_can_proceed_without_acquiring(self) -> None:
        with self.assertRaises(GCSPseudoLockDoesNotExist):
            self.lock_manager.can_proceed(
                schema_type=SchemaType.STATE,
                ingest_instance=DirectIngestInstance.PRIMARY,
            )

    def test_acquire_operations_cannot_proceed_state_ingest_locks_active(self) -> None:
        with self.state_ingest_lock_manager.using_region_lock(expiration_in_seconds=10):
            for schema_type in SchemaType:
                self.lock_manager.acquire_lock(
                    lock_id="lock1",
                    schema_type=schema_type,
                    ingest_instance=DirectIngestInstance.PRIMARY,
                )
            self.assertFalse(
                self.lock_manager.can_proceed(
                    SchemaType.OPERATIONS, DirectIngestInstance.PRIMARY
                )
            )

        # Acquiring the same operations export lock again does not crash
        self.lock_manager.acquire_lock(
            lock_id="lock1",
            schema_type=SchemaType.OPERATIONS,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

    def test_acquire_operations_can_proceed_raw_file_import_lock_active(self) -> None:
        with self.state_ingest_lock_manager.using_raw_file_lock(
            raw_file_tag="my_tag", expiration_in_seconds=10
        ):
            for schema_type in SchemaType:
                self.lock_manager.acquire_lock(
                    lock_id="lock1",
                    schema_type=schema_type,
                    ingest_instance=DirectIngestInstance.PRIMARY,
                )
            self.assertTrue(
                self.lock_manager.can_proceed(
                    SchemaType.OPERATIONS, DirectIngestInstance.PRIMARY
                )
            )

        # Acquiring the same operations export lock again does not crash
        self.lock_manager.acquire_lock(
            lock_id="lock1",
            schema_type=SchemaType.OPERATIONS,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )
