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
"""Tests for base_upload_state_files_to_ingest_bucket_controller.py"""
import datetime
import unittest
from typing import Optional
from unittest.mock import Mock, create_autospec, patch

import pytest
from mock import call

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.results import MultiRequestResultWithSkipped
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct.direct_ingest_cloud_task_queue_manager import (
    QUEUE_STATE_ENUM,
    DirectIngestCloudTaskQueueManagerImpl,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.sftp.base_upload_state_files_to_ingest_bucket_controller import (
    UploadStateFilesToIngestBucketController,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_postgres_helpers

TODAY = datetime.datetime.today()


@pytest.mark.uses_db
@patch("recidiviz.ingest.direct.direct_ingest_control.GcsfsFactory.build")
@patch.object(
    PostgresDirectIngestRawFileMetadataManager,
    "has_raw_file_been_processed",
    lambda _, path: "skipped" in path.abs_path(),
)
@patch.object(
    PostgresDirectIngestRawFileMetadataManager,
    "has_raw_file_been_discovered",
    lambda _, path: "discovered" in path.abs_path(),
)
class TestUploadStateFilesToIngestBucketController(unittest.TestCase):
    """Tests for UploadStateFilesToIngestBucketController."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.project_id = "recidiviz-456"
        self.region = "us_xx"

        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id

        self.task_manager_patcher = patch(
            "recidiviz.ingest.direct.sftp.base_upload_state_files_to_ingest_bucket_controller.DirectIngestCloudTaskQueueManagerImpl",
            return_value=create_autospec(DirectIngestCloudTaskQueueManagerImpl),
        )
        self.task_manager_cls = self.task_manager_patcher.start()
        self.mock_task_manager = self.task_manager_cls()

        self.operations_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_postgres_helpers.use_on_disk_postgresql_database(self.operations_key)

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.operations_key)
        self.task_manager_patcher.stop()
        self.project_id_patcher.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_do_upload_succeeds(self, mock_fs_factory: Mock) -> None:
        self.mock_task_manager.get_scheduler_queue_state.return_value = (
            QUEUE_STATE_ENUM.RUNNING
        )
        mock_fs = FakeGCSFileSystem()
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "recidiviz-456-direct-ingest-state-us-xx", "raw_data/test_file.txt"
            ),
            local_path=None,
        )
        mock_fs_factory.return_value = mock_fs
        controller = UploadStateFilesToIngestBucketController(
            paths_with_timestamps=[
                (
                    "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt",
                    TODAY,
                )
            ],
            project_id="recidiviz-456",
            region_code="us_xx",
            downloaded_paths_to_remote_files={
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt": "test_file.txt"
            },
        )
        expected_result = [
            "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt"
        ]
        result: MultiRequestResultWithSkipped[str, str, str] = controller.do_upload()
        self.assertEqual(result.successes, expected_result)
        self.assertEqual(len(result.failures), 0)
        self.assertEqual(len(controller.skipped_files), 0)
        with SessionFactory.using_database(self.operations_key) as session:
            db_results = session.query(
                schema.DirectIngestSftpIngestReadyFileMetadata
            ).all()
            self.assertEqual(len(db_results), 1)
            self.assertEqual(db_results[0].remote_file_path, "test_file.txt")
        self.mock_task_manager.update_scheduler_queue_state.assert_has_calls(
            [
                call(
                    StateCode.US_XX,
                    DirectIngestInstance.PRIMARY,
                    QUEUE_STATE_ENUM.PAUSED,
                ),
                call(
                    StateCode.US_XX,
                    DirectIngestInstance.PRIMARY,
                    QUEUE_STATE_ENUM.RUNNING,
                ),
            ]
        )

    def test_do_upload_graceful_failures(
        self,
        mock_fs_factory: Mock,
    ) -> None:
        self.mock_task_manager.get_scheduler_queue_state.return_value = (
            QUEUE_STATE_ENUM.RUNNING
        )
        mock_fs = FakeGCSFileSystem()
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "recidiviz-456-direct-ingest-state-us-xx", "raw_data/test_file.txt"
            ),
            local_path=None,
        )
        mock_fs_factory.return_value = mock_fs
        controller = UploadStateFilesToIngestBucketController(
            paths_with_timestamps=[
                (
                    "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt",
                    TODAY,
                ),
                (
                    "recidiviz-456-direct-ingest-state-us-xx/raw_data/non_existent_file.txt",
                    TODAY,
                ),
            ],
            project_id="recidiviz-456",
            region_code="us_xx",
            downloaded_paths_to_remote_files={
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt": "test_file.txt",
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/non_existent_file.txt": "non_existent_file.txt",
            },
        )
        result: MultiRequestResultWithSkipped[str, str, str] = controller.do_upload()
        self.assertEqual(
            result.successes,
            ["recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt"],
        )
        self.assertEqual(
            result.failures,
            ["recidiviz-456-direct-ingest-state-us-xx/raw_data/non_existent_file.txt"],
        )
        self.assertEqual(len(controller.skipped_files), 0)
        self.mock_task_manager.update_scheduler_queue_state.assert_has_calls(
            [
                call(
                    StateCode.US_XX,
                    DirectIngestInstance.PRIMARY,
                    QUEUE_STATE_ENUM.PAUSED,
                ),
                call(
                    StateCode.US_XX,
                    DirectIngestInstance.PRIMARY,
                    QUEUE_STATE_ENUM.RUNNING,
                ),
            ]
        )

    def test_do_upload_sets_correct_content_type(
        self,
        mock_fs_factory: Mock,
    ) -> None:
        self.mock_task_manager.get_scheduler_queue_state.return_value = (
            QUEUE_STATE_ENUM.RUNNING
        )
        mock_fs = FakeGCSFileSystem()
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "recidiviz-456-direct-ingest-state-us-xx", "raw_data/test_file.txt"
            ),
            local_path=None,
        )
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "recidiviz-456-direct-ingest-state-us-xx", "raw_data/test_file.csv"
            ),
            local_path=None,
        )
        mock_fs_factory.return_value = mock_fs
        controller = UploadStateFilesToIngestBucketController(
            paths_with_timestamps=[
                (
                    "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt",
                    TODAY,
                ),
                (
                    "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.csv",
                    TODAY,
                ),
            ],
            project_id="recidiviz-456",
            region_code="us_xx",
            downloaded_paths_to_remote_files={
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt": "test_file.txt",
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.csv": "test_file.csv",
            },
        )
        result: MultiRequestResultWithSkipped[str, str, str] = controller.do_upload()
        self.assertListEqual(
            sorted(result.successes),
            [
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.csv",
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt",
            ],
        )
        resulting_content_types = [file.content_type for file in mock_fs.files.values()]
        self.assertListEqual(
            sorted(resulting_content_types), ["text/csv", "text/plain"]
        )
        self.mock_task_manager.update_scheduler_queue_state.assert_has_calls(
            [
                call(
                    StateCode.US_XX,
                    DirectIngestInstance.PRIMARY,
                    QUEUE_STATE_ENUM.PAUSED,
                ),
                call(
                    StateCode.US_XX,
                    DirectIngestInstance.PRIMARY,
                    QUEUE_STATE_ENUM.RUNNING,
                ),
            ]
        )

    def test_get_paths_to_upload_is_correct(
        self,
        mock_fs_factory: Mock,
    ) -> None:
        self.mock_task_manager.get_scheduler_queue_state.return_value = (
            QUEUE_STATE_ENUM.RUNNING
        )
        mock_fs = FakeGCSFileSystem()
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "recidiviz-456-direct-ingest-state-us-xx", "raw_data/test_file.txt"
            ),
            local_path=None,
        )
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "recidiviz-456-direct-ingest-state-us-xx",
                "raw_data/subdir1/test_file.txt",
            ),
            local_path=None,
        )
        mock_fs_factory.return_value = mock_fs
        controller = UploadStateFilesToIngestBucketController(
            paths_with_timestamps=[
                ("recidiviz-456-direct-ingest-state-us-xx/raw_data/", TODAY),
            ],
            project_id="recidiviz-456",
            region_code="us_xx",
            downloaded_paths_to_remote_files={
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/": "raw_data.zip"
            },
        )
        result = [
            ("recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt", TODAY),
            (
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/subdir1/test_file.txt",
                TODAY,
            ),
        ]
        self.assertListEqual(result, controller.get_paths_to_upload())
        with SessionFactory.using_database(self.operations_key) as session:
            db_results = session.query(
                schema.DirectIngestSftpIngestReadyFileMetadata
            ).all()
            for db_result in db_results:
                self.assertEqual(db_result.remote_file_path, "raw_data.zip")
        self.mock_task_manager.update_scheduler_queue_state.assert_not_called()

    def test_skip_already_processed_or_discovered_files(
        self,
        mock_fs_factory: Mock,
    ) -> None:
        mock_fs = FakeGCSFileSystem()
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "recidiviz-456-direct-ingest-state-us-xx", "raw_data/test_file.txt"
            ),
            local_path=None,
        )
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "recidiviz-456-direct-ingest-state-us-xx", "raw_data/test_file.csv"
            ),
            local_path=None,
        )

        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "recidiviz-456-direct-ingest-state-us-xx",
                "raw_data/skipped.csv",
            ),
            local_path=None,
        )
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "recidiviz-456-direct-ingest-state-us-xx",
                "raw_data/discovered.csv",
            ),
            local_path=None,
        )
        mock_fs_factory.return_value = mock_fs
        controller = UploadStateFilesToIngestBucketController(
            paths_with_timestamps=[
                (
                    "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt",
                    TODAY,
                ),
                (
                    "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.csv",
                    TODAY,
                ),
                (
                    "recidiviz-456-direct-ingest-state-us-xx/raw_data/skipped.csv",
                    TODAY,
                ),
                (
                    "recidiviz-456-direct-ingest-state-us-xx/raw_data/discovered.csv",
                    TODAY,
                ),
            ],
            project_id="recidiviz-456",
            region_code="us_xx",
            downloaded_paths_to_remote_files={
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt": "test_file.txt",
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.csv": "test_file.csv",
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/skipped.csv": "skipped.csv",
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/discovered.csv": "discovered.csv",
            },
        )
        result: MultiRequestResultWithSkipped[str, str, str] = controller.do_upload()
        self.assertCountEqual(
            result.successes,
            [
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt",
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.csv",
            ],
        )
        self.assertCountEqual(
            result.skipped,
            [
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/skipped.csv",
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/discovered.csv",
            ],
        )

    def test_do_upload_succeeds_ingest_was_paused(self, mock_fs_factory: Mock) -> None:
        self.mock_task_manager.get_scheduler_queue_state.return_value = (
            QUEUE_STATE_ENUM.PAUSED
        )
        mock_fs = FakeGCSFileSystem()
        mock_fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                "recidiviz-456-direct-ingest-state-us-xx", "raw_data/test_file.txt"
            ),
            local_path=None,
        )
        mock_fs_factory.return_value = mock_fs
        controller = UploadStateFilesToIngestBucketController(
            paths_with_timestamps=[
                (
                    "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt",
                    TODAY,
                )
            ],
            project_id="recidiviz-456",
            region_code="us_xx",
            downloaded_paths_to_remote_files={
                "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt": "test_file.txt"
            },
        )
        expected_result = [
            "recidiviz-456-direct-ingest-state-us-xx/raw_data/test_file.txt"
        ]
        result: MultiRequestResultWithSkipped[str, str, str] = controller.do_upload()
        self.assertEqual(result.successes, expected_result)
        self.assertEqual(len(result.failures), 0)
        self.assertEqual(len(controller.skipped_files), 0)
        self.mock_task_manager.update_scheduler_queue_state.assert_not_called()
