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

"""Tests for ingest/direct_control.py."""
import datetime
import json
import unittest
from collections import defaultdict
from http import HTTPStatus
from typing import Any, Tuple
from unittest import mock

from flask import Flask
from mock import Mock, call, create_autospec, patch
from paramiko.hostkeys import HostKeyEntry

from recidiviz.cloud_storage.gcs_pseudo_lock_manager import (
    GCSPseudoLockBody,
    GCSPseudoLockManager,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.results import MultiRequestResultWithSkipped
from recidiviz.common.sftp_connection import RecidivizSftpConnection
from recidiviz.ingest.direct import direct_ingest_control
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.direct_ingest_bucket_name_utils import (
    build_ingest_bucket_name,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    DirectIngestCloudTaskManager,
    DirectIngestCloudTaskManagerImpl,
)
from recidiviz.ingest.direct.direct_ingest_control import kick_all_schedulers
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
    to_normalized_unprocessed_raw_file_path,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
)
from recidiviz.ingest.direct.sftp.base_sftp_download_delegate import (
    BaseSftpDownloadDelegate,
)
from recidiviz.ingest.direct.sftp.base_upload_state_files_to_ingest_bucket_controller import (
    UploadStateFilesToIngestBucketController,
)
from recidiviz.ingest.direct.sftp.download_files_from_sftp import (
    DownloadFilesFromSftpController,
    SftpAuth,
)
from recidiviz.ingest.direct.types.cloud_task_args import (
    ExtractAndMergeArgs,
    GcsfsRawDataBQImportArgs,
    IngestViewMaterializationArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.errors import (
    DirectIngestError,
    DirectIngestErrorType,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.regions import Region

CONTROL_PACKAGE_NAME = direct_ingest_control.__name__
TODAY = datetime.datetime.today()

APP_ENGINE_HEADERS = {
    "X-Appengine-Cron": "test-cron",
    "X-AppEngine-TaskName": "my-task-id",
}


@patch("recidiviz.utils.metadata.project_number", Mock(return_value="123456789"))
class TestDirectIngestControl(unittest.TestCase):
    """Tests for requests to the Direct Ingest API."""

    def setUp(self) -> None:
        app = Flask(__name__)
        app.register_blueprint(direct_ingest_control.direct_ingest_control)
        app.config["TESTING"] = True
        self.client = app.test_client()

        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-project"
        self.bq_client_patcher = patch("google.cloud.bigquery.Client")
        self.storage_client_patcher = patch("google.cloud.storage.Client")
        self.fake_fs = FakeGCSFileSystem()

        def mock_build_fs() -> FakeGCSFileSystem:
            return self.fake_fs

        self.fs_patcher = patch.object(GcsfsFactory, "build", new=mock_build_fs)

        self.bq_client_patcher.start()
        self.storage_client_patcher.start()
        self.fs_patcher.start()

        self.controller_factory_patcher: Any = patch(
            f"{CONTROL_PACKAGE_NAME}.DirectIngestControllerFactory"
        )
        self.mock_controller_factory = self.controller_factory_patcher.start()

        self.task_manager_patcher = patch(
            f"{CONTROL_PACKAGE_NAME}.DirectIngestCloudTaskManagerImpl"
        )
        self.mock_task_manager = create_autospec(DirectIngestCloudTaskManagerImpl)
        self.task_manager_patcher.start().return_value = self.mock_task_manager

        self.region_code = "us_nd"
        self.primary_bucket = gcsfs_direct_ingest_bucket_for_state(
            region_code=self.region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        self.bq_client_patcher.stop()
        self.storage_client_patcher.stop()
        self.fs_patcher.stop()
        if self.controller_factory_patcher:
            self.controller_factory_patcher.stop()

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_schedule(
        self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock
    ) -> None:
        """Tests that the start operation chains together the correct calls."""

        mock_controller = create_autospec(BaseDirectIngestController)
        self.mock_controller_factory.build.return_value = mock_controller
        mock_region.return_value = fake_region(environment="production")
        mock_environment.return_value = "production"

        request_args = {
            "region": self.region_code,
            "ingest_instance": DirectIngestInstance.PRIMARY.value,
        }
        task_id = "us_nd-primary-scheduler-b1f5a25c-07d2-408e-b9e9-2825be145263"
        headers = {
            **APP_ENGINE_HEADERS,
            "X-AppEngine-TaskName": task_id,
        }
        response = self.client.get(
            "/scheduler", query_string=request_args, headers=headers
        )
        self.assertEqual(200, response.status_code)

        self.mock_controller_factory.build.assert_called_with(
            region_code=self.region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            allow_unlaunched=False,
        )
        mock_controller.schedule_next_ingest_task.assert_called_with(
            current_task_id=task_id, just_finished_job=False
        )

    @patch("recidiviz.utils.regions.get_region")
    def test_schedule_build_controller_throws_input_error(
        self, mock_region: mock.MagicMock
    ) -> None:
        mock_controller = create_autospec(BaseDirectIngestController)

        self.mock_controller_factory.build.side_effect = DirectIngestError(
            msg="Test bad input error",
            error_type=DirectIngestErrorType.INPUT_ERROR,
        )

        mock_region.return_value = fake_region(
            environment="staging", region_code=self.region_code
        )

        request_args = {
            "region": self.region_code,
            "ingest_instance": DirectIngestInstance.PRIMARY.value,
        }
        headers = APP_ENGINE_HEADERS
        response = self.client.get(
            "/scheduler", query_string=request_args, headers=headers
        )

        self.mock_controller_factory.build.assert_called_with(
            region_code=self.region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            allow_unlaunched=False,
        )

        self.assertEqual(400, response.status_code)
        self.assertEqual(
            response.get_data().decode(),
            "Test bad input error",
        )
        mock_controller.schedule_next_ingest_task.assert_not_called()

    @patch("recidiviz.utils.regions.get_region")
    def test_extract_and_merge(self, mock_region: mock.MagicMock) -> None:
        mock_controller = create_autospec(BaseDirectIngestController)
        self.mock_controller_factory.build.return_value = mock_controller
        mock_region.return_value = fake_region(
            region_code=self.region_code, environment="staging"
        )

        dt = datetime.datetime(year=2019, month=6, day=20)
        ingest_view_name = "myIngestViewName"
        ingest_args = ExtractAndMergeArgs(
            ingest_time=datetime.datetime(year=2019, month=7, day=20),
            ingest_view_name=ingest_view_name,
            ingest_instance=DirectIngestInstance.PRIMARY,
            upper_bound_datetime_inclusive=dt,
            batch_number=2,
        )
        request_args = {
            "region": self.region_code,
            "ingest_view_name": ingest_view_name,
            "ingest_instance": "primary",
        }
        body = {
            "cloud_task_args": ingest_args.to_serializable(),
            "args_type": "ExtractAndMergeArgs",
        }
        body_encoded = json.dumps(body).encode()

        headers = APP_ENGINE_HEADERS

        response = self.client.post(
            "/extract_and_merge",
            query_string=request_args,
            headers=headers,
            data=body_encoded,
        )
        self.assertEqual(200, response.status_code)
        mock_controller.run_extract_and_merge_job_and_kick_scheduler_on_completion.assert_called_with(
            ingest_args
        )

    @patch("recidiviz.utils.regions.get_region")
    def test_extract_and_merge_mismatch_instance(
        self, mock_region: mock.MagicMock
    ) -> None:
        mock_controller = create_autospec(BaseDirectIngestController)
        self.mock_controller_factory.build.return_value = mock_controller
        mock_region.return_value = fake_region(
            region_code=self.region_code, environment="staging"
        )

        dt = datetime.datetime(year=2019, month=6, day=20)
        ingest_view_name = "myIngestViewName"
        ingest_args = ExtractAndMergeArgs(
            ingest_time=datetime.datetime(year=2019, month=7, day=20),
            ingest_view_name=ingest_view_name,
            ingest_instance=DirectIngestInstance.PRIMARY,
            upper_bound_datetime_inclusive=dt,
            batch_number=2,
        )
        request_args = {
            "region": self.region_code,
            "ingest_view_name": ingest_view_name,
            "ingest_instance": "secondary",
        }
        body = {
            "cloud_task_args": ingest_args.to_serializable(),
            "args_type": "ExtractAndMergeArgs",
        }
        body_encoded = json.dumps(body).encode()

        headers = APP_ENGINE_HEADERS

        response = self.client.post(
            "/extract_and_merge",
            query_string=request_args,
            headers=headers,
            data=body_encoded,
        )
        self.assertEqual(400, response.status_code)
        mock_controller.run_extract_and_merge_job_and_kick_scheduler_on_completion.assert_not_called()

    @patch("recidiviz.utils.regions.get_region")
    def test_extract_and_merge_mismatch_ingest_view(
        self, mock_region: mock.MagicMock
    ) -> None:
        mock_controller = create_autospec(BaseDirectIngestController)
        self.mock_controller_factory.build.return_value = mock_controller
        mock_region.return_value = fake_region(
            region_code=self.region_code, environment="staging"
        )

        dt = datetime.datetime(year=2019, month=6, day=20)
        ingest_view_name = "myIngestViewName"
        ingest_args = ExtractAndMergeArgs(
            ingest_time=datetime.datetime(year=2019, month=7, day=20),
            ingest_view_name=ingest_view_name,
            ingest_instance=DirectIngestInstance.PRIMARY,
            upper_bound_datetime_inclusive=dt,
            batch_number=2,
        )
        request_args = {
            "region": self.region_code,
            "ingest_view_name": "another_ingest_view_name",
            "ingest_instance": "primary",
        }
        body = {
            "cloud_task_args": ingest_args.to_serializable(),
            "args_type": "ExtractAndMergeArgs",
        }
        body_encoded = json.dumps(body).encode()

        headers = APP_ENGINE_HEADERS

        response = self.client.post(
            "/extract_and_merge",
            query_string=request_args,
            headers=headers,
            data=body_encoded,
        )
        self.assertEqual(400, response.status_code)
        mock_controller.run_extract_and_merge_job_and_kick_scheduler_on_completion.assert_not_called()

    @patch("recidiviz.utils.regions.get_region")
    def test_extract_and_merge_bad_args_type(self, mock_region: mock.MagicMock) -> None:
        mock_controller = create_autospec(BaseDirectIngestController)
        self.mock_controller_factory.build.return_value = mock_controller
        mock_region.return_value = fake_region(
            region_code=self.region_code, environment="staging"
        )

        bucket_name = build_ingest_bucket_name(
            project_id="recidiviz-xxx",
            region_code=self.region_code,
            suffix="",
        )

        file_path = to_normalized_unprocessed_raw_file_path(
            f"{bucket_name}/ingest_view_name.csv"
        )
        ingest_args = GcsfsRawDataBQImportArgs(
            raw_data_file_path=GcsfsFilePath.from_absolute_path(file_path)
        )
        body = {
            "cloud_task_args": ingest_args.to_serializable(),
            "args_type": "GcsfsRawDataBQImportArgs",
        }
        request_args = {
            "region": self.region_code,
            "ingest_view_name": "ingest_view_name",
            "ingest_instance": "primary",
        }
        body_encoded = json.dumps(body).encode()

        headers = APP_ENGINE_HEADERS

        response = self.client.post(
            "/extract_and_merge",
            query_string=request_args,
            headers=headers,
            data=body_encoded,
        )
        self.assertEqual(400, response.status_code)
        mock_controller.run_extract_and_merge_job_and_kick_scheduler_on_completion.assert_not_called()

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_file_no_start_ingest(
        self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock
    ) -> None:
        mock_environment.return_value = "production"
        mock_controller = create_autospec(BaseDirectIngestController)
        self.mock_controller_factory.build.return_value = mock_controller
        mock_region.return_value = fake_region(
            region_code=self.region_code, environment="production"
        )

        path = GcsfsFilePath.from_directory_and_file_name(
            self.primary_bucket, "Elite_Offenders.csv"
        )

        request_args = {
            "start_ingest": "false",
        }
        pubsub_message = {
            "message": {
                "attributes": {
                    "bucketId": path.bucket_name,
                    "objectId": path.blob_name,
                },
            }
        }
        headers = APP_ENGINE_HEADERS
        response = self.client.post(
            "/handle_direct_ingest_file",
            query_string=request_args,
            headers=headers,
            json=pubsub_message,
        )

        mock_controller.handle_file.assert_called_with(path, False)

        # Even though the region isn't supported, we don't crash
        self.assertEqual(200, response.status_code)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_file_start_ingest(
        self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock
    ) -> None:
        mock_environment.return_value = "production"
        mock_controller = create_autospec(BaseDirectIngestController)
        self.mock_controller_factory.build.return_value = mock_controller
        mock_region.return_value = fake_region(
            region_code=self.region_code, environment="production"
        )

        path = GcsfsFilePath.from_directory_and_file_name(
            self.primary_bucket, "elite_offenders.csv"
        )

        request_args = {
            "start_ingest": "True",
        }
        pubsub_message = {
            "message": {
                "attributes": {
                    "bucketId": path.bucket_name,
                    "objectId": path.blob_name,
                },
            }
        }
        headers = APP_ENGINE_HEADERS
        response = self.client.post(
            "/handle_direct_ingest_file",
            query_string=request_args,
            headers=headers,
            json=pubsub_message,
        )

        mock_controller.handle_file.assert_called_with(path, True)

        # Even though the region isn't supported, we don't crash
        self.assertEqual(200, response.status_code)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_file_start_ingest_unsupported_region(
        self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock
    ) -> None:
        mock_environment.return_value = "production"
        mock_controller = create_autospec(BaseDirectIngestController)
        self.mock_controller_factory.build.return_value = mock_controller
        mock_region.return_value = fake_region(
            region_code=self.region_code, environment="staging"
        )

        path = GcsfsFilePath.from_directory_and_file_name(
            self.primary_bucket, "elite_offenders.csv"
        )

        request_args = {
            "start_ingest": "False",
        }
        pubsub_message = {
            "message": {
                "attributes": {
                    "bucketId": path.bucket_name,
                    "objectId": path.blob_name,
                },
            }
        }
        headers = APP_ENGINE_HEADERS

        response = self.client.post(
            "/handle_direct_ingest_file",
            query_string=request_args,
            headers=headers,
            json=pubsub_message,
        )

        mock_controller.handle_file.assert_called_with(path, False)

        # Even though the region isn't supported, we don't crash - the
        # controller handles not starting ingest, and if it does by accident,
        # the actual schedule/extract_and_merge endpoints handle the unlaunched
        # region check.
        self.assertEqual(200, response.status_code)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_files_start_ingest(
        self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock
    ) -> None:
        mock_environment.return_value = "production"
        mock_controller = create_autospec(BaseDirectIngestController)
        self.mock_controller_factory.build.return_value = mock_controller
        mock_region.return_value = fake_region(
            region_code=self.region_code, environment="production"
        )
        request_args = {
            "region": self.region_code,
            "ingest_instance": DirectIngestInstance.PRIMARY.value,
            "can_start_ingest": "True",
        }
        task_id = "us_nd-primary-handle_new_files-b1f5a25c-07d2-408e-b9e9-2825be145263"
        headers = {
            **APP_ENGINE_HEADERS,
            "X-AppEngine-TaskName": task_id,
        }
        response = self.client.get(
            "/handle_new_files", query_string=request_args, headers=headers
        )

        self.assertEqual(200, response.status_code)
        self.mock_controller_factory.build.assert_called_with(
            region_code=self.region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            allow_unlaunched=True,
        )
        mock_controller.handle_new_files.assert_called_with(
            current_task_id=task_id, can_start_ingest=True
        )

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_files_no_start_ingest(
        self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock
    ) -> None:
        mock_environment.return_value = "staging"
        mock_controller = create_autospec(BaseDirectIngestController)
        self.mock_controller_factory.build.return_value = mock_controller
        mock_region.return_value = fake_region(
            region_code=self.region_code, environment="staging"
        )
        request_args = {
            "region": self.region_code,
            "ingest_instance": DirectIngestInstance.PRIMARY.value,
            "can_start_ingest": "False",
        }
        task_id = "us_nd-primary-handle_new_files-b1f5a25c-07d2-408e-b9e9-2825be145263"
        headers = {
            **APP_ENGINE_HEADERS,
            "X-AppEngine-TaskName": task_id,
        }
        response = self.client.get(
            "/handle_new_files", query_string=request_args, headers=headers
        )

        self.assertEqual(200, response.status_code)
        self.mock_controller_factory.build.assert_called_with(
            region_code=self.region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            allow_unlaunched=True,
        )
        mock_controller.handle_new_files.assert_called_with(
            current_task_id=task_id, can_start_ingest=False
        )

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.ingest.direct.direct_ingest_control.GcsfsFactory.build")
    def test_normalize_file_path(
        self, mock_fs_factory: mock.MagicMock, mock_environment: mock.MagicMock
    ) -> None:

        mock_environment.return_value = "production"
        mock_fs = FakeGCSFileSystem()
        mock_fs_factory.return_value = mock_fs

        path = GcsfsFilePath.from_absolute_path("bucket-us-xx/file-tag.csv")

        mock_fs.test_add_path(path, local_path=None)

        pubsub_message = {
            "message": {
                "attributes": {
                    "bucketId": path.bucket_name,
                    "objectId": path.blob_name,
                },
            }
        }
        headers = APP_ENGINE_HEADERS
        response = self.client.post(
            "/normalize_raw_file_path", headers=headers, json=pubsub_message
        )

        self.assertEqual(200, response.status_code)

        self.assertEqual(1, len(mock_fs.all_paths))
        registered_path = mock_fs.all_paths[0]
        if not isinstance(registered_path, GcsfsFilePath):
            self.fail(f"Unexpected type for path [{type(registered_path)}]")
        self.assertTrue(
            DirectIngestGCSFileSystem.is_normalized_file_path(registered_path)
        )

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.ingest.direct.direct_ingest_control.GcsfsFactory.build")
    def test_normalize_file_path_does_not_change_already_normalized(
        self, mock_fs_factory: mock.MagicMock, mock_environment: mock.MagicMock
    ) -> None:

        mock_environment.return_value = "production"
        mock_fs = FakeGCSFileSystem()
        mock_fs_factory.return_value = mock_fs

        path = GcsfsFilePath.from_absolute_path("bucket-us-xx/file-tag.csv")
        fs = DirectIngestGCSFileSystem(mock_fs)

        mock_fs.test_add_path(path, local_path=None)
        normalized_path = fs.mv_raw_file_to_normalized_path(path)

        pubsub_message = {
            "message": {
                "attributes": {
                    "bucketId": normalized_path.bucket_name,
                    "objectId": normalized_path.blob_name,
                },
            }
        }

        headers = APP_ENGINE_HEADERS
        response = self.client.post(
            "/normalize_raw_file_path", headers=headers, json=pubsub_message
        )

        self.assertEqual(200, response.status_code)

        self.assertEqual(1, len(mock_fs.all_paths))
        registered_path = mock_fs.all_paths[0]
        if not isinstance(registered_path, GcsfsFilePath):
            self.fail(f"Unexpected type for path [{type(registered_path)}]")
        self.assertTrue(
            DirectIngestGCSFileSystem.is_normalized_file_path(registered_path)
        )
        # No change!
        self.assertEqual(registered_path, normalized_path)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_new_files_no_start_ingest_in_production(
        self, mock_region: mock.MagicMock, mock_environment: mock.MagicMock
    ) -> None:
        """Tests that handle_new_files will run and rename files in unlaunched locations, but will not schedule a job to
        process any files."""
        mock_environment.return_value = "production"
        mock_controller = create_autospec(BaseDirectIngestController)
        self.mock_controller_factory.build.return_value = mock_controller
        mock_region.return_value = fake_region(
            region_code=self.region_code, environment="staging"
        )
        request_args = {
            "region": self.region_code,
            "ingest_instance": DirectIngestInstance.PRIMARY.value,
            "can_start_ingest": "False",
        }
        task_id = "us_nd-primary-handle_new_files-b1f5a25c-07d2-408e-b9e9-2825be145263"
        headers = {
            **APP_ENGINE_HEADERS,
            "X-AppEngine-TaskName": task_id,
        }
        response = self.client.get(
            "/handle_new_files", query_string=request_args, headers=headers
        )

        self.assertEqual(200, response.status_code)
        self.mock_controller_factory.build.assert_called_with(
            region_code=self.region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            allow_unlaunched=True,
        )
        mock_controller.schedule_next_ingest_task.assert_not_called()
        mock_controller.run_extract_and_merge_job_and_kick_scheduler_on_completion.assert_not_called()
        mock_controller.handle_new_files.assert_called_with(
            current_task_id=task_id, can_start_ingest=False
        )

    @patch(f"{CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes")
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_ensure_all_raw_file_paths_normalized(
        self,
        mock_get_region: mock.MagicMock,
        mock_environment: mock.MagicMock,
        mock_supported_region_codes: mock.MagicMock,
    ) -> None:
        mock_environment.return_value = "production"

        fake_supported_regions = {
            "us_mo": fake_region(region_code="us_mo", environment="staging"),
            self.region_code: fake_region(
                region_code=self.region_code, environment="production"
            ),
        }

        mock_cloud_task_manager = create_autospec(DirectIngestCloudTaskManager)
        mock_controllers_by_region_code = {}

        def mock_build_controller(
            region_code: str,
            ingest_instance: DirectIngestInstance,
            allow_unlaunched: bool,
        ) -> BaseDirectIngestController:
            self.assertTrue(allow_unlaunched)
            self.assertEqual(DirectIngestInstance.PRIMARY, ingest_instance)

            mock_controller = Mock(__class__=BaseDirectIngestController)
            mock_controller.cloud_task_manager = mock_cloud_task_manager
            mock_controller.ingest_instance = ingest_instance
            mock_controller.region = fake_supported_regions[region_code.lower()]
            mock_controller.ingest_instance = DirectIngestInstance.PRIMARY

            mock_controllers_by_region_code[region_code] = mock_controller
            return mock_controller

        self.mock_controller_factory.build.side_effect = mock_build_controller

        def fake_get_region(region_code: str, is_direct_ingest: bool) -> Region:
            if not is_direct_ingest:
                self.fail("is_direct_ingest is False")

            return fake_supported_regions[region_code]

        mock_get_region.side_effect = fake_get_region

        mock_supported_region_codes.return_value = fake_supported_regions.keys()

        headers = APP_ENGINE_HEADERS
        response = self.client.get(
            "/ensure_all_raw_file_paths_normalized", query_string={}, headers=headers
        )

        self.assertEqual(200, response.status_code)
        mock_cloud_task_manager.create_direct_ingest_handle_new_files_task.assert_has_calls(
            [
                call(
                    fake_supported_regions["us_mo"],
                    ingest_instance=mock_controllers_by_region_code[
                        "us_mo"
                    ].ingest_instance,
                    can_start_ingest=False,
                ),
                call(
                    fake_supported_regions[self.region_code],
                    ingest_instance=mock_controllers_by_region_code[
                        self.region_code
                    ].ingest_instance,
                    can_start_ingest=True,
                ),
            ]
        )

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch(
        "recidiviz.ingest.direct.controllers.base_direct_ingest_controller.DirectIngestCloudTaskManagerImpl"
    )
    def test_ensure_all_raw_file_paths_normalized_actual_regions(
        self, mock_cloud_task_manager: mock.MagicMock, mock_environment: mock.MagicMock
    ) -> None:
        # We want to use real controllers for this test so we stop and clear the patcher
        self.controller_factory_patcher.stop()
        self.controller_factory_patcher = None
        with local_project_id_override("recidiviz-staging"):
            mock_environment.return_value = "staging"
            mock_cloud_task_manager.return_value = create_autospec(
                DirectIngestCloudTaskManager
            )

            headers = APP_ENGINE_HEADERS
            response = self.client.get(
                "/ensure_all_raw_file_paths_normalized",
                query_string={},
                headers=headers,
            )

            self.assertEqual(200, response.status_code)

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_raw_data_import(
        self,
        mock_region: mock.MagicMock,
        mock_environment: mock.MagicMock,
    ) -> None:

        region_code = "us_xx"

        mock_environment.return_value = "staging"
        mock_controller = create_autospec(BaseDirectIngestController)
        self.mock_controller_factory.build.return_value = mock_controller
        mock_region.return_value = fake_region(
            region_code=region_code, environment="staging"
        )
        bucket_name = build_ingest_bucket_name(
            project_id="recidiviz-xxx",
            region_code=self.region_code,
            suffix="",
        )

        raw_data_path = to_normalized_unprocessed_raw_file_path(
            f"{bucket_name}/raw_data_path.csv"
        )
        import_args = GcsfsRawDataBQImportArgs(
            raw_data_file_path=GcsfsFilePath.from_absolute_path(raw_data_path)
        )
        request_args = {"region": region_code, "file_path": raw_data_path}
        body = {
            "cloud_task_args": import_args.to_serializable(),
            "args_type": "GcsfsRawDataBQImportArgs",
        }
        body_encoded = json.dumps(body).encode()

        headers = APP_ENGINE_HEADERS

        response = self.client.post(
            "/raw_data_import",
            query_string=request_args,
            headers=headers,
            data=body_encoded,
        )
        self.assertEqual(200, response.status_code)
        mock_controller.do_raw_data_import.assert_called_with(import_args)

    @patch("recidiviz.utils.regions.get_region")
    def test_materialize_ingest_view(
        self,
        mock_region: mock.MagicMock,
    ) -> None:

        region_code = "us_xx"

        mock_controller = create_autospec(BaseDirectIngestController)
        self.mock_controller_factory.build.return_value = mock_controller
        mock_region.return_value = fake_region(
            region_code=region_code, environment="staging"
        )

        ingest_view_name = "my_ingest_view"
        materialization_args = IngestViewMaterializationArgs(
            ingest_view_name=ingest_view_name,
            ingest_instance=DirectIngestInstance.PRIMARY,
            lower_bound_datetime_exclusive=datetime.datetime(2020, 4, 29),
            upper_bound_datetime_inclusive=datetime.datetime(2020, 4, 30),
        )

        request_args = {
            "region": region_code,
            "ingest_view_name": ingest_view_name,
            "ingest_instance": "primary",
        }
        body = {
            "cloud_task_args": materialization_args.to_serializable(),
            "args_type": "IngestViewMaterializationArgs",
        }
        body_encoded = json.dumps(body).encode()

        headers = APP_ENGINE_HEADERS

        response = self.client.post(
            "/materialize_ingest_view",
            query_string=request_args,
            headers=headers,
            data=body_encoded,
        )
        self.assertEqual(200, response.status_code)
        mock_controller.do_ingest_view_materialization.assert_called_with(
            materialization_args
        )

    @patch(f"{CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes")
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_heartbeat(
        self,
        mock_get_region: mock.MagicMock,
        mock_environment: mock.MagicMock,
        mock_supported_region_codes: mock.MagicMock,
    ) -> None:

        fake_supported_regions = {
            "us_mo": fake_region(region_code="us_mo", environment="staging"),
            self.region_code: fake_region(
                region_code=self.region_code, environment="production"
            ),
        }

        mock_cloud_task_manager = create_autospec(DirectIngestCloudTaskManager)
        region_to_mock_controller = defaultdict(list)

        def mock_build_controller(
            region_code: str,
            ingest_instance: DirectIngestInstance,  # pylint: disable=unused-argument
            allow_unlaunched: bool,
        ) -> BaseDirectIngestController:
            self.assertFalse(allow_unlaunched)
            mock_controller = Mock(__class__=BaseDirectIngestController)
            mock_controller.cloud_task_manager.return_value = mock_cloud_task_manager
            region_to_mock_controller[region_code.lower()].append(mock_controller)
            return mock_controller

        self.mock_controller_factory.build.side_effect = mock_build_controller

        def fake_get_region(region_code: str, is_direct_ingest: bool) -> Region:
            if not is_direct_ingest:
                self.fail("is_direct_ingest is False")

            return fake_supported_regions[region_code]

        mock_get_region.side_effect = fake_get_region

        mock_supported_region_codes.return_value = fake_supported_regions.keys()

        mock_environment.return_value = "staging"

        headers = APP_ENGINE_HEADERS

        response = self.client.post(
            "/heartbeat",
            query_string={},
            headers=headers,
            data={},
        )
        self.assertEqual(200, response.status_code)

        mock_supported_region_codes.assert_called()
        for controllers in region_to_mock_controller.values():
            self.assertEqual(len(controllers), len(DirectIngestInstance))
            for mock_controller in controllers:
                mock_controller.kick_scheduler.assert_called_once()

    @patch(f"{CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes")
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_kick_all_schedulers(
        self,
        mock_get_region: mock.MagicMock,
        mock_environment: mock.MagicMock,
        mock_supported_region_codes: mock.MagicMock,
    ) -> None:

        fake_supported_regions = {
            "us_mo": fake_region(region_code="us_mo", environment="staging"),
            self.region_code: fake_region(
                region_code=self.region_code, environment="production"
            ),
        }

        mock_cloud_task_manager = create_autospec(DirectIngestCloudTaskManager)
        region_to_mock_controller = defaultdict(list)

        def mock_build_controller(
            region_code: str,
            ingest_instance: DirectIngestInstance,  # pylint: disable=unused-argument
            allow_unlaunched: bool,
        ) -> BaseDirectIngestController:
            self.assertFalse(allow_unlaunched)
            if region_code is None:
                raise ValueError("Expected nonnull region code")
            mock_controller = Mock(__class__=BaseDirectIngestController)
            mock_controller.cloud_task_manager.return_value = mock_cloud_task_manager
            region_to_mock_controller[region_code.lower()].append(mock_controller)
            return mock_controller

        self.mock_controller_factory.build.side_effect = mock_build_controller

        def fake_get_region(region_code: str, is_direct_ingest: bool) -> Region:
            if not is_direct_ingest:
                self.fail("is_direct_ingest is False")

            return fake_supported_regions[region_code]

        mock_get_region.side_effect = fake_get_region

        mock_supported_region_codes.return_value = fake_supported_regions.keys()

        mock_environment.return_value = "staging"

        kick_all_schedulers()

        mock_supported_region_codes.assert_called()
        for controllers in region_to_mock_controller.values():
            self.assertEqual(len(controllers), len(DirectIngestInstance))
            for mock_controller in controllers:
                mock_controller.kick_scheduler.assert_called_once()

    @patch(f"{CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes")
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_kick_all_schedulers_ignores_unlaunched_environments(
        self,
        mock_get_region: mock.MagicMock,
        mock_environment: mock.MagicMock,
        mock_supported_region_codes: mock.MagicMock,
    ) -> None:

        fake_supported_regions = {
            "us_mo": fake_region(region_code="us_mo", environment="staging"),
            self.region_code: fake_region(
                region_code=self.region_code, environment="production"
            ),
        }

        mock_cloud_task_manager = create_autospec(DirectIngestCloudTaskManager)
        region_to_mock_controller = {}

        def mock_build_controller(
            region_code: str,
            ingest_instance: DirectIngestInstance,  # pylint: disable=unused-argument
            allow_unlaunched: bool,
        ) -> BaseDirectIngestController:
            self.assertFalse(allow_unlaunched)
            mock_controller = Mock(__class__=BaseDirectIngestController)
            mock_controller.cloud_task_manager.return_value = mock_cloud_task_manager
            region_to_mock_controller[region_code.lower()] = mock_controller
            return mock_controller

        self.mock_controller_factory.build = mock_build_controller

        def fake_get_region(region_code: str, is_direct_ingest: bool) -> Region:
            if not is_direct_ingest:
                self.fail("is_direct_ingest is False")

            return fake_supported_regions[region_code]

        mock_get_region.side_effect = fake_get_region

        mock_supported_region_codes.return_value = fake_supported_regions.keys()

        mock_environment.return_value = "production"

        kick_all_schedulers()

        mock_supported_region_codes.assert_called()
        for region_code, controller in region_to_mock_controller.items():
            if fake_supported_regions[region_code].environment == "staging":
                controller.kick_all_scheduler.assert_not_called()
            else:
                controller.kick_scheduler.assert_called_once()

    @patch.object(
        target=RecidivizSftpConnection,
        attribute="__enter__",
        return_value=Mock(spec=RecidivizSftpConnection),
    )
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.ingest.direct.sftp.download_files_from_sftp.SftpAuth.for_region")
    @patch(
        "recidiviz.ingest.direct.sftp.sftp_download_delegate_factory.SftpDownloadDelegateFactory.build"
    )
    @patch("recidiviz.ingest.direct.direct_ingest_control.GcsfsFactory.build")
    @patch(
        "recidiviz.ingest.direct.sftp.download_files_from_sftp."
        "DownloadFilesFromSftpController"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.base_upload_state_files_to_ingest_bucket_controller."
        "UploadStateFilesToIngestBucketController"
    )
    @patch.object(
        DownloadFilesFromSftpController,
        "do_fetch",
        lambda _: MultiRequestResultWithSkipped[
            Tuple[str, datetime.datetime], str, str
        ](
            successes=[("test_file1.txt", TODAY), ("test_file2.txt", TODAY)],
            failures=[],
            skipped=[],
        ),
    )
    @patch.object(
        UploadStateFilesToIngestBucketController,
        "do_upload",
        lambda _: MultiRequestResultWithSkipped[str, str, str](
            successes=["test_file1.txt", "test_file2.txt"], failures=[], skipped=[]
        ),
    )
    @patch("recidiviz.utils.regions.get_region")
    def test_upload_from_sftp(
        self,
        mock_get_region: mock.MagicMock,
        _mock_upload_controller: mock.MagicMock,
        _mock_download_controller: mock.MagicMock,
        mock_fs_factory: mock.MagicMock,
        mock_download_delegate_factory: mock.MagicMock,
        mock_sftp_auth: mock.MagicMock,
        mock_environment: mock.MagicMock,
        _mock_client: mock.MagicMock,
    ) -> None:

        region_code = "us_xx"
        fake_regions = {
            "us_xx": fake_region(region_code="us_xx", environment="staging")
        }

        mock_get_region.side_effect = (
            lambda region_code, is_direct_ingest: fake_regions.get(region_code)
        )

        mock_environment.return_value = "staging"
        request_args = {"region": region_code, "date": "2021-01-01"}
        headers = APP_ENGINE_HEADERS

        mock_fs_factory.return_value = FakeGCSFileSystem()

        mock_download_delegate_factory.return_value = Mock(
            spec=BaseSftpDownloadDelegate,
            root_directory=lambda _, candidate_paths: ".",
            filter_paths=lambda _, candidate_paths: candidate_paths,
            post_process_downloads=lambda _, download_directory_path: None,
        )
        mock_sftp_auth.return_value = SftpAuth(
            "host",
            "host ssh-rsa some-key",
            HostKeyEntry(["host"], None),
            "username",
            "password",
            None,
        )

        response = self.client.post(
            "/upload_from_sftp", query_string=request_args, headers=headers
        )
        self.assertEqual(HTTPStatus.OK, response.status_code)
        self.mock_task_manager.create_direct_ingest_handle_new_files_task.assert_called_once()

    @patch.object(
        target=RecidivizSftpConnection,
        attribute="__enter__",
        return_value=Mock(spec=RecidivizSftpConnection),
    )
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.ingest.direct.sftp.download_files_from_sftp.SftpAuth.for_region")
    @patch(
        "recidiviz.ingest.direct.sftp.sftp_download_delegate_factory.SftpDownloadDelegateFactory.build"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.download_files_from_sftp."
        "DownloadFilesFromSftpController"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.base_upload_state_files_to_ingest_bucket_controller."
        "UploadStateFilesToIngestBucketController"
    )
    @patch.object(
        DownloadFilesFromSftpController,
        "do_fetch",
        lambda _: MultiRequestResultWithSkipped[
            Tuple[str, datetime.datetime], str, str
        ](
            successes=[("test_file1.txt", TODAY)],
            failures=[("test_file2.txt")],
            skipped=[],
        ),
    )
    @patch.object(
        UploadStateFilesToIngestBucketController,
        "do_upload",
        lambda _: MultiRequestResultWithSkipped[str, str, str](
            successes=["test_file1.txt"], failures=[], skipped=[]
        ),
    )
    def test_upload_from_sftp_handles_partial_downloads(
        self,
        _mock_upload_controller: mock.MagicMock,
        _mock_download_controller: mock.MagicMock,
        mock_download_delegate_factory: mock.MagicMock,
        mock_sftp_auth: mock.MagicMock,
        mock_environment: mock.MagicMock,
        _mock_client: mock.MagicMock,
    ) -> None:
        region_code = "us_xx"
        mock_environment.return_value = "staging"
        request_args = {"region": region_code, "date": "2021-01-01"}
        headers = APP_ENGINE_HEADERS

        mock_download_delegate_factory.return_value = Mock(
            spec=BaseSftpDownloadDelegate,
            root_directory=lambda _, candidate_paths: ".",
            filter_paths=lambda _, candidate_paths: candidate_paths,
            post_process_downloads=lambda _, download_directory_path: None,
        )
        mock_sftp_auth.return_value = SftpAuth(
            "host",
            "host ssh-rsa some-key",
            HostKeyEntry(["host"], None),
            "username",
            "password",
            None,
        )

        response = self.client.post(
            "/upload_from_sftp", query_string=request_args, headers=headers
        )
        self.assertEqual(HTTPStatus.MULTI_STATUS, response.status_code)

    @patch.object(
        target=RecidivizSftpConnection,
        attribute="__enter__",
        return_value=Mock(spec=RecidivizSftpConnection),
    )
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.ingest.direct.sftp.download_files_from_sftp.SftpAuth.for_region")
    @patch(
        "recidiviz.ingest.direct.sftp.sftp_download_delegate_factory.SftpDownloadDelegateFactory.build"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.download_files_from_sftp."
        "DownloadFilesFromSftpController"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.base_upload_state_files_to_ingest_bucket_controller."
        "UploadStateFilesToIngestBucketController"
    )
    @patch.object(
        DownloadFilesFromSftpController,
        "do_fetch",
        lambda _: MultiRequestResultWithSkipped[
            Tuple[str, datetime.datetime], str, str
        ](
            successes=[("test_file1.txt", TODAY), ("test_file2.txt", TODAY)],
            failures=[],
            skipped=[],
        ),
    )
    @patch.object(
        UploadStateFilesToIngestBucketController,
        "do_upload",
        lambda _: MultiRequestResultWithSkipped[str, str, str](
            successes=["test_file1.txt"], failures=["test_file2.txt"], skipped=[]
        ),
    )
    def test_upload_from_sftp_handles_partial_uploads(
        self,
        _mock_upload_controller: mock.MagicMock,
        _mock_download_controller: mock.MagicMock,
        mock_download_delegate_factory: mock.MagicMock,
        mock_sftp_auth: mock.MagicMock,
        mock_environment: mock.MagicMock,
        _mock_client: mock.MagicMock,
    ) -> None:

        region_code = "us_xx"
        mock_environment.return_value = "staging"
        request_args = {"region": region_code, "date": "2021-01-01"}
        headers = APP_ENGINE_HEADERS

        mock_download_delegate_factory.return_value = Mock(
            spec=BaseSftpDownloadDelegate,
            root_directory=lambda _, candidate_paths: ".",
            filter_paths=lambda _, candidate_paths: candidate_paths,
            post_process_downloads=lambda _, download_directory_path: None,
        )
        mock_sftp_auth.return_value = SftpAuth(
            "host",
            "host ssh-rsa some-key",
            HostKeyEntry(["host"], None),
            "username",
            "password",
            None,
        )

        response = self.client.post(
            "/upload_from_sftp", query_string=request_args, headers=headers
        )
        self.assertEqual(HTTPStatus.MULTI_STATUS, response.status_code)

    @patch.object(
        target=RecidivizSftpConnection,
        attribute="__enter__",
        return_value=Mock(spec=RecidivizSftpConnection),
    )
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.ingest.direct.sftp.download_files_from_sftp.SftpAuth.for_region")
    @patch(
        "recidiviz.ingest.direct.sftp.sftp_download_delegate_factory.SftpDownloadDelegateFactory.build"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.download_files_from_sftp."
        "DownloadFilesFromSftpController"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.base_upload_state_files_to_ingest_bucket_controller."
        "UploadStateFilesToIngestBucketController"
    )
    @patch.object(
        DownloadFilesFromSftpController,
        "do_fetch",
        lambda _: MultiRequestResultWithSkipped[
            Tuple[str, datetime.datetime], str, str
        ](
            successes=[("test_file1.txt", TODAY), ("test_file3.txt", TODAY)],
            failures=[("test_file2.txt")],
            skipped=[],
        ),
    )
    @patch.object(
        UploadStateFilesToIngestBucketController,
        "do_upload",
        lambda _: MultiRequestResultWithSkipped[str, str, str](
            successes=["test_file1.txt"], failures=["test_file3.txt"], skipped=[]
        ),
    )
    def test_upload_from_sftp_handles_both_partial_uploads_and_downloads(
        self,
        _mock_upload_controller: mock.MagicMock,
        _mock_download_controller: mock.MagicMock,
        mock_download_delegate_factory: mock.MagicMock,
        mock_sftp_auth: mock.MagicMock,
        mock_environment: mock.MagicMock,
        _mock_client: mock.MagicMock,
    ) -> None:
        region_code = "us_xx"
        mock_environment.return_value = "staging"
        request_args = {"region": region_code, "date": "2021-01-01"}
        headers = APP_ENGINE_HEADERS

        mock_download_delegate_factory.return_value = Mock(
            spec=BaseSftpDownloadDelegate,
            root_directory=lambda _, candidate_paths: ".",
            filter_paths=lambda _, candidate_paths: candidate_paths,
            post_process_downloads=lambda _, download_directory_path: None,
        )
        mock_sftp_auth.return_value = SftpAuth(
            "host",
            "host ssh-rsa some-key",
            HostKeyEntry(["host"], None),
            "username",
            "password",
            None,
        )

        response = self.client.post(
            "/upload_from_sftp", query_string=request_args, headers=headers
        )
        self.assertEqual(HTTPStatus.MULTI_STATUS, response.status_code)

    @patch.object(
        target=RecidivizSftpConnection,
        attribute="__enter__",
        return_value=Mock(spec=RecidivizSftpConnection),
    )
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.ingest.direct.sftp.download_files_from_sftp.SftpAuth.for_region")
    @patch(
        "recidiviz.ingest.direct.sftp.sftp_download_delegate_factory.SftpDownloadDelegateFactory.build"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.download_files_from_sftp."
        "DownloadFilesFromSftpController"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.base_upload_state_files_to_ingest_bucket_controller."
        "UploadStateFilesToIngestBucketController"
    )
    @patch.object(
        DownloadFilesFromSftpController,
        "do_fetch",
        lambda _: MultiRequestResultWithSkipped[
            Tuple[str, datetime.datetime], str, str
        ](successes=[], failures=["test_file1.txt"], skipped=[]),
    )
    def test_upload_from_sftp_handles_all_downloads_failing(
        self,
        mock_upload_controller: mock.MagicMock,
        mock_download_controller: mock.MagicMock,
        mock_download_delegate_factory: mock.MagicMock,
        mock_sftp_auth: mock.MagicMock,
        mock_environment: mock.MagicMock,
        _mock_client: mock.MagicMock,
    ) -> None:
        region_code = "us_xx"
        mock_environment.return_value = "staging"
        request_args = {"region": region_code, "date": "2021-01-01"}
        headers = APP_ENGINE_HEADERS

        mock_download_delegate_factory.return_value = Mock(
            spec=BaseSftpDownloadDelegate,
            root_directory=lambda _, candidate_paths: ".",
            filter_paths=lambda _, candidate_paths: candidate_paths,
            post_process_downloads=lambda _, download_directory_path: None,
        )
        mock_sftp_auth.return_value = SftpAuth(
            "host",
            "host ssh-rsa some-key",
            HostKeyEntry(["host"], None),
            "username",
            "password",
            None,
        )

        mock_download_controller.return_value = create_autospec(
            DownloadFilesFromSftpController
        )
        mock_upload_controller.return_value = create_autospec(
            UploadStateFilesToIngestBucketController
        )

        response = self.client.post(
            "/upload_from_sftp", query_string=request_args, headers=headers
        )
        mock_upload_controller.do_upload().assert_not_called()
        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    @patch.object(
        target=RecidivizSftpConnection,
        attribute="__enter__",
        return_value=Mock(spec=RecidivizSftpConnection),
    )
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.ingest.direct.sftp.download_files_from_sftp.SftpAuth.for_region")
    @patch(
        "recidiviz.ingest.direct.sftp.sftp_download_delegate_factory.SftpDownloadDelegateFactory.build"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.download_files_from_sftp."
        "DownloadFilesFromSftpController"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.base_upload_state_files_to_ingest_bucket_controller."
        "UploadStateFilesToIngestBucketController"
    )
    @patch.object(
        DownloadFilesFromSftpController,
        "do_fetch",
        lambda _: MultiRequestResultWithSkipped[
            Tuple[str, datetime.datetime], str, str
        ](
            successes=[("test_file1.txt", TODAY), ("test_file2.txt", TODAY)],
            failures=[],
            skipped=[],
        ),
    )
    @patch.object(
        UploadStateFilesToIngestBucketController,
        "do_upload",
        lambda _: MultiRequestResultWithSkipped[str, str, str](
            successes=[], failures=["test_file1.txt", "test_file2.txt"], skipped=[]
        ),
    )
    def test_upload_from_sftp_handles_all_uploads_failing(
        self,
        mock_upload_controller: mock.MagicMock,
        mock_download_controller: mock.MagicMock,
        mock_download_delegate_factory: mock.MagicMock,
        mock_sftp_auth: mock.MagicMock,
        mock_environment: mock.MagicMock,
        _mock_client: mock.MagicMock,
    ) -> None:
        region_code = "us_xx"
        mock_environment.return_value = "staging"
        request_args = {"region": region_code, "date": "2021-01-01"}
        headers = APP_ENGINE_HEADERS

        mock_download_delegate_factory.return_value = Mock(
            spec=BaseSftpDownloadDelegate,
            root_directory=lambda _, candidate_paths: ".",
            filter_paths=lambda _, candidate_paths: candidate_paths,
            post_process_downloads=lambda _, download_directory_path: None,
        )
        mock_sftp_auth.return_value = SftpAuth(
            "host",
            "host ssh-rsa some-key",
            HostKeyEntry(["host"], None),
            "username",
            "password",
            None,
        )

        mock_download_controller.return_value = create_autospec(
            DownloadFilesFromSftpController
        )
        mock_upload_controller.return_value = create_autospec(
            UploadStateFilesToIngestBucketController
        )

        response = self.client.post(
            "/upload_from_sftp", query_string=request_args, headers=headers
        )
        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    @patch.object(
        target=RecidivizSftpConnection,
        attribute="__enter__",
        return_value=Mock(spec=RecidivizSftpConnection),
    )
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.ingest.direct.sftp.download_files_from_sftp.SftpAuth.for_region")
    @patch(
        "recidiviz.ingest.direct.sftp.sftp_download_delegate_factory.SftpDownloadDelegateFactory.build"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.download_files_from_sftp."
        "DownloadFilesFromSftpController"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.base_upload_state_files_to_ingest_bucket_controller."
        "UploadStateFilesToIngestBucketController"
    )
    @patch.object(
        DownloadFilesFromSftpController,
        "do_fetch",
        lambda _: MultiRequestResultWithSkipped[
            Tuple[str, datetime.datetime], str, str
        ](successes=[], failures=[], skipped=[]),
    )
    def test_upload_from_sftp_handles_missing_downloads(
        self,
        mock_upload_controller: mock.MagicMock,
        mock_download_controller: mock.MagicMock,
        mock_download_delegate_factory: mock.MagicMock,
        mock_sftp_auth: mock.MagicMock,
        mock_environment: mock.MagicMock,
        _mock_client: mock.MagicMock,
    ) -> None:
        region_code = "us_xx"
        mock_environment.return_value = "staging"
        request_args = {"region": region_code, "date": "2021-01-01"}
        headers = APP_ENGINE_HEADERS

        mock_download_delegate_factory.return_value = Mock(
            spec=BaseSftpDownloadDelegate,
            root_directory=lambda _, candidate_paths: ".",
            filter_paths=lambda _, candidate_paths: candidate_paths,
            post_process_downloads=lambda _, download_directory_path: None,
        )
        mock_sftp_auth.return_value = SftpAuth(
            "host",
            "host ssh-rsa some-key",
            HostKeyEntry(["host"], None),
            "username",
            "password",
            None,
        )

        mock_download_controller.return_value = create_autospec(
            DownloadFilesFromSftpController
        )
        mock_upload_controller.return_value = create_autospec(
            UploadStateFilesToIngestBucketController
        )

        response = self.client.post(
            "/upload_from_sftp", query_string=request_args, headers=headers
        )
        mock_upload_controller.do_upload().assert_not_called()
        self.assertEqual(HTTPStatus.OK, response.status_code)

    @patch.object(
        target=RecidivizSftpConnection,
        attribute="__enter__",
        return_value=Mock(spec=RecidivizSftpConnection),
    )
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.ingest.direct.sftp.download_files_from_sftp.SftpAuth.for_region")
    @patch(
        "recidiviz.ingest.direct.sftp.sftp_download_delegate_factory.SftpDownloadDelegateFactory.build"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.download_files_from_sftp."
        "DownloadFilesFromSftpController"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.base_upload_state_files_to_ingest_bucket_controller."
        "UploadStateFilesToIngestBucketController"
    )
    @patch.object(
        DownloadFilesFromSftpController,
        "do_fetch",
        lambda _: MultiRequestResultWithSkipped[
            Tuple[str, datetime.datetime], str, str
        ](successes=[], failures=[], skipped=["test_file1.txt", "test_file2.txt"]),
    )
    def test_upload_from_sftp_handles_all_downloads_skipped(
        self,
        mock_upload_controller: mock.MagicMock,
        mock_download_controller: mock.MagicMock,
        mock_download_delegate_factory: mock.MagicMock,
        mock_sftp_auth: mock.MagicMock,
        mock_environment: mock.MagicMock,
        _mock_client: mock.MagicMock,
    ) -> None:
        region_code = "us_xx"
        mock_environment.return_value = "staging"
        request_args = {"region": region_code, "date": "2021-01-01"}
        headers = APP_ENGINE_HEADERS

        mock_download_delegate_factory.return_value = Mock(
            spec=BaseSftpDownloadDelegate,
            root_directory=lambda _, candidate_paths: ".",
            filter_paths=lambda _, candidate_paths: candidate_paths,
            post_process_downloads=lambda _, download_directory_path: None,
        )
        mock_sftp_auth.return_value = SftpAuth(
            "host",
            "host ssh-rsa some-key",
            HostKeyEntry(["host"], None),
            "username",
            "password",
            None,
        )

        mock_download_controller.return_value = create_autospec(
            DownloadFilesFromSftpController
        )
        mock_upload_controller.return_value = create_autospec(
            UploadStateFilesToIngestBucketController
        )

        response = self.client.post(
            "/upload_from_sftp", query_string=request_args, headers=headers
        )
        mock_upload_controller.do_upload().assert_not_called()
        self.assertEqual(HTTPStatus.OK, response.status_code)

    @patch.object(
        target=RecidivizSftpConnection,
        attribute="__enter__",
        return_value=Mock(spec=RecidivizSftpConnection),
    )
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.ingest.direct.sftp.download_files_from_sftp.SftpAuth.for_region")
    @patch(
        "recidiviz.ingest.direct.sftp.sftp_download_delegate_factory.SftpDownloadDelegateFactory.build"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.download_files_from_sftp."
        "DownloadFilesFromSftpController"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.base_upload_state_files_to_ingest_bucket_controller."
        "UploadStateFilesToIngestBucketController"
    )
    @patch.object(
        DownloadFilesFromSftpController,
        "do_fetch",
        lambda _: MultiRequestResultWithSkipped[
            Tuple[str, datetime.datetime], str, str
        ](
            successes=[("test_file1.txt", TODAY), ("test_file2.txt", TODAY)],
            failures=[],
            skipped=[],
        ),
    )
    @patch.object(
        UploadStateFilesToIngestBucketController,
        "do_upload",
        lambda _: MultiRequestResultWithSkipped[str, str, str](
            successes=[], failures=[], skipped=["test_file1.txt", "test_file2.txt"]
        ),
    )
    def test_upload_from_sftp_handles_all_uploads_skipped(
        self,
        mock_upload_controller: mock.MagicMock,
        mock_download_controller: mock.MagicMock,
        mock_download_delegate_factory: mock.MagicMock,
        mock_sftp_auth: mock.MagicMock,
        mock_environment: mock.MagicMock,
        _mock_client: mock.MagicMock,
    ) -> None:
        region_code = "us_xx"
        mock_environment.return_value = "staging"
        request_args = {"region": region_code, "date": "2021-01-01"}
        headers = APP_ENGINE_HEADERS

        mock_download_delegate_factory.return_value = Mock(
            spec=BaseSftpDownloadDelegate,
            root_directory=lambda _, candidate_paths: ".",
            filter_paths=lambda _, candidate_paths: candidate_paths,
            post_process_downloads=lambda _, download_directory_path: None,
        )
        mock_sftp_auth.return_value = SftpAuth(
            "host",
            "host ssh-rsa some-key",
            HostKeyEntry(["host"], None),
            "username",
            "password",
            None,
        )

        mock_download_controller.return_value = create_autospec(
            DownloadFilesFromSftpController
        )
        mock_upload_controller.return_value = create_autospec(
            UploadStateFilesToIngestBucketController
        )

        response = self.client.post(
            "/upload_from_sftp", query_string=request_args, headers=headers
        )
        self.assertEqual(HTTPStatus.OK, response.status_code)

    @patch.object(
        target=RecidivizSftpConnection,
        attribute="__enter__",
        return_value=Mock(spec=RecidivizSftpConnection),
    )
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.ingest.direct.sftp.download_files_from_sftp.SftpAuth.for_region")
    @patch(
        "recidiviz.ingest.direct.sftp.sftp_download_delegate_factory.SftpDownloadDelegateFactory.build"
    )
    @patch("recidiviz.ingest.direct.direct_ingest_control.GcsfsFactory.build")
    @patch(
        "recidiviz.ingest.direct.sftp.download_files_from_sftp."
        "DownloadFilesFromSftpController"
    )
    @patch(
        "recidiviz.ingest.direct.sftp.base_upload_state_files_to_ingest_bucket_controller."
        "UploadStateFilesToIngestBucketController"
    )
    @patch.object(
        DownloadFilesFromSftpController,
        "do_fetch",
        lambda _: MultiRequestResultWithSkipped[
            Tuple[str, datetime.datetime], str, str
        ](
            successes=[("test_file1.txt", TODAY), ("test_file2.txt", TODAY)],
            failures=[],
            skipped=[],
        ),
    )
    @patch.object(
        UploadStateFilesToIngestBucketController,
        "do_upload",
        lambda _: MultiRequestResultWithSkipped[str, str, str](
            successes=["test_file1.txt", "test_file2.txt"], failures=[], skipped=[]
        ),
    )
    @patch("recidiviz.utils.regions.get_region")
    @patch(
        "recidiviz.cloud_storage.gcs_pseudo_lock_manager.GCSPseudoLockManager._lock_body_for_path"
    )
    @patch.object(GCSPseudoLockManager, "unlock", lambda self, name: None)
    def test_upload_from_sftp_skips_if_locks_acquired(
        self,
        mock_get_lock_body: mock.MagicMock,
        _mock_get_region: mock.MagicMock,
        _mock_upload_controller: mock.MagicMock,
        _mock_download_controller: mock.MagicMock,
        _mock_fs_factory: mock.MagicMock,
        _mock_download_delegate_factory: mock.MagicMock,
        _mock_sftp_auth: mock.MagicMock,
        mock_environment: mock.MagicMock,
        _mock_client: mock.MagicMock,
    ) -> None:
        region_code = "us_xx"
        mock_environment.return_value = "staging"
        request_args = {"region": region_code, "date": "2021-01-01"}
        headers = APP_ENGINE_HEADERS

        self.fake_fs.test_add_path(
            GcsfsFilePath(
                bucket_name="recidiviz-project-gcslock",
                blob_name="GCS_SFTP_BUCKET_LOCK_US_XX",
            ),
            None,
        )

        mock_get_lock_body.return_value = GCSPseudoLockBody(
            lock_time=datetime.datetime.now(), expiration_in_seconds=3600 * 3
        )

        response = self.client.post(
            "/upload_from_sftp", query_string=request_args, headers=headers
        )
        self.assertEqual(HTTPStatus.NOT_MODIFIED, response.status_code)

    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_sftp_files(
        self,
        mock_get_region: mock.MagicMock,
        mock_environment: mock.MagicMock,
        _mock_cloud_tasks_client: mock.MagicMock,
    ) -> None:
        fake_regions = {
            "us_id": fake_region(region_code="us_id", environment="staging")
        }

        mock_get_region.side_effect = (
            lambda region_code, is_direct_ingest: fake_regions.get(region_code)
        )
        mock_environment.return_value = "staging"

        headers = APP_ENGINE_HEADERS
        request_args = {"region": "us_id"}
        response = self.client.get(
            "/handle_sftp_files", query_string=request_args, headers=headers
        )
        self.assertEqual(200, response.status_code)
        self.mock_task_manager.create_direct_ingest_sftp_download_task.assert_called_once()
