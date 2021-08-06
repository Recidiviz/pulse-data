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
"""Tests for the DirectIngestCloudTaskManagerImpl."""
import datetime
import json
from unittest import TestCase
from urllib.parse import urlencode

import mock
from freezegun import freeze_time
from google.cloud import tasks_v2
from mock import MagicMock, patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import (
    QUEUES_REGION,
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import (
    DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2,
    DIRECT_INGEST_SCHEDULER_QUEUE_V2,
    DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2,
)
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_file_path,
)
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
    GcsfsIngestArgs,
    GcsfsIngestViewExportArgs,
    GcsfsRawDataBQImportArgs,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    DirectIngestCloudTaskManagerImpl,
    ProcessIngestJobCloudTaskQueueInfo,
    _build_task_id,
)
from recidiviz.utils import regions

_REGION = regions.Region(
    region_code="us_xx",
    agency_name="agency_name",
    agency_type="state",
    base_url="base_url",
    timezone="America/New_York",
    jurisdiction_id="jid",
    environment="production",
    is_direct_ingest=True,
)


class TestCloudTaskQueueInfo(TestCase):
    """Tests for the CloudTaskQueueInfo."""

    def test_is_task_queued_no_tasks(self) -> None:
        # Arrange
        info = ProcessIngestJobCloudTaskQueueInfo(
            queue_name="queue_name", task_names=[]
        )

        file_path = to_normalized_unprocessed_file_path(
            "bucket/file_path.csv", GcsfsDirectIngestFileType.INGEST_VIEW
        )
        gcsfs_args = GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path=GcsfsFilePath.from_absolute_path(file_path),
        )

        # Act
        gcsfs_args_queued = info.is_task_queued(_REGION, gcsfs_args)

        # Assert
        self.assertFalse(gcsfs_args_queued)

        self.assertFalse(info.is_task_queued(_REGION, gcsfs_args))

    def test_is_task_queued_has_tasks(self) -> None:
        # Arrange
        file_path = to_normalized_unprocessed_file_path(
            "bucket/file_path.csv", GcsfsDirectIngestFileType.INGEST_VIEW
        )
        gcsfs_args = GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path=GcsfsFilePath.from_absolute_path(file_path),
        )

        full_task_name = _build_task_id(_REGION.region_code, gcsfs_args.task_id_tag())
        info = ProcessIngestJobCloudTaskQueueInfo(
            queue_name="queue_name",
            task_names=[
                "projects/path/to/random_task",
                f"projects/path/to/{full_task_name}",
            ],
        )
        file_path = to_normalized_unprocessed_file_path(
            "bucket/file_path.csv", GcsfsDirectIngestFileType.INGEST_VIEW
        )
        gcsfs_args = GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path=GcsfsFilePath.from_absolute_path(file_path),
        )

        # Act
        gcsfs_args_queued = info.is_task_queued(_REGION, gcsfs_args)

        # Assert
        self.assertTrue(gcsfs_args_queued)


class TestDirectIngestCloudTaskManagerImpl(TestCase):
    """Tests for the DirectIngestCloudTaskManagerImpl."""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id = "recidiviz-456"
        self.mock_project_id_fn.return_value = self.mock_project_id

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    @patch("recidiviz.ingest.direct.direct_ingest_cloud_task_manager.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-07-20")
    def test_create_direct_ingest_scheduler_queue_task(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        body_encoded = json.dumps({}).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        queue_path = f"{DIRECT_INGEST_SCHEDULER_QUEUE_V2}-path"

        task_name = DIRECT_INGEST_SCHEDULER_QUEUE_V2 + "/{}-{}-{}".format(
            _REGION.region_code, "2019-07-20", uuid
        )
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": f"/direct/scheduler?region={_REGION.region_code}&"
                f"bucket=some-bucket&just_finished_job=False",
                "body": body_encoded,
            },
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskManagerImpl().create_direct_ingest_scheduler_queue_task(
            region=_REGION,
            ingest_instance=DirectIngestInstance.PRIMARY,
            ingest_bucket=GcsfsBucketPath("some-bucket"),
            just_finished_job=False,
        )

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, DIRECT_INGEST_SCHEDULER_QUEUE_V2
        )
        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task
        )

    @patch("recidiviz.ingest.direct.direct_ingest_cloud_task_manager.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-07-20")
    def test_create_direct_ingest_scheduler_queue_task_secondary(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        body_encoded = json.dumps({}).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        queue_path = "us_xx-scheduler-queue-path"
        queue_name = "direct-ingest-state-us-xx-scheduler"

        task_name = "{}/{}-{}-{}".format(
            queue_name, _REGION.region_code, "2019-07-20", uuid
        )
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": f"/direct/scheduler?region={_REGION.region_code}&"
                f"bucket=some-bucket&just_finished_job=False",
                "body": body_encoded,
            },
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskManagerImpl().create_direct_ingest_scheduler_queue_task(
            region=_REGION,
            ingest_instance=DirectIngestInstance.SECONDARY,
            ingest_bucket=GcsfsBucketPath("some-bucket"),
            just_finished_job=False,
        )

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, queue_name
        )
        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task
        )

    @patch("recidiviz.ingest.direct.direct_ingest_cloud_task_manager.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-07-20")
    def test_create_direct_ingest_process_job_task(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        file_path = to_normalized_unprocessed_file_path(
            "bucket/ingest_view_name.csv",
            file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
        )
        ingest_args = GcsfsIngestArgs(
            datetime.datetime(year=2019, month=7, day=20),
            file_path=GcsfsFilePath.from_absolute_path(file_path),
        )
        body = {
            "cloud_task_args": ingest_args.to_serializable(),
            "args_type": "GcsfsIngestArgs",
        }
        body_encoded = json.dumps(body).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        date = "2019-07-20"
        queue_path = "process-queue-path"

        task_name = "{}/{}-{}-{}".format(
            DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2, _REGION.region_code, date, uuid
        )
        url_params = {"region": _REGION.region_code, "file_path": file_path}
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": f"/direct/process_job?{urlencode(url_params)}",
                "body": body_encoded,
            },
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskManagerImpl().create_direct_ingest_process_job_task(
            _REGION, DirectIngestInstance.PRIMARY, ingest_args
        )

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id,
            QUEUES_REGION,
            DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2,
        )
        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task
        )

    @patch("recidiviz.ingest.direct.direct_ingest_cloud_task_manager.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-07-20")
    def test_create_direct_ingest_process_job_task_secondary(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        file_path = to_normalized_unprocessed_file_path(
            "bucket/ingest_view_name.csv",
            file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
        )
        ingest_args = GcsfsIngestArgs(
            datetime.datetime(year=2019, month=7, day=20),
            file_path=GcsfsFilePath.from_absolute_path(file_path),
        )
        body = {
            "cloud_task_args": ingest_args.to_serializable(),
            "args_type": "GcsfsIngestArgs",
        }
        body_encoded = json.dumps(body).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        date = "2019-07-20"
        queue_path = "us-xx-process-queue-path"
        queue_name = "direct-ingest-state-us-xx-process-job-queue"

        task_name = "{}/{}-{}-{}".format(
            DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2, _REGION.region_code, date, uuid
        )
        url_params = {"region": _REGION.region_code, "file_path": file_path}
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": f"/direct/process_job?{urlencode(url_params)}",
                "body": body_encoded,
            },
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskManagerImpl().create_direct_ingest_process_job_task(
            _REGION, DirectIngestInstance.SECONDARY, ingest_args
        )

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id,
            QUEUES_REGION,
            queue_name,
        )
        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task
        )

    @patch(
        "recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper." "datetime"
    )
    @patch("recidiviz.ingest.direct.direct_ingest_cloud_task_manager.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    def test_create_direct_ingest_process_job_task_gcsfs_args(
        self, mock_client: MagicMock, mock_uuid: MagicMock, mock_datetime: MagicMock
    ) -> None:
        # Arrange
        file_path = to_normalized_unprocessed_file_path(
            "bucket/file_path.csv", GcsfsDirectIngestFileType.INGEST_VIEW
        )
        ingest_args = GcsfsIngestArgs(
            ingest_time=datetime.datetime(year=2019, month=7, day=20),
            file_path=GcsfsFilePath.from_absolute_path(file_path),
        )
        body = {
            "cloud_task_args": ingest_args.to_serializable(),
            "args_type": "GcsfsIngestArgs",
        }
        body_encoded = json.dumps(body).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        date = "2019-07-20"
        mock_datetime.date.today.return_value = date
        queue_path = f"{DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2}-path"

        task_name = _REGION.get_queue_name() + "/{}-{}-{}".format(
            _REGION.region_code, date, uuid
        )
        url_params = {"region": _REGION.region_code, "file_path": file_path}
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": f"/direct/process_job?{urlencode(url_params)}",
                "body": body_encoded,
            },
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskManagerImpl().create_direct_ingest_process_job_task(
            _REGION, DirectIngestInstance.PRIMARY, ingest_args
        )

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id,
            QUEUES_REGION,
            DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2,
        )
        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task
        )

    @patch("recidiviz.ingest.direct.direct_ingest_cloud_task_manager.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-07-20")
    def test_create_direct_ingest_raw_data_import_task(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        raw_data_path = GcsfsFilePath.from_absolute_path(
            to_normalized_unprocessed_file_path(
                "bucket/raw_data_path.csv",
                file_type=GcsfsDirectIngestFileType.RAW_DATA,
            )
        )
        import_args = GcsfsRawDataBQImportArgs(raw_data_file_path=raw_data_path)
        body = {
            "cloud_task_args": import_args.to_serializable(),
            "args_type": "GcsfsRawDataBQImportArgs",
        }
        body_encoded = json.dumps(body).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        date = "2019-07-20"
        queue_path = f"{DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2}-path"

        task_name = DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2 + "/{}-{}-{}".format(
            _REGION.region_code, date, uuid
        )
        url_params = {
            "region": _REGION.region_code,
            "file_path": raw_data_path.abs_path(),
        }
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": f"/direct/raw_data_import?{urlencode(url_params)}",
                "body": body_encoded,
            },
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskManagerImpl().create_direct_ingest_raw_data_import_task(
            _REGION, DirectIngestInstance.PRIMARY, import_args
        )

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2
        )
        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task
        )

    @patch("recidiviz.ingest.direct.direct_ingest_cloud_task_manager.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-07-20")
    def test_create_direct_ingest_ingest_view_export_task(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="my_ingest_view",
            output_bucket_name="my_ingest_bucket",
            upper_bound_datetime_prev=datetime.datetime(2020, 4, 29),
            upper_bound_datetime_to_export=datetime.datetime(2020, 4, 30),
        )
        body = {
            "cloud_task_args": export_args.to_serializable(),
            "args_type": "GcsfsIngestViewExportArgs",
        }
        body_encoded = json.dumps(body).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        date = "2019-07-20"
        queue_path = f"{DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2}-path"

        task_name = DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2 + "/{}-{}-{}".format(
            _REGION.region_code, date, uuid
        )
        url_params = {
            "region": _REGION.region_code,
            "output_bucket": "my_ingest_bucket",
        }
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": f"/direct/ingest_view_export?{urlencode(url_params)}",
                "body": body_encoded,
            },
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskManagerImpl().create_direct_ingest_ingest_view_export_task(
            _REGION, DirectIngestInstance.PRIMARY, export_args
        )

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2
        )
        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task
        )

    @patch("recidiviz.ingest.direct.direct_ingest_cloud_task_manager.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2021-01-01")
    def test_create_direct_ingest_sftp_download_task(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        queue_path = "us-xx-sftp-path"
        queue_name = "direct-ingest-state-us-xx-sftp-queue"
        date = "2021-01-01"
        task_name = "{}/{}-{}-{}".format(queue_name, _REGION.region_code, date, uuid)
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": f"/direct/upload_from_sftp?region={_REGION.region_code}",
                "body": json.dumps({}).encode(),
            },
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        DirectIngestCloudTaskManagerImpl().create_direct_ingest_sftp_download_task(
            _REGION
        )

        mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, queue_name
        )
        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task
        )
