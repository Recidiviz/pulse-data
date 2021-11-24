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

import attr
import mock
from freezegun import freeze_time
from google.cloud import tasks_v2
from mock import MagicMock, patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import (
    QUEUES_REGION,
)
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_file_name,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
    GcsfsIngestArgs,
    GcsfsIngestViewExportArgs,
    GcsfsRawDataBQImportArgs,
    gcsfs_direct_ingest_bucket_for_region,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    BQImportExportCloudTaskQueueInfo,
    DirectIngestCloudTaskManagerImpl,
    DirectIngestQueueType,
    IngestViewExportCloudTaskQueueInfo,
    ProcessIngestJobCloudTaskQueueInfo,
    RawDataImportCloudTaskQueueInfo,
    _build_task_id,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
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

_PRIMARY_INGEST_BUCKET = gcsfs_direct_ingest_bucket_for_region(
    project_id="recidiviz-456",
    region_code=_REGION.region_code,
    system_level=SystemLevel.STATE,
    ingest_instance=DirectIngestInstance.PRIMARY,
)
_SECONDARY_INGEST_BUCKET = gcsfs_direct_ingest_bucket_for_region(
    project_id="recidiviz-456",
    region_code=_REGION.region_code,
    system_level=SystemLevel.STATE,
    ingest_instance=DirectIngestInstance.SECONDARY,
)


class TestDirectIngestQueueType(TestCase):
    def test_exists_for_instance(self) -> None:
        primary_only_queue_types = {
            DirectIngestQueueType.SFTP_QUEUE,
            DirectIngestQueueType.RAW_DATA_IMPORT,
            # TODO(#9713): Remove this when we delete this enum value.
            DirectIngestQueueType.BQ_IMPORT_EXPORT,
        }
        for queue_type in primary_only_queue_types:
            self.assertTrue(
                queue_type.exists_for_instance(DirectIngestInstance.PRIMARY)
            )
            self.assertFalse(
                queue_type.exists_for_instance(DirectIngestInstance.SECONDARY)
            )

        other_queue_types = set(DirectIngestQueueType) - primary_only_queue_types
        for queue_type in other_queue_types:
            self.assertTrue(
                queue_type.exists_for_instance(DirectIngestInstance.PRIMARY)
            )
            self.assertTrue(
                queue_type.exists_for_instance(DirectIngestInstance.SECONDARY)
            )


# TODO(#9713): Delete these tests once we delete this queue.
class TestBQImportExportCloudTaskQueueInfo(TestCase):
    """Tests for the BQImportExportCloudTaskQueueInfo."""

    def setUp(self) -> None:
        self.raw_data_file_path = GcsfsFilePath.from_directory_and_file_name(
            _PRIMARY_INGEST_BUCKET,
            to_normalized_unprocessed_file_name(
                "file_path.csv", GcsfsDirectIngestFileType.RAW_DATA
            ),
        )
        self.raw_data_import_args = GcsfsRawDataBQImportArgs(
            raw_data_file_path=self.raw_data_file_path
        )
        self.primary_ingest_view_export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="my_file_tag",
            output_bucket_name=_PRIMARY_INGEST_BUCKET.bucket_name,
            upper_bound_datetime_prev=None,
            upper_bound_datetime_to_export=datetime.datetime.now(),
        )
        self.secondary_ingest_view_export_args = attr.evolve(
            self.primary_ingest_view_export_args,
            output_bucket_name=_SECONDARY_INGEST_BUCKET.bucket_name,
        )

    def test_info_no_tasks(self) -> None:
        info = BQImportExportCloudTaskQueueInfo(queue_name="queue_name", task_names=[])

        for instance in DirectIngestInstance:
            self.assertFalse(
                info.has_any_tasks_for_instance(_REGION.region_code, instance)
            )
            self.assertFalse(
                info.has_ingest_view_export_jobs_queued(_REGION.region_code, instance)
            )
        self.assertFalse(
            info.is_ingest_view_export_task_already_queued(
                _REGION.region_code, self.primary_ingest_view_export_args
            )
        )
        self.assertFalse(
            info.is_raw_data_import_task_already_queued(self.raw_data_import_args)
        )
        self.assertFalse(info.has_raw_data_import_jobs_queued())

    def test_single_raw_task(self) -> None:
        full_task_name = _build_task_id(
            _REGION.region_code,
            DirectIngestInstance.PRIMARY,
            self.raw_data_import_args.task_id_tag(),
        )

        other_args = attr.evolve(
            self.raw_data_import_args,
            raw_data_file_path=GcsfsFilePath.from_directory_and_file_name(
                _PRIMARY_INGEST_BUCKET,
                to_normalized_unprocessed_file_name(
                    "other_file.csv", GcsfsDirectIngestFileType.RAW_DATA
                ),
            ),
        )

        info = BQImportExportCloudTaskQueueInfo(
            queue_name="queue_name",
            task_names=[
                f"projects/path/to/{full_task_name}",
            ],
        )

        self.assertTrue(
            info.has_any_tasks_for_instance(
                _REGION.region_code, DirectIngestInstance.PRIMARY
            )
        )

        self.assertFalse(
            info.has_any_tasks_for_instance(
                _REGION.region_code, DirectIngestInstance.SECONDARY
            )
        )

        for instance in DirectIngestInstance:
            self.assertFalse(
                info.has_ingest_view_export_jobs_queued(_REGION.region_code, instance)
            )
        self.assertFalse(
            info.is_ingest_view_export_task_already_queued(
                _REGION.region_code, self.primary_ingest_view_export_args
            )
        )
        self.assertTrue(
            info.is_raw_data_import_task_already_queued(self.raw_data_import_args)
        )
        self.assertFalse(info.is_raw_data_import_task_already_queued(other_args))
        self.assertTrue(info.has_raw_data_import_jobs_queued())

    def test_single_ingest_view_task(self) -> None:
        full_task_name = _build_task_id(
            _REGION.region_code,
            DirectIngestInstance.PRIMARY,
            self.primary_ingest_view_export_args.task_id_tag(),
        )

        info = BQImportExportCloudTaskQueueInfo(
            queue_name="queue_name",
            task_names=[
                f"projects/path/to/{full_task_name}",
            ],
        )

        for instance in DirectIngestInstance:
            self.assertEqual(
                instance == DirectIngestInstance.PRIMARY,
                info.has_ingest_view_export_jobs_queued(_REGION.region_code, instance),
            )
            self.assertEqual(
                instance == DirectIngestInstance.PRIMARY,
                info.has_any_tasks_for_instance(_REGION.region_code, instance),
            )

        self.assertTrue(
            info.is_ingest_view_export_task_already_queued(
                _REGION.region_code, self.primary_ingest_view_export_args
            )
        )
        self.assertFalse(
            info.is_ingest_view_export_task_already_queued(
                _REGION.region_code, self.secondary_ingest_view_export_args
            )
        )
        self.assertFalse(
            info.is_raw_data_import_task_already_queued(self.raw_data_import_args)
        )
        self.assertFalse(info.has_raw_data_import_jobs_queued())

    def test_two_ingest_view_tasks(self) -> None:
        primary_full_task_name = _build_task_id(
            _REGION.region_code,
            DirectIngestInstance.PRIMARY,
            self.primary_ingest_view_export_args.task_id_tag(),
        )
        secondary_full_task_name = _build_task_id(
            _REGION.region_code,
            DirectIngestInstance.SECONDARY,
            self.secondary_ingest_view_export_args.task_id_tag(),
        )

        info = BQImportExportCloudTaskQueueInfo(
            queue_name="queue_name",
            task_names=[
                f"projects/path/to/{primary_full_task_name}",
                f"projects/path/to/{secondary_full_task_name}",
            ],
        )

        for instance in DirectIngestInstance:
            self.assertTrue(
                info.has_ingest_view_export_jobs_queued(_REGION.region_code, instance),
            )
            self.assertTrue(
                info.has_any_tasks_for_instance(_REGION.region_code, instance),
            )

        self.assertTrue(
            info.is_ingest_view_export_task_already_queued(
                _REGION.region_code, self.primary_ingest_view_export_args
            )
        )
        self.assertTrue(
            info.is_ingest_view_export_task_already_queued(
                _REGION.region_code, self.secondary_ingest_view_export_args
            )
        )
        self.assertFalse(
            info.is_raw_data_import_task_already_queued(self.raw_data_import_args)
        )
        self.assertFalse(info.has_raw_data_import_jobs_queued())


class TestRawDataImportQueueInfo(TestCase):
    """Tests for the RawDataImportCloudTaskQueueInfo."""

    def setUp(self) -> None:
        self.raw_data_file_path = GcsfsFilePath.from_directory_and_file_name(
            _PRIMARY_INGEST_BUCKET,
            to_normalized_unprocessed_file_name(
                "file_path.csv", GcsfsDirectIngestFileType.RAW_DATA
            ),
        )
        self.raw_data_import_args = GcsfsRawDataBQImportArgs(
            raw_data_file_path=self.raw_data_file_path
        )

    def test_info_no_tasks(self) -> None:
        info = RawDataImportCloudTaskQueueInfo(queue_name="queue_name", task_names=[])

        for instance in DirectIngestInstance:
            self.assertFalse(
                info.has_any_tasks_for_instance(_REGION.region_code, instance)
            )

        self.assertFalse(
            info.is_raw_data_import_task_already_queued(self.raw_data_import_args)
        )
        self.assertFalse(info.has_raw_data_import_jobs_queued())

    def test_single_raw_task(self) -> None:
        full_task_name = _build_task_id(
            _REGION.region_code,
            DirectIngestInstance.PRIMARY,
            self.raw_data_import_args.task_id_tag(),
        )

        other_args = attr.evolve(
            self.raw_data_import_args,
            raw_data_file_path=GcsfsFilePath.from_directory_and_file_name(
                _PRIMARY_INGEST_BUCKET,
                to_normalized_unprocessed_file_name(
                    "other_file.csv", GcsfsDirectIngestFileType.RAW_DATA
                ),
            ),
        )

        info = RawDataImportCloudTaskQueueInfo(
            queue_name="queue_name",
            task_names=[
                f"projects/path/to/{full_task_name}",
            ],
        )

        self.assertTrue(
            info.has_any_tasks_for_instance(
                _REGION.region_code, DirectIngestInstance.PRIMARY
            )
        )

        self.assertFalse(
            info.has_any_tasks_for_instance(
                _REGION.region_code, DirectIngestInstance.SECONDARY
            )
        )

        self.assertTrue(
            info.is_raw_data_import_task_already_queued(self.raw_data_import_args)
        )
        self.assertFalse(info.is_raw_data_import_task_already_queued(other_args))
        self.assertTrue(info.has_raw_data_import_jobs_queued())


class TestIngestViewExportCloudTaskQueueInfo(TestCase):
    """Tests for the IngestViewExportCloudTaskQueueInfo."""

    def setUp(self) -> None:
        self.primary_ingest_view_export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="my_file_tag",
            output_bucket_name=_PRIMARY_INGEST_BUCKET.bucket_name,
            upper_bound_datetime_prev=None,
            upper_bound_datetime_to_export=datetime.datetime.now(),
        )
        self.secondary_ingest_view_export_args = attr.evolve(
            self.primary_ingest_view_export_args,
            output_bucket_name=_SECONDARY_INGEST_BUCKET.bucket_name,
        )

    def test_info_no_tasks(self) -> None:
        info = IngestViewExportCloudTaskQueueInfo(
            queue_name="queue_name", task_names=[]
        )

        for instance in DirectIngestInstance:
            self.assertFalse(
                info.has_any_tasks_for_instance(_REGION.region_code, instance)
            )
            self.assertFalse(
                info.has_ingest_view_export_jobs_queued(_REGION.region_code, instance)
            )
        self.assertFalse(
            info.is_ingest_view_export_task_already_queued(
                _REGION.region_code, self.primary_ingest_view_export_args
            )
        )

    def test_single_ingest_view_task(self) -> None:
        full_task_name = _build_task_id(
            _REGION.region_code,
            DirectIngestInstance.SECONDARY,
            self.secondary_ingest_view_export_args.task_id_tag(),
        )

        info = IngestViewExportCloudTaskQueueInfo(
            queue_name="queue_name",
            task_names=[
                f"projects/path/to/{full_task_name}",
            ],
        )

        for instance in DirectIngestInstance:
            self.assertEqual(
                instance == DirectIngestInstance.SECONDARY,
                info.has_ingest_view_export_jobs_queued(_REGION.region_code, instance),
            )
            self.assertEqual(
                instance == DirectIngestInstance.SECONDARY,
                info.has_any_tasks_for_instance(_REGION.region_code, instance),
            )

        self.assertFalse(
            info.is_ingest_view_export_task_already_queued(
                _REGION.region_code, self.primary_ingest_view_export_args
            )
        )
        self.assertTrue(
            info.is_ingest_view_export_task_already_queued(
                _REGION.region_code, self.secondary_ingest_view_export_args
            )
        )


class TestProcessIngestJobCloudTaskQueueInfo(TestCase):
    """Tests for the ProcessIngestJobCloudTaskQueueInfo."""

    def setUp(self) -> None:
        bucket = gcsfs_direct_ingest_bucket_for_region(
            project_id="recidiviz-456",
            region_code=_REGION.region_code,
            system_level=SystemLevel.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )
        self.ingest_view_file_path = GcsfsFilePath.from_directory_and_file_name(
            bucket,
            to_normalized_unprocessed_file_name(
                "file_path.csv", GcsfsDirectIngestFileType.INGEST_VIEW
            ),
        )

    def test_info_no_tasks(self) -> None:
        # Arrange
        info = ProcessIngestJobCloudTaskQueueInfo(
            queue_name="queue_name", task_names=[]
        )

        gcsfs_args = GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path=self.ingest_view_file_path,
        )

        # Act
        gcsfs_args_queued = info.is_task_already_queued(_REGION.region_code, gcsfs_args)

        # Assert
        self.assertFalse(gcsfs_args_queued)

        self.assertFalse(info.is_task_already_queued(_REGION.region_code, gcsfs_args))
        for instance in DirectIngestInstance:
            self.assertFalse(
                info.has_any_tasks_for_instance(_REGION.region_code, instance)
            )

    def test_info_single_task(self) -> None:
        # Arrange
        gcsfs_args = GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path=self.ingest_view_file_path,
        )

        full_task_name = _build_task_id(
            _REGION.region_code, DirectIngestInstance.PRIMARY, gcsfs_args.task_id_tag()
        )
        info = ProcessIngestJobCloudTaskQueueInfo(
            queue_name="queue_name",
            task_names=[
                "projects/path/to/random_task",
                f"projects/path/to/{full_task_name}",
            ],
        )
        gcsfs_args = GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path=self.ingest_view_file_path,
        )

        # Act
        gcsfs_args_queued = info.is_task_already_queued(_REGION.region_code, gcsfs_args)

        # Assert
        self.assertTrue(gcsfs_args_queued)
        self.assertTrue(
            info.has_any_tasks_for_instance(
                _REGION.region_code, DirectIngestInstance.PRIMARY
            )
        )
        self.assertFalse(
            info.has_any_tasks_for_instance(
                _REGION.region_code, DirectIngestInstance.SECONDARY
            )
        )

    def test_info_tasks_both_instances(self) -> None:
        # Arrange
        gcsfs_args = GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path=self.ingest_view_file_path,
        )

        full_task_names = [
            _build_task_id(
                _REGION.region_code,
                ingest_instance,
                gcsfs_args.task_id_tag(),
            )
            for ingest_instance in DirectIngestInstance
        ]

        info = ProcessIngestJobCloudTaskQueueInfo(
            queue_name="queue_name",
            task_names=[
                "projects/path/to/random_task",
            ]
            + [
                f"projects/path/to/{full_task_name}"
                for full_task_name in full_task_names
            ],
        )
        gcsfs_args = GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path=self.ingest_view_file_path,
        )

        # Act
        gcsfs_args_queued = info.is_task_already_queued(_REGION.region_code, gcsfs_args)

        # Assert
        self.assertTrue(gcsfs_args_queued)
        for ingest_instance in DirectIngestInstance:
            self.assertTrue(
                info.has_any_tasks_for_instance(_REGION.region_code, ingest_instance)
            )


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
        queue_name = "direct-ingest-state-us-xx-scheduler"
        queue_path = f"{queue_name}-path"

        task_name = queue_name + f"/{_REGION.region_code}-2019-07-20-{uuid}"
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": f"/direct/scheduler?region={_REGION.region_code}&"
                f"bucket={_PRIMARY_INGEST_BUCKET.bucket_name}&just_finished_job=False",
                "body": body_encoded,
            },
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskManagerImpl().create_direct_ingest_scheduler_queue_task(
            region=_REGION,
            ingest_bucket=_PRIMARY_INGEST_BUCKET,
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
    def test_create_direct_ingest_scheduler_queue_task_secondary(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        body_encoded = json.dumps({}).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        queue_path = "us_xx-scheduler-queue-path"
        queue_name = "direct-ingest-state-us-xx-scheduler"

        task_name = f"{queue_name}/{_REGION.region_code}-2019-07-20-{uuid}"
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": f"/direct/scheduler?region={_REGION.region_code}&"
                f"bucket={_SECONDARY_INGEST_BUCKET.bucket_name}&just_finished_job=False",
                "body": body_encoded,
            },
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskManagerImpl().create_direct_ingest_scheduler_queue_task(
            region=_REGION,
            ingest_bucket=_SECONDARY_INGEST_BUCKET,
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

        file_path = GcsfsFilePath.from_directory_and_file_name(
            _PRIMARY_INGEST_BUCKET,
            to_normalized_unprocessed_file_name(
                file_name="ingest_view_name.csv",
                file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
            ),
        )

        ingest_args = GcsfsIngestArgs(
            datetime.datetime(year=2019, month=7, day=20),
            file_path=file_path,
        )
        body = {
            "cloud_task_args": ingest_args.to_serializable(),
            "args_type": "GcsfsIngestArgs",
        }
        body_encoded = json.dumps(body).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        date = "2019-07-20"
        queue_name = "direct-ingest-state-us-xx-process-job-queue"
        queue_path = "process-queue-path"

        task_name = f"{queue_name}/{_REGION.region_code}-{date}-{uuid}"
        url_params = {"region": _REGION.region_code, "file_path": file_path.abs_path()}
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
            _REGION, ingest_args
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

    @patch("recidiviz.ingest.direct.direct_ingest_cloud_task_manager.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-07-20")
    def test_create_direct_ingest_process_job_task_secondary(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        file_path = GcsfsFilePath.from_directory_and_file_name(
            _SECONDARY_INGEST_BUCKET,
            to_normalized_unprocessed_file_name(
                file_name="ingest_view_name.csv",
                file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
            ),
        )

        ingest_args = GcsfsIngestArgs(
            datetime.datetime(year=2019, month=7, day=20),
            file_path=file_path,
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

        task_name = f"{queue_name}/{_REGION.region_code}-{date}-{uuid}"
        url_params = {"region": _REGION.region_code, "file_path": file_path.abs_path()}
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
            _REGION, ingest_args
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
        file_path = GcsfsFilePath.from_directory_and_file_name(
            _PRIMARY_INGEST_BUCKET,
            to_normalized_unprocessed_file_name(
                file_name="file_path.csv",
                file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
            ),
        )

        ingest_args = GcsfsIngestArgs(
            ingest_time=datetime.datetime(year=2019, month=7, day=20),
            file_path=file_path,
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
        queue_name = "direct-ingest-state-us-xx-process-job-queue"
        queue_path = f"{queue_name}-path"

        task_name = _REGION.get_queue_name() + f"/{_REGION.region_code}-{date}-{uuid}"
        url_params = {"region": _REGION.region_code, "file_path": file_path.abs_path()}
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
            _REGION, ingest_args
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

    @patch("recidiviz.ingest.direct.direct_ingest_cloud_task_manager.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-07-20")
    def test_create_direct_ingest_raw_data_import_task(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        raw_data_path = GcsfsFilePath.from_directory_and_file_name(
            _PRIMARY_INGEST_BUCKET,
            to_normalized_unprocessed_file_name(
                file_name="raw_data_path.csv",
                file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
            ),
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
        queue_name = "direct-ingest-state-us-xx-raw-data-import"
        queue_path = f"{queue_name}-path"

        task_name = queue_name + f"/{_REGION.region_code}-{date}-{uuid}"
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
            _REGION, import_args
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

    @patch("recidiviz.ingest.direct.direct_ingest_cloud_task_manager.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-07-20")
    def test_create_direct_ingest_ingest_view_export_task(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="my_ingest_view",
            output_bucket_name=_PRIMARY_INGEST_BUCKET.bucket_name,
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
        queue_name = "direct-ingest-state-us-xx-ingest-view-export"
        queue_path = f"{queue_name}-path"

        task_name = queue_name + f"/{_REGION.region_code}-{date}-{uuid}"
        url_params = {
            "region": _REGION.region_code,
            "output_bucket": _PRIMARY_INGEST_BUCKET.bucket_name,
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
            _REGION, export_args
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
        task_name = f"{queue_name}/{_REGION.region_code}-{date}-{uuid}"
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
