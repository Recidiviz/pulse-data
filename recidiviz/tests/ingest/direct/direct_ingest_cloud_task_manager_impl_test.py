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
from mock import patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import (
    QUEUES_REGION,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    DirectIngestCloudTaskManagerImpl,
    DirectIngestQueueType,
    ExtractAndMergeCloudTaskQueueInfo,
    IngestViewMaterializationCloudTaskQueueInfo,
    RawDataImportCloudTaskQueueInfo,
    _build_task_id,
)
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_file_name,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
)
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.types.cloud_task_args import (
    ExtractAndMergeArgs,
    GcsfsRawDataBQImportArgs,
    IngestViewMaterializationArgs,
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

_PRIMARY_INGEST_BUCKET = gcsfs_direct_ingest_bucket_for_state(
    project_id="recidiviz-456",
    region_code=_REGION.region_code,
    ingest_instance=DirectIngestInstance.PRIMARY,
)
_SECONDARY_INGEST_BUCKET = gcsfs_direct_ingest_bucket_for_state(
    project_id="recidiviz-456",
    region_code=_REGION.region_code,
    ingest_instance=DirectIngestInstance.SECONDARY,
)


class TestDirectIngestQueueType(TestCase):
    def test_exists_for_instance(self) -> None:
        primary_only_queue_types = {
            DirectIngestQueueType.SFTP_QUEUE,
            DirectIngestQueueType.RAW_DATA_IMPORT,
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


class TestIngestViewMaterializationCloudTaskQueueInfo(TestCase):
    """Tests for the IngestViewMaterializationCloudTaskQueueInfo."""

    def setUp(self) -> None:
        self.primary_ingest_view_export_args = IngestViewMaterializationArgs(
            ingest_view_name="my_file_tag",
            ingest_instance=DirectIngestInstance.PRIMARY,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime.now(),
        )
        self.secondary_ingest_view_export_args = attr.evolve(
            self.primary_ingest_view_export_args,
            ingest_instance=DirectIngestInstance.SECONDARY,
        )

    def test_info_no_tasks(self) -> None:
        info = IngestViewMaterializationCloudTaskQueueInfo(
            queue_name="queue_name", task_names=[]
        )

        for instance in DirectIngestInstance:
            self.assertFalse(
                info.has_any_tasks_for_instance(_REGION.region_code, instance)
            )
            self.assertFalse(
                info.has_ingest_view_materialization_jobs_queued(
                    _REGION.region_code, instance
                )
            )
        self.assertFalse(
            info.is_ingest_view_materialization_task_already_queued(
                _REGION.region_code, self.primary_ingest_view_export_args
            )
        )

    def test_single_ingest_view_task(self) -> None:
        full_task_name = _build_task_id(
            _REGION.region_code,
            DirectIngestInstance.SECONDARY,
            self.secondary_ingest_view_export_args.task_id_tag(),
        )

        info = IngestViewMaterializationCloudTaskQueueInfo(
            queue_name="queue_name",
            task_names=[
                f"projects/path/to/{full_task_name}",
            ],
        )

        for instance in DirectIngestInstance:
            self.assertEqual(
                instance == DirectIngestInstance.SECONDARY,
                info.has_ingest_view_materialization_jobs_queued(
                    _REGION.region_code, instance
                ),
            )
            self.assertEqual(
                instance == DirectIngestInstance.SECONDARY,
                info.has_any_tasks_for_instance(_REGION.region_code, instance),
            )

        self.assertFalse(
            info.is_ingest_view_materialization_task_already_queued(
                _REGION.region_code, self.primary_ingest_view_export_args
            )
        )
        self.assertTrue(
            info.is_ingest_view_materialization_task_already_queued(
                _REGION.region_code, self.secondary_ingest_view_export_args
            )
        )


class TestExtractAndMergeCloudTaskQueueInfo(TestCase):
    """Tests for the ExtractAndMergeCloudTaskQueueInfo."""

    def setUp(self) -> None:
        self.ingest_instance = DirectIngestInstance.PRIMARY
        self.ingest_view_name = "my_ingest_view"

    def test_info_no_tasks(self) -> None:
        # Arrange
        info = ExtractAndMergeCloudTaskQueueInfo(queue_name="queue_name", task_names=[])

        gcsfs_args = ExtractAndMergeArgs(
            ingest_time=datetime.datetime.now(),
            ingest_instance=self.ingest_instance,
            ingest_view_name=self.ingest_view_name,
            upper_bound_datetime_inclusive=datetime.datetime(2022, 1, 1, 1, 1, 1),
            batch_number=0,
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
        gcsfs_args = ExtractAndMergeArgs(
            ingest_time=datetime.datetime.now(),
            ingest_instance=self.ingest_instance,
            ingest_view_name=self.ingest_view_name,
            upper_bound_datetime_inclusive=datetime.datetime(2022, 1, 1, 1, 1, 1),
            batch_number=0,
        )

        full_task_name = _build_task_id(
            _REGION.region_code, DirectIngestInstance.PRIMARY, gcsfs_args.task_id_tag()
        )
        info = ExtractAndMergeCloudTaskQueueInfo(
            queue_name="queue_name",
            task_names=[
                "projects/path/to/random_task",
                f"projects/path/to/{full_task_name}",
            ],
        )
        gcsfs_args = ExtractAndMergeArgs(
            ingest_time=datetime.datetime.now(),
            ingest_instance=self.ingest_instance,
            ingest_view_name=self.ingest_view_name,
            upper_bound_datetime_inclusive=datetime.datetime(2022, 1, 1, 1, 1, 1),
            batch_number=0,
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
        gcsfs_args = ExtractAndMergeArgs(
            ingest_time=datetime.datetime.now(),
            ingest_instance=self.ingest_instance,
            ingest_view_name=self.ingest_view_name,
            upper_bound_datetime_inclusive=datetime.datetime(2022, 1, 1, 1, 1, 1),
            batch_number=0,
        )

        full_task_names = [
            _build_task_id(
                _REGION.region_code,
                ingest_instance,
                gcsfs_args.task_id_tag(),
            )
            for ingest_instance in DirectIngestInstance
        ]

        info = ExtractAndMergeCloudTaskQueueInfo(
            queue_name="queue_name",
            task_names=[
                "projects/path/to/random_task",
            ]
            + [
                f"projects/path/to/{full_task_name}"
                for full_task_name in full_task_names
            ],
        )
        gcsfs_args = ExtractAndMergeArgs(
            ingest_time=datetime.datetime.now(),
            ingest_instance=self.ingest_instance,
            ingest_view_name=self.ingest_view_name,
            upper_bound_datetime_inclusive=datetime.datetime(2022, 1, 1, 1, 1, 1),
            batch_number=0,
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
        self,
        mock_client: mock.MagicMock,
        mock_uuid: mock.MagicMock,
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
        self,
        mock_client: mock.MagicMock,
        mock_uuid: mock.MagicMock,
    ) -> None:
        # Arrange
        body_encoded = json.dumps({}).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        queue_path = "us_xx-scheduler-queue-path"
        queue_name = "direct-ingest-state-us-xx-scheduler-secondary"

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
    def test_create_direct_ingest_extract_and_merge_task(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        dt = datetime.datetime(2019, 11, 22, 11, 22, 33, 444444)
        args = ExtractAndMergeArgs(
            ingest_time=datetime.datetime.now(),
            ingest_view_name="my_ingest_view_name",
            ingest_instance=DirectIngestInstance.PRIMARY,
            upper_bound_datetime_inclusive=dt,
            batch_number=2,
        )
        body = {
            "cloud_task_args": args.to_serializable(),
            "args_type": "ExtractAndMergeArgs",
        }
        body_encoded = json.dumps(body).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        date = "2019-07-20"
        queue_path = "us-xx-extract-and-merge-path"
        queue_name = "direct-ingest-state-us-xx-extract-and-merge"

        task_name = f"{queue_name}/{_REGION.region_code}-{date}-{uuid}"
        url_params = {
            "region": _REGION.region_code,
            "ingest_instance": "primary",
            "ingest_view_name": "my_ingest_view_name",
        }
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": f"/direct/extract_and_merge?{urlencode(url_params)}",
                "body": body_encoded,
            },
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskManagerImpl().create_direct_ingest_extract_and_merge_task(
            _REGION, args
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
    def test_create_direct_ingest_extract_and_merge_task_secondary(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        dt = datetime.datetime(2019, 11, 22, 11, 22, 33, 444444)
        args = ExtractAndMergeArgs(
            ingest_time=datetime.datetime.now(),
            ingest_view_name="my_ingest_view_name",
            ingest_instance=DirectIngestInstance.SECONDARY,
            upper_bound_datetime_inclusive=dt,
            batch_number=2,
        )
        body = {
            "cloud_task_args": args.to_serializable(),
            "args_type": "ExtractAndMergeArgs",
        }
        body_encoded = json.dumps(body).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        date = "2019-07-20"
        queue_path = "us-xx-extract-and-merge-path"
        queue_name = "direct-ingest-state-us-xx-extract-and-merge-secondary"

        task_name = f"{queue_name}/{_REGION.region_code}-{date}-{uuid}"
        url_params = {
            "region": _REGION.region_code,
            "ingest_instance": "secondary",
            "ingest_view_name": "my_ingest_view_name",
        }
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": f"/direct/extract_and_merge?{urlencode(url_params)}",
                "body": body_encoded,
            },
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskManagerImpl().create_direct_ingest_extract_and_merge_task(
            _REGION,
            args,
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
        self,
        mock_client: mock.MagicMock,
        mock_uuid: mock.MagicMock,
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
    def test_create_direct_ingest_ingest_view_materialization_task(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        export_args = IngestViewMaterializationArgs(
            ingest_view_name="my_ingest_view",
            ingest_instance=DirectIngestInstance.PRIMARY,
            lower_bound_datetime_exclusive=datetime.datetime(2020, 4, 29),
            upper_bound_datetime_inclusive=datetime.datetime(2020, 4, 30),
        )
        body = {
            "cloud_task_args": export_args.to_serializable(),
            "args_type": "IngestViewMaterializationArgs",
        }
        body_encoded = json.dumps(body).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        date = "2019-07-20"
        queue_name = "direct-ingest-state-us-xx-materialize-ingest-view"
        queue_path = f"{queue_name}-path"

        task_name = queue_name + f"/{_REGION.region_code}-{date}-{uuid}"
        url_params = {
            "region": _REGION.region_code,
            "ingest_instance": "primary",
            "ingest_view_name": "my_ingest_view",
        }
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": f"/direct/materialize_ingest_view?{urlencode(url_params)}",
                "body": body_encoded,
            },
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskManagerImpl().create_direct_ingest_view_materialization_task(
            _REGION,
            export_args,
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
        self,
        mock_client: mock.MagicMock,
        mock_uuid: mock.MagicMock,
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
