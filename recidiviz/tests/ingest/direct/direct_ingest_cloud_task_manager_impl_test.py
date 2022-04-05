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

from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
from mock import patch

from recidiviz.common.google_cloud.google_cloud_task_queue_config import \
    DIRECT_INGEST_SCHEDULER_QUEUE_V2, DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import \
    to_normalized_unprocessed_file_path
from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsFilePath
from recidiviz.common import queues
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import \
    DirectIngestCloudTaskManagerImpl, CloudTaskQueueInfo, _build_task_id
from recidiviz.ingest.direct.controllers.direct_ingest_types import IngestArgs
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    GcsfsIngestArgs
from recidiviz.utils import metadata, regions

_REGION = regions.Region(
    region_code='us_nc',
    agency_name='agency_name',
    agency_type='state',
    base_url='base_url',
    shared_queue=DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2,
    timezone='America/New_York',
    jurisdiction_id='jid',
    environment='production')


class TestCloudTaskQueueInfo(TestCase):
    """Tests for the CloudTaskQueueInfo."""

    def test_is_task_queued_no_tasks(self):
        # Arrange
        info = CloudTaskQueueInfo(queue_name='queue_name', task_names=[])

        file_path = to_normalized_unprocessed_file_path('bucket/file_path.csv')
        args = IngestArgs(ingest_time=datetime.datetime.now())
        gcsfs_args = \
            GcsfsIngestArgs(
                ingest_time=datetime.datetime.now(),
                file_path=GcsfsFilePath.from_absolute_path(file_path))

        # Act
        basic_args_queued = info.is_task_queued(_REGION, args)
        gcsfs_args_queued = info.is_task_queued(_REGION, gcsfs_args)

        # Assert
        self.assertFalse(basic_args_queued)
        self.assertFalse(gcsfs_args_queued)

        self.assertFalse(info.is_task_queued(_REGION, gcsfs_args))

    def test_is_task_queued_has_tasks(self):
        # Arrange
        file_path = to_normalized_unprocessed_file_path('bucket/file_path.csv')
        gcsfs_args = \
            GcsfsIngestArgs(
                ingest_time=datetime.datetime.now(),
                file_path=GcsfsFilePath.from_absolute_path(file_path))

        full_task_name = \
            _build_task_id(_REGION.region_code, gcsfs_args.task_id_tag())
        info = CloudTaskQueueInfo(
            queue_name='queue_name',
            task_names=[f'projects/path/to/random_task',
                        f'projects/path/to/{full_task_name}'])
        file_path = to_normalized_unprocessed_file_path('bucket/file_path.csv')
        gcsfs_args = \
            GcsfsIngestArgs(
                ingest_time=datetime.datetime.now(),
                file_path=GcsfsFilePath.from_absolute_path(file_path))

        # Act
        gcsfs_args_queued = info.is_task_queued(_REGION, gcsfs_args)

        # Assert
        self.assertTrue(gcsfs_args_queued)


class TestDirectIngestCloudTaskManagerImpl(TestCase):
    """Tests for the DirectIngestCloudTaskManagerImpl."""

    def setup_method(self, _test_method):
        queues.clear_legacy_cloud_tasks_client()

    def teardown_method(self, _test_method):
        queues.clear_legacy_cloud_tasks_client()

    @patch('recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper.'
           'datetime_helpers')
    @patch('recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper.'
           'datetime')
    @patch('recidiviz.ingest.direct.direct_ingest_cloud_task_manager.uuid')
    @patch('google.cloud.tasks_v2.CloudTasksClient')
    def test_create_direct_ingest_scheduler_queue_task(
            self, mock_client, mock_uuid, mock_datetime, mock_datetime_helpers):
        # Arrange
        delay_sec = 5
        time = datetime.datetime(year=2019, month=7, day=20)
        mock_datetime.datetime.now.return_value = time

        mock_datetime_helpers.to_milliseconds.return_value = 100000
        time_in_seconds = 100
        time_proto = timestamp_pb2.Timestamp(seconds=time_in_seconds)

        body = {}
        body_encoded = json.dumps(body).encode()
        uuid = 'random-uuid'
        mock_uuid.uuid4.return_value = uuid
        date = '2019-07-20'
        mock_datetime.date.today.return_value = date
        queue_path = _REGION.shared_queue + '-path'

        task_name = DIRECT_INGEST_SCHEDULER_QUEUE_V2 + \
            '/{}-{}-{}'.format(_REGION.region_code, date, uuid)
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            schedule_time=time_proto,
            app_engine_http_request={
                'relative_uri':
                    f'/direct/scheduler?region={_REGION.region_code}&'
                    f'just_finished_job=False',
                'body': body_encoded
            }
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskManagerImpl().\
            create_direct_ingest_scheduler_queue_task(_REGION, False, delay_sec)

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            metadata.project_id(), metadata.region(),
            DIRECT_INGEST_SCHEDULER_QUEUE_V2)
        mock_client.return_value.create_task.assert_called_with(
            queue_path, task)

    @patch('recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper.'
           'datetime')
    @patch('recidiviz.ingest.direct.direct_ingest_cloud_task_manager.uuid')
    @patch('google.cloud.tasks_v2.CloudTasksClient')
    def test_create_direct_ingest_process_job_task(
            self, mock_client, mock_uuid, mock_datetime):
        # Arrange
        ingest_args = IngestArgs(datetime.datetime(year=2019, month=7, day=20))
        body = {'ingest_args': ingest_args.to_serializable(),
                'args_type': 'IngestArgs'}
        body_encoded = json.dumps(body).encode()
        uuid = 'random-uuid'
        mock_uuid.uuid4.return_value = uuid
        date = '2019-07-20'
        mock_datetime.date.today.return_value = date
        queue_path = _REGION.shared_queue + '-path'

        task_name = _REGION.shared_queue + '/{}-{}-{}'.format(
            _REGION.region_code, date, uuid)
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            app_engine_http_request={
                'relative_uri':
                    f'/direct/process_job?region={_REGION.region_code}',
                'body': body_encoded
            }
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskManagerImpl().\
            create_direct_ingest_process_job_task(_REGION, ingest_args)

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            metadata.project_id(), metadata.region(), _REGION.shared_queue)
        mock_client.return_value.create_task.assert_called_with(
            queue_path, task)

    @patch('recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper.'
           'datetime')
    @patch('recidiviz.ingest.direct.direct_ingest_cloud_task_manager.uuid')
    @patch('google.cloud.tasks_v2.CloudTasksClient')
    def test_create_direct_ingest_process_job_task_gcsfs_args(
            self, mock_client, mock_uuid, mock_datetime):
        # Arrange
        file_path = to_normalized_unprocessed_file_path('bucket/file_path.csv')
        ingest_args = \
            GcsfsIngestArgs(
                ingest_time=datetime.datetime(year=2019, month=7, day=20),
                file_path=GcsfsFilePath.from_absolute_path(file_path))
        body = {'ingest_args': ingest_args.to_serializable(),
                'args_type': 'GcsfsIngestArgs'}
        body_encoded = json.dumps(body).encode()
        uuid = 'random-uuid'
        mock_uuid.uuid4.return_value = uuid
        date = '2019-07-20'
        mock_datetime.date.today.return_value = date
        queue_path = _REGION.shared_queue + '-path'

        task_name = _REGION.shared_queue + '/{}-{}-{}'.format(
            _REGION.region_code, date, uuid)
        task = tasks_v2.types.task_pb2.Task(
            name=task_name,
            app_engine_http_request={
                'relative_uri':
                    f'/direct/process_job?region={_REGION.region_code}',
                'body': body_encoded
            }
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskManagerImpl().\
            create_direct_ingest_process_job_task(_REGION, ingest_args)

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            metadata.project_id(), metadata.region(), _REGION.shared_queue)
        mock_client.return_value.create_task.assert_called_with(
            queue_path, task)
