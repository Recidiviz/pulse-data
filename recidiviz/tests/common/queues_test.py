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

"""Cloud Tasks queue helper function tests."""
import datetime
import json
import unittest

from google.cloud import tasks
from google.protobuf import timestamp_pb2
from mock import call, patch

from recidiviz.common import queues
from recidiviz.ingest.direct.controllers.direct_ingest_types import IngestArgs
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsIngestArgs
from recidiviz.utils import metadata, regions

_REGION = regions.Region(
    region_code='us_nc',
    agency_name='agency_name',
    agency_type='state',
    base_url='base_url',
    shared_queue=queues.DIRECT_INGEST_STATE_TASK_QUEUE,
    timezone='America/New_York',
    jurisdiction_id='jid',
    environment='production')


class QueuesTest(unittest.TestCase):
    """Cloud Tasks queue helper function tests."""

    def setup_method(self, _test_method):
        queues.clear_client()

    def teardown_method(self, _test_method):
        queues.clear_client()

    @patch('google.cloud.tasks.CloudTasksClient')
    def test_create_scrape_task(self, mock_client):
        """Tests that a task is created."""
        url = '/test/work'
        queue_name = 'testqueue'
        params = {'a': 'hello'}
        queue_path = queue_name + '-path'
        mock_client.return_value.queue_path.return_value = queue_path
        task_path = queue_path + '/us_ny-12345'
        mock_client.return_value.task_path.return_value = task_path

        queues.create_scrape_task(
            region_code='us_ny', queue_name=queue_name, url=url, body=params)

        body_encoded = json.dumps(params).encode()
        task = tasks.types.Task(
            name=task_path,
            app_engine_http_request={
                'relative_uri': url,
                'body': body_encoded
            }
        )

        mock_client.return_value.queue_path.assert_called_with(
            metadata.project_id(), metadata.region(), queue_name)
        mock_client.return_value.create_task.assert_called_with(
            queue_path, task)

    @patch('google.cloud.tasks.CloudTasksClient')
    def test_purge_scrape_tasks(self, mock_client):
        queue_name = 'testqueue'
        queue_path = queue_name + '-path'
        mock_client.return_value.queue_path.return_value = queue_path
        task_path = queue_path + '/us_ny'
        mock_client.return_value.task_path.return_value = task_path

        mock_client.return_value.list_tasks.return_value = [
            tasks.types.Task(name=queue_path + '/us_ny-123'),
            tasks.types.Task(name=queue_path + '/us_pa-456'),
            tasks.types.Task(name=queue_path + '/us_ny-789')
        ]

        queues.purge_scrape_tasks(region_code='us_ny', queue_name=queue_name)

        mock_client.return_value.delete_task.assert_has_calls([
            call(queue_path + '/us_ny-123'), call(queue_path + '/us_ny-789')])

    @patch('google.cloud.tasks.CloudTasksClient')
    def test_list_scrape_tasks(self, mock_client):
        queue_name = 'testqueue'
        queue_path = queue_name + '-path'
        mock_client.return_value.queue_path.return_value = queue_path
        task_path = queue_path + '/us_ny'
        mock_client.return_value.task_path.return_value = task_path

        mock_client.return_value.list_tasks.return_value = [
            tasks.types.Task(name=queue_path + '/us_ny-123'),
            tasks.types.Task(name=queue_path + '/us_pa-456'),
            tasks.types.Task(name=queue_path + '/us_ny-789')
        ]

        listed_tasks = queues.list_scrape_tasks(
            region_code='us_ny', queue_name=queue_name)

        assert listed_tasks == [
            tasks.types.Task(name=queue_path + '/us_ny-123'),
            tasks.types.Task(name=queue_path + '/us_ny-789')
        ]

        mock_client.return_value.queue_path.assert_called_with(
            metadata.project_id(), metadata.region(), queue_name)
        mock_client.return_value.list_tasks.assert_called_with(queue_path)

    @patch('recidiviz.common.queues.datetime')
    @patch('recidiviz.common.queues.uuid')
    @patch('google.cloud.tasks.CloudTasksClient')
    def test_create_bq_task(self, mock_client, mock_uuid, mock_datetime):
        """Tests that a BQ export task is created."""
        url = '/test/bq'
        queue_name = queues.BIGQUERY_QUEUE
        table_name = 'test_table'
        module = 'test_module'
        uuid = 'random-uuid'
        date = '1900-01-01'
        queue_path = queue_name + '-path'
        mock_uuid.uuid4.return_value = uuid
        mock_datetime.datet.today.return_value = date
        mock_client.return_value.queue_path.return_value = queue_path
        task_path = queue_path + '/{}-{}-{}-{}'.format(
            table_name, module, date, uuid)
        mock_client.return_value.task_path.return_value = task_path

        queues.create_bq_task(
            table_name=table_name, url=url, module=module)

        params = {'table_name': table_name, 'module': module}
        body_encoded = json.dumps(params).encode()

        task = tasks.types.Task(
            name=task_path,
            app_engine_http_request={
                'relative_uri': url,
                'body': body_encoded
            }
        )

        mock_client.return_value.queue_path.assert_called_with(
            metadata.project_id(), metadata.region(), queue_name)
        mock_client.return_value.create_task.assert_called_with(
            queue_path, task)

    @patch('recidiviz.common.queues.datetime_helpers')
    @patch('recidiviz.common.queues.datetime')
    @patch('recidiviz.common.queues.uuid')
    @patch('google.cloud.tasks.CloudTasksClient')
    def test_create_direct_ingest_scheduler_queue_task(
            self, mock_client, mock_uuid, mock_datetime, mock_datetime_helpers):
        # Arrange
        delay_sec = 5
        time = datetime.datetime(year=2019, month=7, day=20)
        mock_datetime.datetime.now.return_value = time

        mock_datetime_helpers.to_milliseconds.return_value = 100000
        time_in_seconds = 100
        time_proto = timestamp_pb2.Timestamp(seconds=time_in_seconds)

        ingest_args = IngestArgs(time)
        body = {'ingest_args': ingest_args.to_serializable(),
                'args_type': 'IngestArgs'}
        body_encoded = json.dumps(body).encode()
        uuid = 'random-uuid'
        mock_uuid.uuid4.return_value = uuid
        date = '2019-07-20'
        mock_datetime.date.today.return_value = date
        queue_path = _REGION.shared_queue + '-path'

        task_name = queues.DIRECT_INGEST_SCHEDULER_QUEUE + '/{}-{}-{}'.format(
            _REGION.region_code, date, uuid)
        task = tasks.types.Task(
            name=task_name,
            schedule_time=time_proto,
            app_engine_http_request={
                'relative_uri':
                    f'/direct/scheduler?region={_REGION.region_code}',
                'body': body_encoded
            }
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        queues.create_direct_ingest_scheduler_queue_task(
            _REGION, ingest_args, delay_sec)

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            metadata.project_id(), metadata.region(),
            queues.DIRECT_INGEST_SCHEDULER_QUEUE)
        mock_client.return_value.create_task.assert_called_with(
            queue_path, task)

    @patch('recidiviz.common.queues.datetime')
    @patch('recidiviz.common.queues.uuid')
    @patch('google.cloud.tasks.CloudTasksClient')
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
        task = tasks.types.Task(
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
        queues.create_direct_ingest_process_job_task(_REGION, ingest_args)

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            metadata.project_id(), metadata.region(), _REGION.shared_queue)
        mock_client.return_value.create_task.assert_called_with(
            queue_path, task)

    @patch('recidiviz.common.queues.datetime')
    @patch('recidiviz.common.queues.uuid')
    @patch('google.cloud.tasks.CloudTasksClient')
    def test_create_direct_ingest_process_job_task_gcsfs_args(
            self, mock_client, mock_uuid, mock_datetime):
        # Arrange
        ingest_args = GcsfsIngestArgs(
            datetime.datetime(year=2019, month=7, day=20),
            file_path='file_path', storage_bucket='storage')
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
        task = tasks.types.Task(
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
        queues.create_direct_ingest_process_job_task(_REGION, ingest_args)

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            metadata.project_id(), metadata.region(), _REGION.shared_queue)
        mock_client.return_value.create_task.assert_called_with(
            queue_path, task)
