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
from recidiviz.utils import metadata


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
    def test_create_bq_monitor_task(self, mock_client, mock_uuid,
                                    mock_datetime, mock_datetime_helpers):
        """Tests that a BQ monitor task is created."""
        url = '/test/bq_monitor'
        queue_name = queues.JOB_MONITOR_QUEUE
        topic = 'test.topic'
        topic_dashes = topic.replace('.', '-')
        message = 'test message'
        uuid = 'random-uuid'
        date = '1900-01-01'
        queue_path = queue_name + '-path'
        mock_uuid.uuid4.return_value = uuid

        time = datetime.datetime(year=2019, month=7, day=20)
        mock_datetime.datetime.now.return_value = time

        mock_datetime_helpers.to_milliseconds.return_value = 60000
        time_in_seconds = 60
        time_proto = timestamp_pb2.Timestamp(seconds=time_in_seconds)

        mock_datetime.date.today.return_value = date
        mock_client.return_value.queue_path.return_value = queue_path
        task_path = queue_path + '/{}-{}-{}'.format(
            topic_dashes, date, uuid)
        mock_client.return_value.task_path.return_value = task_path

        queues.create_bq_monitor_task(topic=topic, message=message, url=url)

        params = {'topic': topic, 'message': message}
        body_encoded = json.dumps(params).encode()

        task = tasks.types.Task(
            schedule_time=time_proto,
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
    def test_create_dataflow_monitor_task(self, mock_client, mock_uuid,
                                          mock_datetime, mock_datetime_helpers):
        """Tests that a Dataflow monitor task is created."""
        url = '/test/dataflow_monitor'
        queue_name = queues.JOB_MONITOR_QUEUE
        project_id = "test-project"
        job_id = "12345"
        location = "test-location"
        topic = 'test.topic'
        uuid = 'random-uuid'
        date = '1900-01-01'
        queue_path = queue_name + '-path'
        mock_uuid.uuid4.return_value = uuid

        time = datetime.datetime(year=2019, month=7, day=20)
        mock_datetime.datetime.now.return_value = time

        mock_datetime_helpers.to_milliseconds.return_value = 300000
        time_in_seconds = 300
        time_proto = timestamp_pb2.Timestamp(seconds=time_in_seconds)

        mock_datetime.date.today.return_value = date
        mock_client.return_value.queue_path.return_value = queue_path
        task_path = queue_path + '/{}-{}-{}'.format(
            job_id, date, uuid)
        mock_client.return_value.task_path.return_value = task_path

        queues.create_dataflow_monitor_task(project_id, job_id, location,
                                            topic, url)

        params = {'project_id': project_id, 'job_id': job_id,
                  'location': location, 'topic': topic}
        body_encoded = json.dumps(params).encode()

        task = tasks.types.Task(
            schedule_time=time_proto,
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
    def test_create_task(self, mock_client, mock_uuid, mock_datetime,
                         mock_datetime_helpers):
        """Tests that a task is created."""
        url = '/test/url'
        queue_name = "queue"
        project_id = "test-project"
        job_id = "12345"
        location = "test-location"
        topic = 'test.topic'
        uuid = 'random-uuid'
        date = '1900-01-01'
        queue_path = queue_name + '-path'
        mock_uuid.uuid4.return_value = uuid

        time = datetime.datetime(year=2019, month=7, day=20)
        mock_datetime.datetime.now.return_value = time

        mock_datetime_helpers.to_milliseconds.return_value = 300000
        time_in_seconds = 300
        time_proto = timestamp_pb2.Timestamp(seconds=time_in_seconds)

        mock_datetime.date.today.return_value = date
        mock_client.return_value.queue_path.return_value = queue_path
        task_id = '/{}-{}-{}'.format(job_id, date, uuid)
        task_path = queue_path + task_id
        mock_client.return_value.task_path.return_value = task_path

        body = {'project_id': project_id, 'job_id': job_id,
                'location': location, 'topic': topic}
        body_encoded = json.dumps(body).encode()

        queues.create_task(task_id, queue_name, time_in_seconds, url, body)

        task = tasks.types.Task(
            schedule_time=time_proto,
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
