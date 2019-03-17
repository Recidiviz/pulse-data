# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

import json
import unittest
from mock import call, patch

from google.cloud.tasks_v2beta3.types import Task

from recidiviz.common import queues
from recidiviz.utils import metadata


class QueuesTest(unittest.TestCase):
    """Cloud Tasks queue helper function tests."""

    def setup_method(self, _test_method):
        queues.clear_client()

    def teardown_method(self, _test_method):
        queues.clear_client()


    @patch('google.cloud.tasks_v2beta3.CloudTasksClient')
    def test_create_task(self, mock_client):
        """Tests that a task is created."""
        url = '/test/work'
        queue_name = 'testqueue'
        params = {'a': 'hello'}
        queue_path = queue_name + '-path'
        mock_client.return_value.queue_path.return_value = queue_path
        task_path = queue_path + '/us_ny-12345'
        mock_client.return_value.task_path.return_value = task_path

        queues.create_task(
            region_code='us_ny', queue_name=queue_name, url=url, body=params)

        body_encoded = json.dumps(params).encode()
        task = Task(
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


    @patch('google.cloud.tasks_v2beta3.CloudTasksClient')
    def test_purge_tasks(self, mock_client):
        queue_name = 'testqueue'
        queue_path = queue_name + '-path'
        mock_client.return_value.queue_path.return_value = queue_path
        task_path = queue_path + '/us_ny'
        mock_client.return_value.task_path.return_value = task_path

        mock_client.return_value.list_tasks.return_value = [
            Task(name=queue_path + '/us_ny-123'),
            Task(name=queue_path + '/us_pa-456'),
            Task(name=queue_path + '/us_ny-789')
        ]

        queues.purge_tasks(region_code='us_ny', queue_name=queue_name)

        mock_client.return_value.delete_task.assert_has_calls([
            call(queue_path + '/us_ny-123'), call(queue_path + '/us_ny-789')])

    @patch('google.cloud.tasks_v2beta3.CloudTasksClient')
    def test_list_tasks(self, mock_client):
        queue_name = 'testqueue'
        queue_path = queue_name + '-path'
        mock_client.return_value.queue_path.return_value = queue_path
        task_path = queue_path + '/us_ny'
        mock_client.return_value.task_path.return_value = task_path

        mock_client.return_value.list_tasks.return_value = [
            Task(name=queue_path + '/us_ny-123'),
            Task(name=queue_path + '/us_pa-456'),
            Task(name=queue_path + '/us_ny-789')
        ]

        tasks = queues.list_tasks(region_code='us_ny', queue_name=queue_name)

        assert tasks == [Task(name=queue_path + '/us_ny-123'),
                         Task(name=queue_path + '/us_ny-789')]

        mock_client.return_value.queue_path.assert_called_with(
            metadata.project_id(), metadata.region(), queue_name)
        mock_client.return_value.list_tasks.assert_called_with(queue_path)
