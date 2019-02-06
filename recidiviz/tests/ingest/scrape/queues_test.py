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
from mock import patch

from recidiviz.ingest.scrape import queues
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

        queues.create_task(url, queue_name, params)

        body_encoded = json.dumps(params).encode()
        task = {
            'app_engine_http_request': {
                'relative_uri': url,
                'body': body_encoded
            }
        }

        mock_client.return_value.queue_path.assert_called_with(
            metadata.project_id(), metadata.region(), queue_name)
        mock_client.return_value.create_task.assert_called_with(queue_path,
                                                                task)


    @patch('google.cloud.tasks_v2beta3.CloudTasksClient')
    def test_purge_queue(self, mock_client):
        """Tests that a queue is purged."""
        queue_name = 'testqueue'
        queue_path = queue_name + '-path'
        mock_client.return_value.queue_path.return_value = queue_path

        queues.purge_queue(queue_name)

        mock_client.return_value.queue_path.assert_called_with(
            metadata.project_id(), metadata.region(), queue_name)
        mock_client.return_value.purge_queue.assert_called_with(queue_path)
