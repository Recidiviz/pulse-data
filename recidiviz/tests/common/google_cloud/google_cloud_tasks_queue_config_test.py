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
"""Tests for google_cloud_task_queue_config.py."""

import os
import unittest
from typing import Dict

from google.cloud import tasks_v2
from google.cloud.tasks_v2.proto import queue_pb2
from mock import patch, create_autospec

from recidiviz.common.google_cloud import google_cloud_task_queue_config
from recidiviz.utils import regions


class TestGoogleCloudTasksQueueConfig(unittest.TestCase):
    """Tests for google_cloud_task_queue_config.py."""

    @staticmethod
    def create_mock_cloud_tasks_client():
        mock_client = create_autospec(tasks_v2.CloudTasksClient)
        mock_client.queue_path.side_effect = \
            tasks_v2.CloudTasksClient.queue_path

        return mock_client

    def setUp(self):
        self.mock_client_patcher = patch(
            'google.cloud.tasks_v2.CloudTasksClient',
            return_value=self.create_mock_cloud_tasks_client())

        self.mock_client_cls = self.mock_client_patcher.start()
        self.mock_client = self.mock_client_cls()

    def tearDown(self):
        self.mock_client_patcher.stop()

    @patch('recidiviz.utils.metadata.region')
    @patch('recidiviz.utils.metadata.project_id')
    def test_initialize_queues(self, mock_project_id, mock_instance_region):
        # Arrange
        mock_project_id.return_value = 'my-project-id'
        mock_instance_region.return_value = 'us-east1'

        # Act
        google_cloud_task_queue_config.initialize_queues()

        # Assert
        queues_updated_by_id: Dict[str, queue_pb2.Queue] = {}
        for method_name, args, _kwargs in self.mock_client.mock_calls:
            if method_name == 'update_queue':
                queue = args[0]
                if not isinstance(queue, queue_pb2.Queue):
                    self.fail(f"Unexpected type [{type(queue)}]")
                _, queue_id = os.path.split(queue.name)
                queues_updated_by_id[queue_id] = queue

        for queue in queues_updated_by_id.values():
            self.assertTrue(
                queue.name.startswith(
                    'projects/my-project-id/locations/us-east1/queues/'))

        direct_ingest_queue_ids = {
            'direct-ingest-state-process-job-queue-v2',
            'direct-ingest-jpp-process-job-queue-v2',
            'direct-ingest-scheduler-v2'
        }
        self.assertFalse(
            direct_ingest_queue_ids.difference(queues_updated_by_id.keys()))

        for queue_id in direct_ingest_queue_ids:
            queue = queues_updated_by_id[queue_id]
            self.assertEqual(queue.rate_limits.max_concurrent_dispatches, 1)

        for region in regions.get_supported_regions():
            self.assertTrue(region.get_queue_name() in queues_updated_by_id)

        self.assertTrue('bigquery-v2' in queues_updated_by_id)
        self.assertTrue('job-monitor-v2' in queues_updated_by_id)
        self.assertTrue('scraper-phase-v2' in queues_updated_by_id)
