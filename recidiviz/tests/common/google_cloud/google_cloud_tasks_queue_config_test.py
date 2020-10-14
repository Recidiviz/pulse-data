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
from google.protobuf import duration_pb2
from mock import create_autospec, patch

from recidiviz.common.google_cloud import google_cloud_task_queue_config
from recidiviz.utils import metadata, regions


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

    def get_updated_queues(self) -> Dict[str, tasks_v2.Queue]:
        queues_updated_by_id: Dict[str, tasks_v2.Queue] = {}
        for method_name, _args, kwargs in self.mock_client.mock_calls:
            if method_name == 'update_queue':
                queue = kwargs['queue']
                if not isinstance(queue, tasks_v2.Queue):
                    self.fail(f"Unexpected type [{type(queue)}]")
                _, queue_id = os.path.split(queue.name)
                queues_updated_by_id[queue_id] = queue
        return queues_updated_by_id

    def test_initialize_queues(self):
        # Act
        with metadata.local_project_id_override('my-project-id'):
            google_cloud_task_queue_config.initialize_queues(
                google_auth_token='fake-auth-token')

        # Assert
        queues_updated_by_id = self.get_updated_queues()
        for queue in queues_updated_by_id.values():
            self.assertTrue(
                queue.name.startswith(
                    'projects/my-project-id/locations/us-east1/queues/'))
            self.assertEqual(queue.stackdriver_logging_config.sampling_ratio, 1.0)

        direct_ingest_queue_ids = {
            'direct-ingest-state-process-job-queue-v2',
            'direct-ingest-jpp-process-job-queue-v2',
            'direct-ingest-bq-import-export-v2',
            'direct-ingest-scheduler-v2'
        }
        self.assertFalse(
            direct_ingest_queue_ids.difference(queues_updated_by_id.keys()))

        for queue_id in direct_ingest_queue_ids:
            queue = queues_updated_by_id[queue_id]
            self.assertEqual(queue.rate_limits.max_concurrent_dispatches, 1)

        for region in regions.get_supported_regions():
            self.assertTrue(region.get_queue_name() in queues_updated_by_id)

        # Test that composition works as expected
        self.assertEqual(
            queues_updated_by_id[regions.get_region('us_ny').get_queue_name()],
            tasks_v2.Queue(
                name='projects/my-project-id/locations/us-east1/queues/us-ny-scraper-v2',
                rate_limits=tasks_v2.RateLimits(
                    # This is overridden in the manifest.yaml
                    max_dispatches_per_second=0.3,
                    max_concurrent_dispatches=3,
                ),
                retry_config=tasks_v2.RetryConfig(
                    min_backoff=duration_pb2.Duration(seconds=5),
                    max_backoff=duration_pb2.Duration(seconds=300),
                    max_attempts=5,
                ),
                stackdriver_logging_config=tasks_v2.StackdriverLoggingConfig(
                    sampling_ratio=1.0,
                )))

        # Test that other regions are unaffected
        self.assertEqual(
            queues_updated_by_id[regions.get_region('us_pa').get_queue_name()],
            tasks_v2.Queue(
                name='projects/my-project-id/locations/us-east1/queues/us-pa-scraper-v2',
                rate_limits=tasks_v2.RateLimits(
                    max_dispatches_per_second=0.08333333333,
                    max_concurrent_dispatches=3,
                ),
                retry_config=tasks_v2.RetryConfig(
                    min_backoff=duration_pb2.Duration(seconds=5),
                    max_backoff=duration_pb2.Duration(seconds=300),
                    max_attempts=5,
                ),
                stackdriver_logging_config=tasks_v2.StackdriverLoggingConfig(
                    sampling_ratio=1.0,
                )))

        self.assertTrue('bigquery-v2' in queues_updated_by_id)
        self.assertTrue('job-monitor-v2' in queues_updated_by_id)
        self.assertTrue('scraper-phase-v2' in queues_updated_by_id)
