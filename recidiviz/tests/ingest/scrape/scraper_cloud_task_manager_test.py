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
"""Tests for ScraperCloudTaskManager"""

import datetime
import json
import unittest

from freezegun import freeze_time
from google.cloud import tasks_v2
from mock import patch, call

from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import \
    SCRAPER_PHASE_QUEUE_V2
from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import \
    QUEUES_REGION
from recidiviz.ingest.scrape import scraper_cloud_task_manager
from recidiviz.ingest.scrape.constants import ScrapeType, TaskType
from recidiviz.ingest.scrape.scraper_cloud_task_manager import \
    ScraperCloudTaskManager
from recidiviz.ingest.scrape.task_params import QueueRequest, Task

CLOUD_TASK_MANAGER_PACKAGE_NAME = scraper_cloud_task_manager.__name__


class TestScraperCloudTaskManager(unittest.TestCase):
    """Tests for ScraperCloudTaskManager"""

    def task_path(self, project, queues_region, queue, task_id):
        return f'queue_path/{project}/{queues_region}/{queue}/{task_id}'

    @patch('google.cloud.tasks_v2.CloudTasksClient')
    def test_purge_scrape_tasks(self, mock_client):
        # Arrange
        region_code = 'us_ca_san_francisco'
        project_id = 'recidiviz-456'

        queue_name = 'test-queue-name'
        queue_path = f'queue_path/{project_id}/{QUEUES_REGION}/{queue_name}'

        task1 = tasks_v2.types.task_pb2.Task(
            name=self.task_path(project_id,
                                QUEUES_REGION,
                                queue_name,
                                'us_ca_san_francisco-12345')
        )
        task2 = tasks_v2.types.task_pb2.Task(
            name=self.task_path(project_id,
                                QUEUES_REGION,
                                queue_name,
                                'us_ca_san_francisco-12345')
        )

        mock_client.return_value.list_tasks.return_value = [task1, task2]

        mock_client.return_value.task_path.side_effect = self.task_path
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        ScraperCloudTaskManager(project_id=project_id). \
            purge_scrape_tasks(region_code=region_code,
                               queue_name=queue_name)

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            project_id,
            QUEUES_REGION,
            queue_name)
        mock_client.return_value.list_tasks.assert_called_with(parent=queue_path)
        self.assertEqual(
            mock_client.return_value.delete_task.mock_calls,
            [
                call(name=task1.name),
                call(name=task2.name),
            ]
        )

    @patch('google.cloud.tasks_v2.CloudTasksClient')
    def test_list_scrape_tasks(self, mock_client):
        # Arrange
        region_code = 'us_ca_san_francisco'
        project_id = 'recidiviz-456'

        queue_name = 'test-queue-name'
        queue_path = f'queue_path/{project_id}/{QUEUES_REGION}/{queue_name}'

        mock_client.return_value.task_path.side_effect = self.task_path
        mock_client.return_value.queue_path.return_value = queue_path

        task1 = tasks_v2.types.task_pb2.Task(
            name=self.task_path(project_id,
                                QUEUES_REGION,
                                queue_name,
                                'us_ca_san_francisco-12345')
        )
        task2 = tasks_v2.types.task_pb2.Task(
            name=self.task_path(project_id,
                                QUEUES_REGION,
                                queue_name,
                                'us_ca_san_mateo-12345')
        )

        mock_client.return_value.list_tasks.return_value = [task1, task2]

        # Act
        tasks = ScraperCloudTaskManager(project_id=project_id). \
            list_scrape_tasks(region_code=region_code,
                              queue_name=queue_name)

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            project_id,
            QUEUES_REGION,
            queue_name)
        mock_client.return_value.list_tasks.assert_called_with(parent=queue_path)

        self.assertCountEqual(tasks, [task1])

    @patch(f'{CLOUD_TASK_MANAGER_PACKAGE_NAME}.uuid')
    @patch('google.cloud.tasks_v2.CloudTasksClient')
    @freeze_time('2019-04-14')
    def test_create_scrape_task(self, mock_client, mock_uuid):
        # Arrange
        uuid = 'random-uuid'
        mock_uuid.uuid4.return_value = uuid

        region_code = 'us_ca_san_francisco'
        project_id = 'recidiviz-456'

        queue_name = 'test-queue-name'
        queue_path = f'queue_path/{project_id}/{QUEUES_REGION}/{queue_name}'
        task_id = 'us_ca_san_francisco-random-uuid'
        task_path = f'{queue_path}/{task_id}'
        url = '/my_scrape/task'

        body = {
            'region': region_code,
            'params': QueueRequest(
                next_task=Task(
                    task_type=TaskType.INITIAL,
                    endpoint='www.google.com'
                ),
                scrape_type=ScrapeType.BACKGROUND,
                scraper_start_time=datetime.datetime.now()
            ).to_serializable()
        }
        task = tasks_v2.types.task_pb2.Task(
            name=task_path,
            app_engine_http_request={
                'http_method': 'POST',
                'relative_uri': url,
                'body': json.dumps(body).encode()
            }
        )

        mock_client.return_value.task_path.return_value = task_path
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        ScraperCloudTaskManager(project_id=project_id). \
            create_scrape_task(region_code=region_code,
                               queue_name=queue_name,
                               url=url,
                               body=body)

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            project_id,
            QUEUES_REGION,
            queue_name)
        mock_client.return_value.task_path.assert_called_with(
            project_id,
            QUEUES_REGION,
            queue_name,
            task_id)
        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task)

    @patch(f'{CLOUD_TASK_MANAGER_PACKAGE_NAME}.uuid')
    @patch('google.cloud.tasks_v2.CloudTasksClient')
    def test_create_scraper_phase_task(self, mock_client, mock_uuid):
        # Arrange
        uuid = 'random-uuid'
        mock_uuid.uuid4.return_value = uuid

        region_code = 'us_ca_san_francisco'
        project_id = 'recidiviz-456'

        queue_path = f'queue_path/{project_id}/{QUEUES_REGION}'
        task_id = 'us_ca_san_francisco-random-uuid'
        task_path = f'{queue_path}/{task_id}'
        url = '/my_enqueue/phase'

        task = tasks_v2.types.task_pb2.Task(
            name=task_path,
            app_engine_http_request={
                'http_method': 'GET',
                'relative_uri': f'{url}?region={region_code}',
            }
        )

        mock_client.return_value.task_path.return_value = task_path
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        ScraperCloudTaskManager(project_id=project_id). \
            create_scraper_phase_task(region_code=region_code, url=url)

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            project_id,
            QUEUES_REGION,
            SCRAPER_PHASE_QUEUE_V2)
        mock_client.return_value.task_path.assert_called_with(
            project_id,
            QUEUES_REGION,
            SCRAPER_PHASE_QUEUE_V2,
            task_id)
        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task)
