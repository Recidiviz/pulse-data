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
"""Tests for dataflow_monitor_manager.py."""
from http import HTTPStatus
import json
import unittest
from unittest import mock

import flask
from mock import Mock

from recidiviz.calculator.pipeline.utils import dataflow_monitor_manager

MONITOR_MANAGER_PACKAGE_NAME = dataflow_monitor_manager.__name__


@mock.patch('recidiviz.utils.metadata.project_number', Mock(return_value='123456789'))
class DataflowMonitorManagerTest(unittest.TestCase):
    """Tests for dataflow_monitor_manager.py."""

    def setUp(self):
        self.mock_app = flask.Flask(__name__)
        self.mock_app.config['TESTING'] = True
        self.mock_app.register_blueprint(
            dataflow_monitor_manager.dataflow_monitor_blueprint)
        self.mock_flask_client = self.mock_app.test_client()

    @mock.patch('recidiviz.utils.metadata.project_id')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.pubsub_helper')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.CalculateCloudTaskManager')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.get_dataflow_job_with_id')
    def test_handle_dataflow_monitor_task_success(self, mock_get_job,
                                                  mock_cloud_task_manager,
                                                  mock_pubsub_helper,
                                                  mock_project_id):
        """Tests that a message is published to a Pub/Sub topic when a Dataflow
        job is found that has successfully completed."""
        mock_get_job.return_value = {
            'currentState': 'JOB_STATE_DONE'
        }

        project_id = 'test-project'
        mock_project_id.return_value = project_id
        job_id = "12345"
        location = "fake_location"
        topic = 'fake_topic'
        message = 'Dataflow job {} complete'.format(job_id)
        route = '/monitor'
        data = {"job_id": job_id, "location": location, "topic": topic}

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type='application/json',
            headers={'X-Appengine-Inbound-Appid': 'test-project'})
        assert response.status_code == HTTPStatus.OK

        mock_cloud_task_manager.return_value.create_dataflow_monitor_task. \
            assert_not_called()
        mock_pubsub_helper.publish_message_to_topic.assert_called_with(message,
                                                                       topic)

    @mock.patch('recidiviz.utils.metadata.project_id')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.pubsub_helper')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.CalculateCloudTaskManager')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.get_dataflow_job_with_id')
    def test_handle_dataflow_monitor_task_running(self, mock_get_job,
                                                  mock_cloud_task_manager,
                                                  mock_pubsub_helper,
                                                  mock_project_id):
        """Tests that a new Dataflow monitor task is created when the Dataflow
        job is still running."""
        mock_get_job.return_value = {
            'currentState': 'JOB_STATE_RUNNING'
        }

        project_id = 'test-project'
        mock_project_id.return_value = project_id
        job_id = "12345"
        location = "fake_location"
        topic = 'fake_topic'
        route = '/monitor'
        data = {"job_id": job_id, "location": location, "topic": topic}

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type='application/json',
            headers={'X-Appengine-Inbound-Appid': 'test-project'})
        assert response.status_code == HTTPStatus.OK

        mock_cloud_task_manager.return_value. \
            create_dataflow_monitor_task.assert_called_with(job_id,
                                                            location,
                                                            topic)
        mock_pubsub_helper.publish_message_to_topic.assert_not_called()

    @mock.patch('recidiviz.utils.metadata.project_id')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.pubsub_helper')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.CalculateCloudTaskManager')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.get_dataflow_job_with_id')
    def test_handle_dataflow_monitor_task_stopped(self, mock_get_job,
                                                  mock_cloud_task_manager,
                                                  mock_pubsub_helper,
                                                  mock_project_id):
        """Tests that a new Dataflow monitor task is created when the Dataflow
        job is still temporarily stopped."""
        mock_get_job.return_value = {
            'currentState': 'JOB_STATE_STOPPED'
        }

        project_id = 'test-project'
        mock_project_id.return_value = project_id
        job_id = "12345"
        location = "fake_location"
        topic = 'fake_topic'
        route = '/monitor'
        data = {"job_id": job_id, "location": location, "topic": topic}

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type='application/json',
            headers={'X-Appengine-Inbound-Appid': 'test-project'})
        assert response.status_code == HTTPStatus.OK

        mock_cloud_task_manager.return_value.create_dataflow_monitor_task. \
            assert_called_with(job_id,
                               location,
                               topic)
        mock_pubsub_helper.publish_message_to_topic.assert_not_called()

    @mock.patch('recidiviz.utils.metadata.project_id')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.pubsub_helper')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.CalculateCloudTaskManager')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.get_dataflow_job_with_id')
    def test_handle_dataflow_monitor_task_pending(self, mock_get_job,
                                                  mock_cloud_task_manager,
                                                  mock_pubsub_helper,
                                                  mock_project_id):
        """Tests that a new Dataflow monitor task is created when the Dataflow
        job is pending."""
        mock_get_job.return_value = {
            'currentState': 'JOB_STATE_PENDING'
        }

        project_id = 'test-project'
        mock_project_id.return_value = project_id
        job_id = "12345"
        location = "fake_location"
        topic = 'fake_topic'
        route = '/monitor'
        data = {"job_id": job_id, "location": location, "topic": topic}

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type='application/json',
            headers={'X-Appengine-Inbound-Appid': 'test-project'})
        assert response.status_code == HTTPStatus.OK

        mock_cloud_task_manager.return_value.create_dataflow_monitor_task. \
            assert_called_with(job_id,
                               location,
                               topic)
        mock_pubsub_helper.publish_message_to_topic.assert_not_called()

    @mock.patch('recidiviz.utils.metadata.project_id')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.pubsub_helper')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.CalculateCloudTaskManager')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.get_dataflow_job_with_id')
    def test_handle_dataflow_monitor_task_queued(self, mock_get_job,
                                                 mock_cloud_task_manager,
                                                 mock_pubsub_helper,
                                                 mock_project_id):
        """Tests that a new Dataflow monitor task is created when the Dataflow
        job is queued."""
        mock_get_job.return_value = {
            'currentState': 'JOB_STATE_QUEUED'
        }

        project_id = 'test-project'
        mock_project_id.return_value = project_id
        job_id = "12345"
        location = "fake_location"
        topic = 'fake_topic'
        route = '/monitor'
        data = {"job_id": job_id, "location": location, "topic": topic}

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type='application/json',
            headers={'X-Appengine-Inbound-Appid': 'test-project'})
        assert response.status_code == HTTPStatus.OK

        mock_cloud_task_manager.return_value.create_dataflow_monitor_task. \
            assert_called_with(job_id,
                               location,
                               topic)
        mock_pubsub_helper.publish_message_to_topic.assert_not_called()

    @mock.patch('recidiviz.utils.metadata.project_id')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.pubsub_helper')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.CalculateCloudTaskManager')
    @mock.patch(f'{MONITOR_MANAGER_PACKAGE_NAME}.get_dataflow_job_with_id')
    def test_handle_dataflow_monitor_task_failed(self, mock_get_job,
                                                 mock_cloud_task_manager,
                                                 mock_pubsub_helper,
                                                 mock_project_id):
        """Tests that no message is published and no new monitor tasks are
        created when the job has failed."""
        mock_get_job.return_value = {
            'currentState': 'JOB_STATE_FAILED'
        }

        project_id = 'test-project'
        mock_project_id.return_value = project_id
        job_id = "12345"
        location = "fake_location"
        topic = 'fake_topic'
        route = '/monitor'
        data = {"job_id": job_id, "location": location, "topic": topic}

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type='application/json',
            headers={'X-Appengine-Inbound-Appid': 'test-project'})
        assert response.status_code == HTTPStatus.OK

        mock_cloud_task_manager.return_value.create_dataflow_monitor_task.\
            assert_not_called()
        mock_pubsub_helper.publish_message_to_topic.assert_not_called()
