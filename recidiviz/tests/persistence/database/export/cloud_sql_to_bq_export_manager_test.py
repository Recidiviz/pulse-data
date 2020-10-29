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
"""Tests for cloud_sql_to_bq_export_manager.py."""

from http import HTTPStatus
import json
import unittest
from unittest import mock

import flask
from mock import Mock

from recidiviz.persistence.database.export import cloud_sql_to_bq_export_manager
from recidiviz.persistence.database.sqlalchemy_engine_manager import SchemaType
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import CloudTaskQueueInfo


class CloudSqlToBQExportManagerTest(unittest.TestCase):
    """Tests for cloud_sql_to_bq_export_manager.py."""

    def setUp(self) -> None:
        self.bq_load_patcher = mock.patch(
            'recidiviz.persistence.database.export.cloud_sql_to_bq_export_manager.bq_load')
        self.mock_bq_load = self.bq_load_patcher.start()

        self.client_patcher = mock.patch(
            'recidiviz.persistence.database.export.cloud_sql_to_bq_export_manager.BigQueryClientImpl')
        self.mock_client = self.client_patcher.start().return_value

        self.cloudsql_export_patcher = mock.patch(
            'recidiviz.persistence.database.export.cloud_sql_to_bq_export_manager.cloudsql_export')
        self.mock_cloudsql_export = self.cloudsql_export_patcher.start()

        self.fake_table_name = 'first_table'

        self.export_config_patcher = mock.patch(
            'recidiviz.persistence.database.export.cloud_sql_to_bq_export_manager.CloudSqlToBQConfig')
        self.mock_export_config = self.export_config_patcher.start()

        self.mock_app = flask.Flask(__name__)
        self.mock_app.config['TESTING'] = True
        self.mock_app.register_blueprint(
            cloud_sql_to_bq_export_manager.export_manager_blueprint)
        self.mock_flask_client = self.mock_app.test_client()

    def tearDown(self) -> None:
        self.bq_load_patcher.stop()
        self.client_patcher.stop()
        self.cloudsql_export_patcher.stop()
        self.export_config_patcher.stop()

    def test_export_table_then_load_table_succeeds(self) -> None:
        """Test that export_table_then_load_table passes the client, table, and config
            to bq_load.refresh_bq_table_from_gcs_export_synchronous if the export succeeds.
        """

        cloud_sql_to_bq_export_manager.export_table_then_load_table(
            self.mock_client, self.fake_table_name, self.mock_export_config)

        self.mock_cloudsql_export.export_table.assert_called_with(
            self.fake_table_name, self.mock_export_config)

        self.mock_bq_load.refresh_bq_table_from_gcs_export_synchronous.assert_called_with(
            self.mock_client, self.fake_table_name, self.mock_export_config)

    def test_export_table_then_load_table_export_fails(self) -> None:
        """Test that export_table_then_load_table does not pass args to load the table
            if export fails and raises an error.
        """
        self.mock_cloudsql_export.export_table.return_value = False

        with self.assertRaises(ValueError):
            cloud_sql_to_bq_export_manager.export_table_then_load_table(
                self.mock_client, 'random-table', self.mock_export_config)

        self.mock_bq_load.assert_not_called()

    @mock.patch('recidiviz.utils.metadata.project_id', Mock(return_value='test-project'))
    @mock.patch('recidiviz.utils.metadata.project_number', Mock(return_value='123456789'))
    @mock.patch('recidiviz.persistence.database.export.cloud_sql_to_bq_export_manager.export_table_then_load_table')
    def test_handle_bq_export_task(self, mock_export: mock.MagicMock) -> None:
        """Tests that the export is called for a given table and module when
        the /export_manager/export endpoint is hit."""
        mock_export.return_value = True

        self.mock_export_config.for_schema_type.return_value = self.mock_export_config

        table = 'fake_table'
        module = SchemaType.JAILS.value
        route = '/export'
        data = {"table_name": table, "schema_type": module}

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type='application/json',
            headers={'X-Appengine-Inbound-Appid': 'test-project'})
        assert response.status_code == HTTPStatus.OK
        mock_export.assert_called_with(self.mock_client,
                                       table,
                                       self.mock_export_config)

    @mock.patch('recidiviz.utils.metadata.project_id', Mock(return_value='test-project'))
    @mock.patch('recidiviz.utils.metadata.project_number', Mock(return_value='123456789'))
    @mock.patch('recidiviz.persistence.database.export.cloud_sql_to_bq_export_manager.export_table_then_load_table')
    def test_handle_bq_export_task_invalid_module(self, mock_export: mock.MagicMock) -> None:
        """Tests that there is an error when the /export_manager/export
        endpoint is hit with an invalid module."""
        mock_export.return_value = True
        table = 'fake_table'
        module = 'INVALID'
        route = '/export'
        data = {"table_name": table, "schema_type": module}

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type='application/json',
            headers={'X-Appengine-Inbound-Appid': 'test-project'})
        assert response.status_code == HTTPStatus.BAD_REQUEST
        mock_export.assert_not_called()

    @mock.patch('recidiviz.utils.metadata.project_id', Mock(return_value='test-project'))
    @mock.patch('recidiviz.utils.metadata.project_number', Mock(return_value='123456789'))
    @mock.patch('recidiviz.persistence.database.export.cloud_sql_to_bq_export_manager.pubsub_helper')
    @mock.patch('recidiviz.persistence.database.export.cloud_sql_to_bq_export_manager.BQExportCloudTaskManager')
    def test_handle_bq_monitor_task_requeue(self,
                                            mock_task_manager: mock.MagicMock,
                                            mock_pubsub_helper: mock.MagicMock) -> None:
        """Test that a new bq monitor task is added to the queue when there are
        still unfinished tasks on the bq queue."""
        queue_path = 'test-queue-path'

        mock_task_manager.return_value.get_bq_queue_info.return_value = \
            CloudTaskQueueInfo(queue_name='queue_name',
                               task_names=[
                                   f'{queue_path}/table_name-123',
                                   f'{queue_path}/table_name-456',
                                   f'{queue_path}/table_name-789',
                               ])

        topic = 'fake_topic'
        message = 'fake_message'
        route = '/bq_monitor'
        data = {"topic": topic, "message": message}

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type='application/json',
            headers={'X-Appengine-Inbound-Appid': 'test-project'})
        assert response.status_code == HTTPStatus.OK
        mock_task_manager.return_value.\
            create_bq_monitor_task.assert_called_with(topic, message)
        mock_pubsub_helper.publish_message_to_topic.assert_not_called()

    @mock.patch('recidiviz.utils.metadata.project_id', Mock(return_value='test-project'))
    @mock.patch('recidiviz.utils.metadata.project_number', Mock(return_value='123456789'))
    @mock.patch('recidiviz.persistence.database.export.cloud_sql_to_bq_export_manager.pubsub_helper')
    @mock.patch('recidiviz.persistence.database.export.cloud_sql_to_bq_export_manager.BQExportCloudTaskManager')
    def test_handle_bq_monitor_task_publish(self,
                                            mock_task_manager: mock.MagicMock,
                                            mock_pubsub_helper: mock.MagicMock) -> None:
        """Tests that a message is published to the Pub/Sub topic when there
        are no tasks on the bq queue."""
        mock_task_manager.return_value.get_bq_queue_info.return_value = \
            CloudTaskQueueInfo(queue_name='queue_name', task_names=[])

        topic = 'fake_topic'
        message = 'fake_message'
        route = '/bq_monitor'
        data = {"topic": topic, "message": message}

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type='application/json',
            headers={
                'X-Appengine-Inbound-Appid': 'test-project'})
        assert response.status_code == HTTPStatus.OK
        mock_task_manager.return_value.create_bq_monitor_task.\
            assert_not_called()
        mock_pubsub_helper.publish_message_to_topic.assert_called_with(
            message=message, topic=topic)
