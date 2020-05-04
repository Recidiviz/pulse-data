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
"""Tests for export_manager.py."""

import collections
from http import HTTPStatus
from itertools import chain
import json
import unittest
from unittest import mock

import flask
from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference

from recidiviz.calculator.query import export_manager
from recidiviz.persistence.database.sqlalchemy_engine_manager import SchemaType
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import \
    CloudTaskQueueInfo


class ExportManagerTestCounty(unittest.TestCase):
    """Tests for export_manager.py."""

    def setUp(self):
        self.schema_type = SchemaType.JAILS

        self.mock_project_id = 'fake-recidiviz-project'
        self.mock_dataset_name = 'base_dataset'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_dataset_name)
        self.mock_table_id = 'test_table'
        self.mock_table_query = 'SELECT NULL LIMIT 0'

        self.bq_load_patcher = mock.patch(
            'recidiviz.calculator.query.export_manager.bq_load')
        self.mock_bq_load = self.bq_load_patcher.start()

        self.client_patcher = mock.patch('recidiviz.calculator.query.export_manager.BigQueryClientImpl')
        self.mock_client = self.client_patcher.start().return_value

        self.cloudsql_export_patcher = mock.patch(
            'recidiviz.calculator.query.export_manager.cloudsql_export')
        self.mock_cloudsql_export = self.cloudsql_export_patcher.start()

        Table = collections.namedtuple('Table', ['name'])
        test_tables = [Table('first_table'), Table('second_table')]
        export_config_values = {
            'COUNTY_BASE_TABLES_BQ_DATASET': self.mock_dataset_name,
            'COUNTY_TABLES_TO_EXPORT': test_tables,
            'COUNTY_TABLE_EXPORT_QUERIES': {
                self.mock_table_id: self.mock_table_query,
                **{table.name: self.mock_table_query for table in test_tables}
            }
        }
        self.export_config_patcher = mock.patch(
            'recidiviz.calculator.query.export_manager.export_config',
            **export_config_values)
        self.mock_export_config = self.export_config_patcher.start()

        self.mock_app = flask.Flask(__name__)
        self.mock_app.config['TESTING'] = True
        self.mock_app.register_blueprint(
            export_manager.export_manager_blueprint)
        self.mock_flask_client = self.mock_app.test_client()


    def tearDown(self):
        self.bq_load_patcher.stop()
        self.client_patcher.stop()
        self.cloudsql_export_patcher.stop()
        self.export_config_patcher.stop()

    def test_export_table_then_load_table_dataset(self):
        """Test that export_table_then_load_table uses a dataset if specified.
        """
        table = 'first_table'
        dataset = self.mock_client.dataset('random_dataset')

        export_manager.export_table_then_load_table(table,
                                                    dataset,
                                                    self.schema_type)

        self.mock_bq_load.start_table_load_and_wait.assert_called_with(
            dataset, table, self.schema_type)


    def test_export_table_then_load_table_doesnt_load(self):
        """Test that export_table_then_load_table doesn't load if export fails.
        """
        self.mock_cloudsql_export.export_table.return_value = False

        with self.assertLogs(level='ERROR'):
            export_manager.export_table_then_load_table('random-table',
                                                        self.mock_dataset,
                                                        self.schema_type)

        self.mock_bq_load.assert_not_called()


    def test_export_then_load_all_sequentially(self):
        """Test that tables are exported then loaded sequentially."""
        default_dataset = self.mock_client.dataset_ref_for_id(
            self.mock_export_config.COUNTY_BASE_TABLES_BQ_DATASET)

        # Suppose all exports succeed.
        self.mock_cloudsql_export.export_table.side_effect = (
            [True]*len(self.mock_export_config.COUNTY_TABLES_TO_EXPORT))

        mock_parent = mock.Mock()
        mock_parent.attach_mock(
            self.mock_cloudsql_export.export_table, 'export')
        mock_parent.attach_mock(
            self.mock_bq_load.start_table_load_and_wait, 'load')

        export_then_load_calls = list(chain.from_iterable([
            (mock.call.export(self.schema_type,
                              table.name,
                              self.mock_export_config.
                              COUNTY_TABLE_EXPORT_QUERIES[table.name]),
             mock.call.load(default_dataset, table.name, self.schema_type))
            for table in self.mock_export_config.COUNTY_TABLES_TO_EXPORT
        ]))

        export_manager.export_then_load_all_sequentially(self.schema_type)

        mock_parent.assert_has_calls(export_then_load_calls)


    def test_export_then_load_all_sequentially_fails_invalid_module(self):
        with self.assertLogs(level='ERROR'):
            export_manager.export_then_load_all_sequentially('nonsense')

    @mock.patch('recidiviz.utils.metadata.project_id')
    def test_export_all_then_load_all(self, mock_project_id):
        """Test that export_all_then_load_all exports all tables then loads all
            tables.
        """
        mock_project_id.return_value = 'test-project'

        default_dataset = self.mock_client.dataset_ref_for_id(
            self.mock_export_config.COUNTY_BASE_TABLES_BQ_DATASET)

        mock_parent = mock.Mock()
        mock_parent.attach_mock(
            self.mock_cloudsql_export.export_all_tables, 'export_all')
        mock_parent.attach_mock(
            self.mock_bq_load.load_all_tables_concurrently, 'load_all')

        export_all_then_load_all_calls = [
            mock.call.export_all(
                self.schema_type,
                self.mock_export_config.COUNTY_TABLES_TO_EXPORT,
                self.mock_export_config.COUNTY_TABLE_EXPORT_QUERIES),
            mock.call.load_all(
                default_dataset,
                self.mock_export_config.COUNTY_TABLES_TO_EXPORT,
                self.schema_type)
        ]

        export_manager.export_all_then_load_all(self.schema_type)

        mock_parent.assert_has_calls(export_all_then_load_all_calls)


    def test_export_all_then_load_all_fails_invalid_module(self):
        with self.assertLogs(level='ERROR'):
            export_manager.export_all_then_load_all('nonsense')

    @mock.patch('recidiviz.utils.metadata.project_id')
    @mock.patch('recidiviz.calculator.query.export_manager.export_table_then_load_table')
    def test_handle_bq_export_task_county(self, mock_export, mock_project_id):
        """Tests that the export is called for a given table and module when
        the /export_manager/export endpoint is hit for a table in the COUNTY
        module."""
        mock_export.return_value = True

        mock_project_id.return_value = 'test-project'
        self.mock_client.dataset_ref_for_id.return_value = DatasetReference(mock_project_id.return_value,
                                                                            self.mock_dataset_name)
        table = 'fake_table'
        module = 'JAILS'
        route = '/export'
        data = {"table_name": table, "schema_type": module}

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type='application/json',
            headers={'X-Appengine-Inbound-Appid': 'test-project'})
        assert response.status_code == HTTPStatus.OK
        mock_export.assert_called_with(table,
                                       DatasetReference.from_string(self.mock_dataset_name,
                                                                    mock_project_id.return_value),
                                       SchemaType.JAILS)

    @mock.patch('recidiviz.utils.metadata.project_id')
    @mock.patch('recidiviz.calculator.query.export_manager.export_table_then_load_table')
    def test_handle_bq_export_task_state(self, mock_export, mock_project_id):
        """Tests that the export is called for a given table and module when
        the /export_manager/export endpoint is hit for a table in the STATE
        module."""
        mock_export.return_value = True
        self.mock_export_config.STATE_BASE_TABLES_BQ_DATASET.return_value = 'state'

        mock_project_id.return_value = 'test-project'
        self.mock_client.dataset_ref_for_id.return_value = \
            DatasetReference.from_string('dataset', 'test-project')

        table = 'fake_table'
        module = 'STATE'
        route = '/export'
        data = {"table_name": table, "schema_type": module}

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type='application/json',
            headers={'X-Appengine-Inbound-Appid': 'test-project'})
        assert response.status_code == HTTPStatus.OK
        mock_export.assert_called_with(table, DatasetReference.from_string('dataset',
                                                                           'test-project'),
                                       SchemaType.STATE)

    @mock.patch('recidiviz.utils.metadata.project_id')
    @mock.patch('recidiviz.calculator.query.export_manager.export_table_then_load_table')
    def test_handle_bq_export_task_invalid_module(self, mock_export,
                                                  mock_project_id):
        """Tests that there is an error when the /export_manager/export
        endpoint is hit with an invalid module."""
        self.mock_client.dataset.return_value = 'dataset'
        mock_export.return_value = True

        mock_project_id.return_value = 'test-project'
        table = 'fake_table'
        module = 'INVALID'
        route = '/export'
        data = {"table_name": table, "schema_type": module}

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type='application/json',
            headers={'X-Appengine-Inbound-Appid': 'test-project'})
        assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
        mock_export.assert_not_called()

    @mock.patch('recidiviz.utils.metadata.project_id')
    @mock.patch('recidiviz.calculator.query.export_manager.pubsub_helper')
    @mock.patch(
        'recidiviz.calculator.query.export_manager.BQExportCloudTaskManager')
    def test_handle_bq_monitor_task_requeue(self,
                                            mock_task_manager,
                                            mock_pubsub_helper,
                                            mock_project_id):
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

        mock_project_id.return_value = 'test-project'
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

    @mock.patch('recidiviz.utils.metadata.project_id')
    @mock.patch('recidiviz.calculator.query.export_manager.pubsub_helper')
    @mock.patch(
        'recidiviz.calculator.query.export_manager.BQExportCloudTaskManager')
    def test_handle_bq_monitor_task_publish(self, mock_task_manager,
                                            mock_pubsub_helper,
                                            mock_project_id):
        """Tests that a message is published to the Pub/Sub topic when there
        are no tasks on the bq queue."""
        mock_task_manager.return_value.get_bq_queue_info.return_value = \
            CloudTaskQueueInfo(queue_name='queue_name', task_names=[])

        mock_project_id.return_value = 'test-project'
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
