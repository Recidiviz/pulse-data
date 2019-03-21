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

"""Tests for cloudsql_export.py."""

import collections
import unittest
from unittest import mock

import googleapiclient.errors

from recidiviz.calculator.bq import cloudsql_export


class CloudSqlExportTest(unittest.TestCase):
    """Tests for bq_load.py."""

    def setUp(self):
        self.mock_project_id = 'fake-recidiviz-project'
        self.mock_instance_id = 'test_instance_id'
        self.mock_database = 'test_database'
        self.mock_export_uri = 'gs://fake-export-uri'
        self.mock_table_id = 'test_table'
        self.mock_table_query = 'SELECT NULL LIMIT 0'
        self.mock_table_schema = [
            {'name': 'my_column', 'type': 'STRING', 'mode': 'NULLABLE'}]

        self.client_patcher = mock.patch(
            'recidiviz.calculator.bq.cloudsql_export.client')
        self.mock_client = self.client_patcher.start().return_value

        metadata_values = {
            'project_id.return_value': self.mock_project_id,
        }
        self.metadata_patcher = mock.patch(
            'recidiviz.calculator.bq.cloudsql_export.metadata',
            **metadata_values)
        self.metadata_patcher.start()

        secrets_values = {
            'sqlalchemy_db_name': self.mock_database,
            'cloudsql_instance_id': '{}:zone:{}'.format(
                self.mock_project_id, self.mock_instance_id)
        }
        secrets_config_values = {
            'get_secret.side_effect': secrets_values.get
        }
        self.secrets_patcher = mock.patch(
            'recidiviz.calculator.bq.cloudsql_export.secrets',
            **secrets_config_values)
        self.secrets_patcher.start()

        Table = collections.namedtuple('Table', ['name'])
        test_tables = [Table('first_table'), Table('second_table')]
        export_config_values = {
            'gcs_export_uri.return_value': self.mock_export_uri,
            'TABLE_EXPORT_QUERIES': {
                self.mock_table_id: self.mock_table_query,
                **{table.name: self.mock_table_query for table in test_tables}
            },
            'TABLES_TO_EXPORT': test_tables
        }
        self.export_config_patcher = mock.patch(
            'recidiviz.calculator.bq.cloudsql_export.export_config',
            **export_config_values)
        self.mock_export_config = self.export_config_patcher.start()


    def tearDown(self):
        self.client_patcher.stop()


    def test_cloudsql_instance_id(self):
        """Make sure cloudsql_instance_id is re-formatted correctly.

        Goes from project_id:zone:instance_id format in secrets to instance_id.
        """
        self.assertEqual(
            cloudsql_export.cloudsql_instance_id(), self.mock_instance_id)


    def test_cloudsql_db_name(self):
        """Check that the database name is retrieved from secrets."""
        self.assertEqual(cloudsql_export.cloudsql_db_name(), self.mock_database)


    def test_create_export_context(self):
        """Test that create_export_context returns the correct fields.

        Notably, 'databases' should be retrieved from secrets by
        cloudsql_export.cloudsql_db_name().
        """
        sample_export_context = {
            'exportContext': {
                'kind': 'sql#exportContext',
                'fileType': 'CSV',
                'uri': self.mock_export_uri,
                'databases': [self.mock_database],
                'csvExportOptions': {
                    'selectQuery': self.mock_table_query
                }
            }
        }

        result = cloudsql_export.create_export_context(
            self.mock_export_uri, self.mock_table_query)
        self.assertEqual(result, sample_export_context)


    @mock.patch('recidiviz.calculator.bq.cloudsql_export.time.sleep')
    def test_wait_until_operation_finished_waits(self, mock_time):
        """Test that wait_until_operation_finished waits until op. is done."""
        mock_time.side_effect = None
        mock_get_operation_calls = [
            {'status': 'PENDING'},
            {'status': 'RUNNING'},
            {'status': 'RUNNING'},
            {'status': 'DONE'}
        ]
        mock_get_op = self.mock_client.operations.return_value.get.return_value

        mock_get_op.execute.side_effect = mock_get_operation_calls
        cloudsql_export.wait_until_operation_finished('fake-op')

        sleep_calls = [
            mock.call(cloudsql_export.SECONDS_BETWEEN_OPERATION_STATUS_CHECKS)
        ] * (len(mock_get_operation_calls) - 1)
        mock_time.assert_has_calls(sleep_calls)


    @mock.patch('recidiviz.calculator.bq.cloudsql_export.time.sleep')
    def test_wait_until_operation_finished_surfaces_errors(self, mock_time):
        """Test that errors are logged from wait_until_operation_finished."""
        mock_time.side_effect = None
        mock_get_operation_calls = [
            {'status': 'PENDING'},
            {'status': 'RUNNING'},
            {'status': 'RUNNING'},
            {
                'status': 'DONE',
                'error': {
                    'errors': [{
                        'kind': 'error_kind',
                        'code': 'error_code',
                        'message': 'error_message'
                    }]
                }
            }
        ]
        mock_get_op = self.mock_client.operations.return_value.get.return_value

        mock_get_op.execute.side_effect = mock_get_operation_calls

        with self.assertLogs(level='ERROR'):
            cloudsql_export.wait_until_operation_finished('fake-op')


    @mock.patch(
        'recidiviz.calculator.bq.cloudsql_export.wait_until_operation_finished')
    def test_export_table(self, mock_wait):
        """Test that client().instances().export() is called and
            wait_until_operation_finished is called.
        """
        cloudsql_export.export_table(self.mock_table_id)

        mock_export = self.mock_client.instances.return_value.export
        mock_export.assert_called_with(
            project=self.mock_project_id,
            instance=self.mock_instance_id,
            body=mock.ANY
        )
        mock_wait.assert_called()


    @mock.patch(
        'recidiviz.calculator.bq.cloudsql_export.wait_until_operation_finished')
    def test_export_table_fail(self, mock_wait):
        """Test that export_table fails if its table is not defined."""
        with self.assertLogs(level='ERROR'):
            cloudsql_export.export_table('nonsense-table')

        mock_export = self.mock_client.instances.return_value.export
        mock_export.assert_not_called()
        mock_wait.assert_not_called()


    @mock.patch(
        'recidiviz.calculator.bq.cloudsql_export.wait_until_operation_finished')
    def test_export_table_api_fail(self, mock_wait):
        """Test that export_table fails if the export API request fails."""
        mock_export = self.mock_client.instances.return_value.export
        mock_export_request = mock_export.return_value
        mock_export_request.execute.side_effect = \
            googleapiclient.errors.HttpError('', content=b'')

        with self.assertLogs(level='ERROR'):
            success = cloudsql_export.export_table(self.mock_table_id)

        self.assertFalse(success)
        mock_wait.assert_not_called()


    @mock.patch('recidiviz.calculator.bq.cloudsql_export.export_table')
    def test_export_all_tables(self, mock_export):
        """Test that export_table is called for all tables specified."""
        cloudsql_export.export_all_tables(
            self.mock_export_config.TABLES_TO_EXPORT)

        mock_export.assert_has_calls(
            [mock.call(table.name)
             for table in self.mock_export_config.TABLES_TO_EXPORT]
        )
