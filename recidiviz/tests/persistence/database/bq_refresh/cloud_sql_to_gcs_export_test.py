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

"""Tests for cloud_sql_to_gcs_export.py."""

import collections
import unittest
from unittest import mock

import googleapiclient.errors

from recidiviz.persistence.database.bq_refresh import cloud_sql_to_gcs_export
from recidiviz.persistence.database.sqlalchemy_engine_manager import SchemaType

CLOUD_SQL_TO_GCS_EXPORT_PACKAGE_NAME = cloud_sql_to_gcs_export.__name__


class CloudSqlToGcsExportTest(unittest.TestCase):
    """Tests for cloud_sql_to_gcs_export.py."""

    def setUp(self) -> None:
        self.schema_type = SchemaType.JAILS
        self.mock_project_id = 'fake-recidiviz-project'
        self.mock_instance_id = 'test_instance_id'
        self.mock_database = 'test_database'
        self.mock_export_uri = 'gs://fake-export-uri'
        self.mock_table_id = 'test_table'
        self.mock_table_query = 'SELECT NULL LIMIT 0'

        self.client_patcher = mock.patch(
            f'{CLOUD_SQL_TO_GCS_EXPORT_PACKAGE_NAME}.sqladmin_client')
        self.mock_client = self.client_patcher.start().return_value

        self.metadata_patcher = mock.patch(f'{CLOUD_SQL_TO_GCS_EXPORT_PACKAGE_NAME}.metadata')
        self.mock_metadata = self.metadata_patcher.start()
        self.mock_metadata.project_id.return_value = self.mock_project_id

        secrets_values = {
            'sqlalchemy_db_name': self.mock_database,
            'sqlalchemy_cloudsql_instance_id': '{}:zone:{}'.format(
                self.mock_project_id, self.mock_instance_id)
        }

        self.secrets_patcher = mock.patch('recidiviz.persistence.database.sqlalchemy_engine_manager.secrets')
        self.mock_secrets = self.secrets_patcher.start()
        self.mock_secrets.get_secret.side_effect = secrets_values.get

        Table = collections.namedtuple('Table', ['name'])
        self.tables_to_export = [Table('first_table'),
                                 Table('second_table')]

        HttpErrorResponse = collections.namedtuple('Response', ['status', 'reason'])
        self.http_error_response = HttpErrorResponse(404, 'CloudSQL operation instance does not exist.')

        self.mock_get_operation = self.mock_client.operations.return_value.get.return_value
        self.mock_bq_refresh_config = mock.Mock()
        self.mock_bq_refresh_config.schema_type = self.schema_type
        self.mock_bq_refresh_config.get_tables_to_export.return_value = self.tables_to_export
        self.mock_bq_refresh_config.get_table_export_query.return_value = self.mock_table_query
        self.mock_bq_refresh_config.get_gcs_export_uri_for_table.return_value = self.mock_export_uri

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.metadata_patcher.stop()
        self.secrets_patcher.stop()

    def test_create_export_context(self) -> None:
        """Test that create_export_context returns the correct fields.

        Notably, 'databases' should be retrieved from secrets by
        cloud_sql_to_gcs_export.cloudsql_db_name().
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

        result = cloud_sql_to_gcs_export.create_export_context(
            self.schema_type, self.mock_export_uri, self.mock_table_query)
        self.assertEqual(result, sample_export_context)

    @mock.patch(f'{CLOUD_SQL_TO_GCS_EXPORT_PACKAGE_NAME}.time.sleep')
    def test_wait_until_operation_finishes_api_fails_with_retries(self, mock_time: mock.MagicMock) -> None:
        """Test that wait_until_operation_finished retries if the export API request fails."""
        mock_time.side_effect = None
        mock_get_operation_calls = [
            {'status': 'PENDING'},
            {'status': 'RUNNING'},
            {'status': 'RUNNING'},
            {'status': 'DONE'}
        ]

        self.mock_get_operation.execute.side_effect = [
            googleapiclient.errors.HttpError(self.http_error_response, content=b''),
            *mock_get_operation_calls
        ]

        # Sleeps 1 time for the HttpError + 3 times for the get_operation status calls
        sleep_calls = [mock.call(cloud_sql_to_gcs_export.SECONDS_BETWEEN_OPERATION_STATUS_CHECKS)] * 4

        cloud_sql_to_gcs_export.wait_until_operation_finished('fake-op')

        mock_time.assert_has_calls(sleep_calls)
        self.assertEqual(len(self.mock_get_operation.mock_calls), 5)

    @mock.patch(f'{CLOUD_SQL_TO_GCS_EXPORT_PACKAGE_NAME}.time.sleep')
    def test_wait_until_operation_finishes_api_fails_raises(self, mock_time: mock.MagicMock) -> None:
        """Test that wait_until_operation_finished retries only 3 times until it raises an error."""
        mock_time.side_effect = None
        self.mock_get_operation.execute.side_effect = \
            googleapiclient.errors.HttpError(self.http_error_response, content=b'')
        sleep_calls = [mock.call(cloud_sql_to_gcs_export.SECONDS_BETWEEN_OPERATION_STATUS_CHECKS)] * 3

        with self.assertRaises(ValueError), self.assertLogs(level='DEBUG'):
            cloud_sql_to_gcs_export.wait_until_operation_finished('fake-op')

        mock_time.assert_has_calls(sleep_calls)

    @mock.patch(f'{CLOUD_SQL_TO_GCS_EXPORT_PACKAGE_NAME}.time.sleep')
    def test_wait_until_operation_finished_waits(self, mock_time: mock.MagicMock) -> None:
        """Test that wait_until_operation_finished waits until op. is done."""
        mock_time.side_effect = None
        mock_get_operation_calls = [
            {'status': 'PENDING'},
            {'status': 'RUNNING'},
            {'status': 'RUNNING'},
            {'status': 'DONE'}
        ]

        self.mock_get_operation.execute.side_effect = mock_get_operation_calls
        cloud_sql_to_gcs_export.wait_until_operation_finished('fake-op')

        sleep_calls = [
            mock.call(cloud_sql_to_gcs_export.SECONDS_BETWEEN_OPERATION_STATUS_CHECKS)
        ] * (len(mock_get_operation_calls) - 1)
        mock_time.assert_has_calls(sleep_calls)

    @mock.patch(f'{CLOUD_SQL_TO_GCS_EXPORT_PACKAGE_NAME}.time.sleep')
    def test_wait_until_operation_finished_surfaces_errors(self, mock_time: mock.MagicMock) -> None:
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

        self.mock_get_operation.execute.side_effect = mock_get_operation_calls

        with self.assertLogs(level='ERROR'):
            cloud_sql_to_gcs_export.wait_until_operation_finished('fake-op')

    @mock.patch(f'{CLOUD_SQL_TO_GCS_EXPORT_PACKAGE_NAME}.wait_until_operation_finished')
    def test_export_table(self, mock_wait: mock.MagicMock) -> None:
        """Test that client().instances().export() is called and
            wait_until_operation_finished is called.
        """
        cloud_sql_to_gcs_export.export_table(self.mock_table_id, self.mock_bq_refresh_config)

        mock_export = self.mock_client.instances.return_value.export
        mock_export.assert_called_with(
            project=self.mock_project_id,
            instance=self.mock_instance_id,
            body=mock.ANY
        )
        mock_wait.assert_called()

    @mock.patch(f'{CLOUD_SQL_TO_GCS_EXPORT_PACKAGE_NAME}.wait_until_operation_finished')
    def test_export_table_create_export_context(self, mock_wait: mock.MagicMock) -> None:
        """Test that create_export_context is called with the schema_type, export_query, and export_uri.
        """
        with mock.patch(f'{CLOUD_SQL_TO_GCS_EXPORT_PACKAGE_NAME}.create_export_context') as mock_export_context:
            cloud_sql_to_gcs_export.export_table(self.mock_table_id, self.mock_bq_refresh_config)
            mock_export_context.assert_called_with(
                self.schema_type,
                self.mock_export_uri,
                self.mock_table_query
            )
            mock_wait.assert_called()

    @mock.patch(f'{CLOUD_SQL_TO_GCS_EXPORT_PACKAGE_NAME}.wait_until_operation_finished')
    def test_export_table_api_fail(self, mock_wait: mock.MagicMock) -> None:
        """Test that export_table fails if the export API request fails."""
        mock_export = self.mock_client.instances.return_value.export
        mock_export_request = mock_export.return_value
        mock_export_request.execute.side_effect = \
            googleapiclient.errors.HttpError('', content=b'')

        with self.assertLogs(level='ERROR'):
            success = cloud_sql_to_gcs_export.export_table(self.mock_table_id, self.mock_bq_refresh_config)

        self.assertFalse(success)
        mock_wait.assert_not_called()

    @mock.patch(f'{CLOUD_SQL_TO_GCS_EXPORT_PACKAGE_NAME}.export_table')
    def test_export_all_tables(self, mock_export: mock.MagicMock) -> None:
        """Test that export_table is called for all tables specified."""
        cloud_sql_to_gcs_export.export_all_tables(self.mock_bq_refresh_config)

        mock_export.assert_has_calls(
            [mock.call(table.name, self.mock_bq_refresh_config)
             for table in self.tables_to_export]
        )
