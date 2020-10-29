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

"""Tests for bq_load.py."""

import unittest
from unittest import mock
import collections

from google.cloud import bigquery
from google.cloud import exceptions
from google.cloud.bigquery import SchemaField
from mock import create_autospec

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.calculator.query import bq_load


class BqLoadTest(unittest.TestCase):
    """Tests for bq_load.py."""

    def setUp(self) -> None:
        self.mock_project_id = 'fake-recidiviz-project'
        self.mock_dataset_id = 'fake-dataset'
        self.mock_table_id = 'test_table'
        self.mock_table_schema = [SchemaField('my_column', 'STRING', 'NULLABLE', None, ())]
        self.mock_export_uri = 'gs://fake-export-uri'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_dataset_id)
        self.mock_table = self.mock_dataset.table(self.mock_table_id)

        self.project_id_patcher = mock.patch('recidiviz.utils.metadata.project_id')
        self.project_id_patcher.start().return_value = self.mock_project_id

        self.mock_load_job_patcher = mock.patch('google.cloud.bigquery.job.LoadJob')
        self.mock_load_job = self.mock_load_job_patcher.start()
        self.mock_load_job.destination.return_value = self.mock_table

        Table = collections.namedtuple('Table', ['name'])
        self.tables_to_export = [Table('first_table'), Table('second_table')]
        self.mock_export_config = mock.Mock()
        self.mock_query_builder = mock.Mock()
        self.mock_export_config.get_tables_to_export.return_value = self.tables_to_export
        self.mock_export_config.get_stale_bq_rows_for_excluded_regions_query_builder.return_value = \
            self.mock_query_builder

        self.mock_bq_client = create_autospec(BigQueryClient)

    def tearDown(self) -> None:
        self.mock_load_job_patcher.stop()
        self.project_id_patcher.stop()

    @mock.patch('recidiviz.calculator.query.bq_load.delete_temp_table_if_exists')
    @mock.patch('recidiviz.calculator.query.bq_load.load_rows_excluded_from_refresh_into_temp_table_and_wait')
    @mock.patch('recidiviz.calculator.query.bq_load.load_table_from_gcs_and_wait')
    def test_refresh_bq_table_from_gcs_export_synchronous(self,
                                                          mock_load_gcs: mock.MagicMock,
                                                          mock_load_stale_data: mock.MagicMock,
                                                          mock_delete_temp_table: mock.MagicMock) -> None:
        """Test that refresh_bq_table_from_gcs_export_synchronous calls the following load jobs in order:
            1. load_table_from_gcs_and_wait
            2. load_rows_excluded_from_refresh_into_temp_table_and_wait
            3. big_query_client.load_table_from_table_async
            4. delete_temp_table_if_exists
        """
        temp_table_name = f'{self.mock_table_id}_temp'
        mock_parent = mock.Mock()
        mock_parent.attach_mock(mock_load_gcs, 'load_from_gcs')
        mock_parent.attach_mock(mock_load_stale_data, 'load_stale_data')
        mock_parent.attach_mock(mock_delete_temp_table, 'delete_temp_table')

        expected_calls = [mock.call.load_from_gcs(self.mock_bq_client,
                                                  self.mock_table_id,
                                                  self.mock_export_config,
                                                  destination_table_id=temp_table_name),
                          mock.call.load_stale_data(self.mock_bq_client,
                                                    self.mock_table_id,
                                                    self.mock_export_config,
                                                    destination_table_id=temp_table_name),
                          mock.call.delete_temp_table(self.mock_bq_client,
                                                      temp_table_name,
                                                      self.mock_export_config)]

        bq_load.refresh_bq_table_from_gcs_export_synchronous(self.mock_bq_client, self.mock_table_id,
                                                             self.mock_export_config)
        mock_parent.assert_has_calls(expected_calls)
        self.mock_bq_client.load_table_from_table_async.assert_called()

    @mock.patch('recidiviz.calculator.query.bq_load.delete_temp_table_if_exists')
    @mock.patch('recidiviz.calculator.query.bq_load.load_rows_excluded_from_refresh_into_temp_table_and_wait')
    @mock.patch('recidiviz.calculator.query.bq_load.load_table_from_gcs_and_wait')
    def test_refresh_bq_table_from_gcs_export_synchronous_load_from_gcs_fails(
            self,
            mock_load_gcs: mock.MagicMock,
            mock_load_stale_data: mock.MagicMock,
            mock_delete_temp_table: mock.MagicMock
    ) -> None:
        """Test that refresh_bq_table_from_gcs_export_synchronous raises an error and exits
            if load_from_gcs_and_wait fails
        """
        mock_load_gcs.side_effect = ValueError
        with self.assertRaises(ValueError):
            bq_load.refresh_bq_table_from_gcs_export_synchronous(self.mock_bq_client,
                                                                 self.mock_table_id,
                                                                 self.mock_export_config)
            mock_load_gcs.assert_called()
            mock_load_stale_data.assert_not_called()
            mock_delete_temp_table.assert_not_called()
            self.mock_bq_client.create_table_with_schema.assert_not_called()
            self.mock_bq_client.load_table_from_table_async.assert_not_called()

    @mock.patch('recidiviz.calculator.query.bq_load.load_rows_excluded_from_refresh_into_temp_table_and_wait')
    def test_refresh_from_or_create_table_table_doesnt_exist(self, mock_load_stale_data: mock.MagicMock) -> None:
        """Test if a destination table does not exist, it does not call
            load_rows_excluded_from_refresh_into_temp_table_and_wait and it creates the table with the schema.
        """
        with self.assertLogs(level='INFO'):
            self.mock_export_config.dataset_id = self.mock_dataset_id
            self.mock_export_config.get_bq_schema_for_table.return_value = self.mock_table_schema
            self.mock_bq_client.table_exists.return_value = False

            bq_load.refresh_bq_table_from_gcs_export_synchronous(self.mock_bq_client,
                                                                 'new_table',
                                                                 self.mock_export_config)

            self.mock_bq_client.create_table_with_schema.assert_called_with(dataset_id=self.mock_dataset_id,
                                                                            table_id='new_table',
                                                                            schema_fields=self.mock_table_schema)
            mock_load_stale_data.assert_not_called()
            self.mock_bq_client.load_table_from_table_async.assert_called()

    @mock.patch('recidiviz.calculator.query.bq_load.load_rows_excluded_from_refresh_into_temp_table_and_wait')
    def test_refresh_bq_table_from_gcs_export_synchronous_create_table_fails(
            self,
            mock_load_stale_data: mock.MagicMock
    ) -> None:
        """Test if a create table fails, it raises a ValueError."""
        with self.assertRaises(ValueError):
            self.mock_export_config.dataset_id = self.mock_dataset_id
            self.mock_export_config.get_bq_schema_for_table.return_value = self.mock_table_schema
            self.mock_bq_client.table_exists.return_value = False
            self.mock_bq_client.create_table_with_schema.return_value = None

            bq_load.refresh_bq_table_from_gcs_export_synchronous(self.mock_bq_client,
                                                                 'new_table',
                                                                 self.mock_export_config)
            mock_load_stale_data.assert_not_called()
            self.mock_bq_client.load_table_from_table_async.assert_not_called()

    @mock.patch('recidiviz.calculator.query.bq_load.delete_temp_table_if_exists')
    @mock.patch('recidiviz.calculator.query.bq_load.load_rows_excluded_from_refresh_into_temp_table_and_wait')
    @mock.patch('recidiviz.calculator.query.bq_load.load_table_from_gcs_and_wait')
    def test_refresh_bq_table_from_gcs_export_synchronous_load_stale_data_fails(self,
                                                                                mock_load_gcs: mock.MagicMock,
                                                                                mock_load_stale_data: mock.MagicMock,
                                                                                mock_delete_temp_table:
                                                                                mock.MagicMock) -> None:
        """Test that refresh_bq_table_from_gcs_export_synchronous does not continue when
            load_rows_excluded_from_refresh_into_temp_table_and_wait raises an error."""
        mock_load_stale_data.side_effect = ValueError
        with self.assertRaises(ValueError):
            bq_load.refresh_bq_table_from_gcs_export_synchronous(self.mock_bq_client,
                                                                 self.mock_table_id,
                                                                 self.mock_export_config)
            mock_load_gcs.assert_called()
            mock_load_stale_data.assert_called()
            mock_delete_temp_table.assert_not_called()
            self.mock_bq_client.create_table_with_schema.assert_not_called()
            self.mock_bq_client.load_table_from_table_async.assert_not_called()

    @mock.patch('recidiviz.calculator.query.bq_load.delete_temp_table_if_exists')
    @mock.patch('recidiviz.calculator.query.bq_load.wait_for_table_load')
    def test_refresh_bq_table_from_gcs_export_synchronous_load_table_async_fails(self,
                                                                                 mock_wait: mock.MagicMock,
                                                                                 mock_delete: mock.MagicMock) -> None:
        """Test that refresh_bq_table_from_gcs_export_synchronous raises a ValueError if wait_for_table_load fails
            and does not all delete_temp_table_if_exists
        """
        mock_wait.return_value = False
        with self.assertRaises(ValueError):
            bq_load.refresh_bq_table_from_gcs_export_synchronous(self.mock_bq_client,
                                                                 self.mock_table_id,
                                                                 self.mock_export_config)
            mock_delete.assert_not_called()


    def test_load_rows_excluded_from_refresh_into_temp_table_and_wait(self) -> None:
        """Test that load_rows_excluded_from_refresh_into_temp_table_and_wait calls
            insert_into_table_from_table_async with the correct arguments.
        """
        destination_table_id = 'fake_table_temp'
        self.mock_export_config.dataset_id = self.mock_dataset_id
        self.mock_query_builder.select_clause.return_value = 'fake select clause'
        self.mock_query_builder.join_clause.return_value = 'fake join clause'
        self.mock_query_builder.filter_clause.return_value = 'fake filter clause'

        bq_load.load_rows_excluded_from_refresh_into_temp_table_and_wait(self.mock_bq_client, self.mock_table_id,
                                                                         self.mock_export_config, destination_table_id)

        self.mock_bq_client.insert_into_table_from_table_async.assert_called_with(
            source_dataset_id=self.mock_dataset_id,
            source_table_id=self.mock_table_id,
            destination_dataset_id=self.mock_dataset_id,
            destination_table_id=destination_table_id,
            source_data_filter_clause=self.mock_query_builder.filter_clause(),
            hydrate_missing_columns_with_null=True,
            allow_field_additions=True)


    def test_load_table_from_gcs_and_wait(self) -> None:
        """Test that load_table_from_gcs_and_wait calls load_table_from_cloud_storage_async."""
        self.mock_export_config.get_dataset_ref.return_value = self.mock_dataset
        self.mock_export_config.get_gcs_export_uri_for_table.return_value = self.mock_export_uri
        self.mock_export_config.get_bq_schema_for_table.return_value = self.mock_table_schema
        destination_table_id = 'fake_table_temp'

        bq_load.load_table_from_gcs_and_wait(self.mock_bq_client, self.mock_table_id, self.mock_export_config,
                                             destination_table_id)

        self.mock_bq_client.load_table_from_cloud_storage_async.assert_called_with(
            destination_dataset_ref=self.mock_dataset,
            destination_table_id=destination_table_id,
            destination_table_schema=self.mock_table_schema,
            source_uri=self.mock_export_uri)


    def test_wait_for_table_load_calls_result(self) -> None:
        """Test that wait_for_table_load calls load_job.result()"""
        bq_load.wait_for_table_load(self.mock_bq_client, self.mock_load_job)
        self.mock_load_job.result.assert_called()


    def test_wait_for_table_load_fail(self) -> None:
        """Test wait_for_table_load logs and exits if there is an error."""
        self.mock_load_job.result.side_effect = exceptions.NotFound('!')
        with self.assertLogs(level='ERROR'):
            success = bq_load.wait_for_table_load(self.mock_bq_client, self.mock_load_job)
            self.assertFalse(success)


    def test_delete_temp_table_if_exists(self) -> None:
        """Given a table name, test that BigQueryClient calls delete_table with the temp table name."""
        with self.assertLogs(level='INFO'):
            temp_table_name = f'{self.mock_table_id}_temp'
            self.mock_export_config.dataset_id = self.mock_dataset_id
            bq_load.delete_temp_table_if_exists(self.mock_bq_client, temp_table_name,
                                                self.mock_export_config)
            self.mock_bq_client.delete_table.assert_called_with(dataset_id=self.mock_dataset_id,
                                                                table_id=temp_table_name)
