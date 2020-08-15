# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for BigQueryClientImpl"""
from concurrent import futures
import unittest
from unittest import mock

import pytest
from google.cloud import bigquery, exceptions
from google.cloud.bigquery import SchemaField
from mock import call

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.export.export_query_config import ExportQueryConfig


class BigQueryClientImplTest(unittest.TestCase):
    """Tests for BigQueryClientImpl"""

    def setUp(self):
        self.mock_project_id = 'fake-recidiviz-project'
        self.mock_dataset_id = 'fake-dataset'
        self.mock_table_id = 'test_table'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_dataset_id)
        self.mock_table = self.mock_dataset.table(self.mock_table_id)

        self.metadata_patcher = mock.patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.client_patcher = mock.patch('recidiviz.big_query.big_query_client.client')
        self.mock_client = self.client_patcher.start().return_value

        self.mock_view = BigQueryView(
            dataset_id='dataset',
            view_id='test_view',
            view_query_template='SELECT NULL LIMIT 0',
            materialized_view_table_id='test_view_table'
        )

        self.bq_client = BigQueryClientImpl()

    def tearDown(self):
        self.client_patcher.stop()
        self.metadata_patcher.stop()

    def test_create_dataset_if_necessary(self):
        """Check that a dataset is created if it does not exist."""
        self.mock_client.get_dataset.side_effect = exceptions.NotFound('!')
        self.bq_client.create_dataset_if_necessary(self.mock_dataset)
        self.mock_client.create_dataset.assert_called()

    def test_create_dataset_if_necessary_dataset_exists(self):
        """Check that a dataset is not created if it already exists."""
        self.mock_client.get_dataset.side_effect = None
        self.bq_client.create_dataset_if_necessary(self.mock_dataset)
        self.mock_client.create_dataset.assert_not_called()

    def test_table_exists(self):
        """Check that table_exists returns True if the table exists."""
        self.mock_client.get_table.side_effect = None
        self.assertTrue(
            self.bq_client.table_exists(self.mock_dataset, self.mock_table_id))

    def test_table_exists_does_not_exist(self):
        """Check that table_exists returns False if the table does not exist."""
        self.mock_client.get_table.side_effect = exceptions.NotFound('!')
        with self.assertLogs(level='WARNING'):
            table_exists = self.bq_client.table_exists(
                self.mock_dataset, self.mock_table_id)
            self.assertFalse(table_exists)

    def test_create_or_update_view_creates_view(self):
        """create_or_update_view creates a View if it does not exist."""
        self.mock_client.get_table.side_effect = exceptions.NotFound('!')
        self.bq_client.create_or_update_view(self.mock_dataset, self.mock_view)
        self.mock_client.create_table.assert_called()
        self.mock_client.update_table.assert_not_called()

    def test_create_or_update_view_updates_view(self):
        """create_or_update_view updates a View if it already exist."""
        self.mock_client.get_table.side_effect = None
        self.bq_client.create_or_update_view(self.mock_dataset, self.mock_view)
        self.mock_client.update_table.assert_called()
        self.mock_client.create_table.assert_not_called()

    def test_export_to_cloud_storage(self):
        """export_to_cloud_storage extracts the table corresponding to the
        view."""
        self.assertIsNotNone(self.bq_client.export_table_to_cloud_storage_async(
            source_table_dataset_ref=self.mock_dataset,
            source_table_id='source-table',
            destination_uri=f'gs://{self.mock_project_id}-bucket/destination_path.json',
            destination_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
        ))
        self.mock_client.extract_table.assert_called()

    def test_export_to_cloud_storage_no_table(self):
        """export_to_cloud_storage does not extract from a table if the table
        does not exist."""
        self.mock_client.get_table.side_effect = exceptions.NotFound('!')
        with self.assertLogs(level='WARNING'):
            self.assertIsNone(self.bq_client.export_table_to_cloud_storage_async(
                source_table_dataset_ref=self.mock_dataset,
                source_table_id='source-table',
                destination_uri=f'gs://{self.mock_project_id}-bucket/destination_path.json',
                destination_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON))
            self.mock_client.extract_table.assert_not_called()

    def test_load_table_async_create_dataset(self):
        """Test that load_table_from_cloud_storage_async tries to create a parent dataset."""

        self.mock_client.get_dataset.side_effect = exceptions.NotFound('!')

        self.bq_client.load_table_from_cloud_storage_async(
            destination_dataset_ref=self.mock_dataset,
            destination_table_id=self.mock_table_id,
            destination_table_schema=[SchemaField('my_column', 'STRING', 'NULLABLE', None, ())],
            source_uri='gs://bucket/export-uri')

        self.mock_client.create_dataset.assert_called()
        self.mock_client.load_table_from_uri.assert_called()

    def test_load_table_async_dataset_exists(self):
        """Test that load_table_from_cloud_storage_async does not try to create a parent dataset if it already exists.
        """

        self.bq_client.load_table_from_cloud_storage_async(
            destination_dataset_ref=self.mock_dataset,
            destination_table_id=self.mock_table_id,
            destination_table_schema=[SchemaField('my_column', 'STRING', 'NULLABLE', None, ())],
            source_uri='gs://bucket/export-uri')

        self.mock_client.create_dataset.assert_not_called()
        self.mock_client.load_table_from_uri.assert_called()

    def test_export_query_results_to_cloud_storage_no_table(self):
        bucket = self.mock_project_id + '-bucket'
        self.mock_client.get_table.side_effect = exceptions.NotFound('!')
        with self.assertLogs(level='WARNING'):
            self.bq_client.export_query_results_to_cloud_storage([
                ExportQueryConfig.from_view_query(
                    view=self.mock_view,
                    view_filter_clause='WHERE x = y',
                    intermediate_table_name=self.mock_table_id,
                    output_uri=f'gs://{bucket}/view.json',
                    output_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON)
            ])

    def test_export_query_results_to_cloud_storage(self):
        """export_query_results_to_cloud_storage creates the table from the view query and
        exports the table."""
        bucket = self.mock_project_id + '-bucket'
        query_job = futures.Future()
        query_job.set_result([])
        extract_job = futures.Future()
        extract_job.set_result(None)
        self.mock_client.query.return_value = query_job
        self.mock_client.extract_table.return_value = extract_job
        self.bq_client.export_query_results_to_cloud_storage([
            ExportQueryConfig.from_view_query(
                view=self.mock_view,
                view_filter_clause='WHERE x = y',
                intermediate_table_name=self.mock_table_id,
                output_uri=f'gs://{bucket}/view.json',
                output_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON)
            ])
        self.mock_client.query.assert_called()
        self.mock_client.extract_table.assert_called()
        self.mock_client.delete_table.assert_called_with(
            bigquery.DatasetReference(self.mock_project_id, self.mock_view.dataset_id).table(self.mock_table_id))

    def test_create_table_from_query(self):
        """Tests that the create_table_from_query function calls the function to create a table from a query."""
        self.bq_client.create_table_from_query_async(self.mock_dataset_id, self.mock_table_id,
                                                     query="SELECT * FROM some.fake.table",
                                                     query_parameters=[])
        self.mock_client.query.assert_called()

    def test_insert_into_table_from_table(self):
        """Tests that the insert_into_table_from_table function runs a query."""
        self.bq_client.insert_into_table_from_table_async('fake_source_dataset_id', 'fake_table_id',
                                                          self.mock_dataset_id, self.mock_table_id)
        self.mock_client.get_table.assert_called()
        self.mock_client.query.assert_called()

    def test_insert_into_table_from_table_invalid_destination(self):
        """Tests that the insert_into_table_from_table function does not run the query if the destination table does
        not exist."""
        self.mock_client.get_table.side_effect = exceptions.NotFound('!')

        with pytest.raises(ValueError):
            self.bq_client.insert_into_table_from_table_async(self.mock_dataset_id, self.mock_table_id,
                                                              'fake_source_dataset_id', 'fake_table_id')
        self.mock_client.get_table.assert_called()
        self.mock_client.query.assert_not_called()

    def test_insert_into_table_from_cloud_storage_async(self):
        self.mock_client.get_dataset.side_effect = exceptions.NotFound('!')

        self.bq_client.insert_into_table_from_cloud_storage_async(
            destination_dataset_ref=self.mock_dataset,
            destination_table_id=self.mock_table_id,
            destination_table_schema=[SchemaField('my_column', 'STRING', 'NULLABLE', None, ())],
            source_uri='gs://bucket/export-uri')

        self.mock_client.create_dataset.assert_called()
        self.mock_client.load_table_from_uri.assert_called()

    def test_delete_from_table(self):
        """Tests that the delete_from_table function runs a query."""
        self.bq_client.delete_from_table_async(self.mock_dataset_id, self.mock_table_id, filter_clause="WHERE x > y")
        self.mock_client.query.assert_called()

    def test_delete_from_table_invalid_filter_clause(self):
        """Tests that the delete_from_table function does not run a query when the filter clause is invalid."""
        with pytest.raises(ValueError):
            self.bq_client.delete_from_table_async(self.mock_dataset_id, self.mock_table_id, filter_clause="x > y")
        self.mock_client.query.assert_not_called()

    def test_materialize_view_to_table(self):
        """Tests that the materialize_view_to_table function calls the function to create a table from a query."""
        self.bq_client.materialize_view_to_table(self.mock_view)
        self.mock_client.query.assert_called()

    def test_materialize_view_to_table_no_materialized_view_table_id(self):
        """Tests that the materialize_view_to_table function does not call the function to create a table from a
        query if there is no set materialized_view_table_id on the view."""
        invalid_view = BigQueryView(
            dataset_id='dataset',
            view_id='test_view',
            view_query_template='SELECT NULL LIMIT 0',
            materialized_view_table_id=None
        )

        with pytest.raises(ValueError):
            self.bq_client.materialize_view_to_table(invalid_view)
        self.mock_client.query.assert_not_called()

    def test_create_table_with_schema(self):
        """Tests that the create_table_with_schema function calls the create_table function on the client."""
        self.mock_client.get_table.side_effect = exceptions.NotFound('!')
        schema_fields = [bigquery.SchemaField('new_schema_field', 'STRING')]

        self.bq_client.create_table_with_schema(self.mock_dataset_id, self.mock_table_id, schema_fields)
        self.mock_client.create_table.assert_called()

    def test_create_table_with_schema_table_exists(self):
        """Tests that the create_table_with_schema function raises an error when the table already exists."""
        self.mock_client.get_table.side_effect = None
        schema_fields = [bigquery.SchemaField('new_schema_field', 'STRING')]

        with pytest.raises(ValueError):
            self.bq_client.create_table_with_schema(self.mock_dataset_id, self.mock_table_id, schema_fields)
        self.mock_client.create_table.assert_not_called()

    def test_add_missing_fields_to_schema(self):
        """Tests that the add_missing_fields_to_schema function calls the client to update the table."""
        table_ref = bigquery.TableReference(self.mock_dataset, self.mock_table_id)
        schema_fields = [bigquery.SchemaField('fake_schema_field', 'STRING')]
        table = bigquery.Table(table_ref, schema_fields)
        self.mock_client.get_table.return_value = table

        new_schema_fields = [bigquery.SchemaField('new_schema_field', 'STRING')]

        self.bq_client.add_missing_fields_to_schema(self.mock_dataset_id, self.mock_table_id, new_schema_fields)

        self.mock_client.update_table.assert_called()

    def test_add_missing_fields_to_schema_no_missing_fields(self):
        """Tests that the add_missing_fields_to_schema function does not call the client to update the table when all
        of the fields are already present."""
        table_ref = bigquery.TableReference(self.mock_dataset, self.mock_table_id)
        schema_fields = [bigquery.SchemaField('fake_schema_field', 'STRING')]
        table = bigquery.Table(table_ref, schema_fields)
        self.mock_client.get_table.return_value = table

        new_schema_fields = [bigquery.SchemaField('fake_schema_field', 'STRING')]

        self.bq_client.add_missing_fields_to_schema(self.mock_dataset_id, self.mock_table_id, new_schema_fields)

        self.mock_client.update_table.assert_not_called()

    def test_add_missing_fields_to_schema_no_table(self):
        """Tests that the add_missing_fields_to_schema function does not call the client to update the table when the
        table does not exist."""
        self.mock_client.get_table.side_effect = exceptions.NotFound('!')
        new_schema_fields = [bigquery.SchemaField('fake_schema_field', 'STRING')]

        with pytest.raises(ValueError):
            self.bq_client.add_missing_fields_to_schema(self.mock_dataset_id, self.mock_table_id, new_schema_fields)

        self.mock_client.update_table.assert_not_called()

    def test_add_missing_fields_to_schema_fields_with_same_name_different_type(self):
        """Tests that the add_missing_fields_to_schema function raises an error when the user is trying to add a field
        with the same name but different field_type as an existing field."""
        table_ref = bigquery.TableReference(self.mock_dataset, self.mock_table_id)
        schema_fields = [bigquery.SchemaField('fake_schema_field', 'STRING')]
        table = bigquery.Table(table_ref, schema_fields)
        self.mock_client.get_table.return_value = table

        new_schema_fields = [bigquery.SchemaField('fake_schema_field', 'INTEGER')]

        with pytest.raises(ValueError):
            self.bq_client.add_missing_fields_to_schema(self.mock_dataset_id, self.mock_table_id, new_schema_fields)

        self.mock_client.update_table.assert_not_called()

    def test_add_missing_fields_to_schema_fields_with_same_name_different_mode(self):
        """Tests that the add_missing_fields_to_schema function raises an error when the user is trying to add a field
        with the same name but different mode as an existing field."""
        table_ref = bigquery.TableReference(self.mock_dataset, self.mock_table_id)
        schema_fields = [bigquery.SchemaField('fake_schema_field', 'STRING', mode="NULLABLE")]
        table = bigquery.Table(table_ref, schema_fields)
        self.mock_client.get_table.return_value = table

        new_schema_fields = [bigquery.SchemaField('fake_schema_field', 'STRING', mode="REQUIRED")]

        with pytest.raises(ValueError):
            self.bq_client.add_missing_fields_to_schema(self.mock_dataset_id, self.mock_table_id, new_schema_fields)

        self.mock_client.update_table.assert_not_called()

    def test_delete_table(self):
        """Tests that our delete table function calls the correct client method."""
        self.bq_client.delete_table(self.mock_dataset_id, self.mock_table_id)
        self.mock_client.delete_table.assert_called()

    @mock.patch('google.cloud.bigquery.QueryJob')
    def test_paged_read_single_page_single_row(self, mock_query_job):
        first_row = bigquery.table.Row(
            ['parole', 15, '10N'],
            {'supervision_type': 0, 'revocations': 1, 'district': 2},
        )

        # First call returns a single row, second call returns nothing
        mock_query_job.result.side_effect = [[first_row], []]

        processed_results = []

        def _process_fn(row: bigquery.table.Row) -> None:
            processed_results.append(dict(row))

        self.bq_client.paged_read_and_process(mock_query_job, 1, _process_fn)

        self.assertEqual([dict(first_row)], processed_results)
        mock_query_job.result.assert_has_calls([
            call(max_results=1, start_index=0),
            call(max_results=1, start_index=1),
        ])

    @mock.patch('google.cloud.bigquery.QueryJob')
    def test_paged_read_single_page_multiple_rows(self, mock_query_job):
        first_row = bigquery.table.Row(
            ['parole', 15, '10N'],
            {'supervision_type': 0, 'revocations': 1, 'district': 2},
        )
        second_row = bigquery.table.Row(
            ['probation', 7, '10N'],
            {'supervision_type': 0, 'revocations': 1, 'district': 2},
        )

        # First call returns a single row, second call returns nothing
        mock_query_job.result.side_effect = [[first_row, second_row], []]

        processed_results = []

        def _process_fn(row: bigquery.table.Row) -> None:
            processed_results.append(dict(row))

        self.bq_client.paged_read_and_process(mock_query_job, 10, _process_fn)

        self.assertEqual([dict(first_row), dict(second_row)], processed_results)
        mock_query_job.result.assert_has_calls([
            call(max_results=10, start_index=0),
            call(max_results=10, start_index=2),
        ])

    @mock.patch('google.cloud.bigquery.QueryJob')
    def test_paged_read_multiple_pages(self, mock_query_job):
        p1_r1 = bigquery.table.Row(
            ['parole', 15, '10N'],
            {'supervision_type': 0, 'revocations': 1, 'district': 2},
        )
        p1_r2 = bigquery.table.Row(
            ['probation', 7, '10N'],
            {'supervision_type': 0, 'revocations': 1, 'district': 2},
        )

        p2_r1 = bigquery.table.Row(
            ['parole', 8, '10F'],
            {'supervision_type': 0, 'revocations': 1, 'district': 2},
        )
        p2_r2 = bigquery.table.Row(
            ['probation', 3, '10F'],
            {'supervision_type': 0, 'revocations': 1, 'district': 2},
        )

        # First two calls returns results, third call returns nothing
        mock_query_job.result.side_effect = [
            [p1_r1, p1_r2],
            [p2_r1, p2_r2],
            []
        ]

        processed_results = []

        def _process_fn(row: bigquery.table.Row) -> None:
            processed_results.append(dict(row))

        self.bq_client.paged_read_and_process(mock_query_job, 2, _process_fn)

        self.assertEqual([dict(p1_r1), dict(p1_r2), dict(p2_r1), dict(p2_r2)], processed_results)
        mock_query_job.result.assert_has_calls([
            call(max_results=2, start_index=0),
            call(max_results=2, start_index=2),
            call(max_results=2, start_index=4),
        ])
