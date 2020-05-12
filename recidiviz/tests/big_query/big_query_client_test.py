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

from recidiviz.big_query.big_query_client import BigQueryClientImpl, ExportViewConfig
from recidiviz.big_query.big_query_view import BigQueryView


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

        self.client_patcher = mock.patch(
            'recidiviz.big_query.big_query_client.client')
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

    def test_create_or_update_table_from_view(self):
        """create_or_update_table_from_view queries a view and loads the result
        into a table."""
        self.assertIsNotNone(self.bq_client.export_view_to_table_async(
            view_dataset_ref=self.mock_dataset,
            view=self.mock_view,
            output_table_dataset_ref=self.mock_dataset,
            output_table_id='output_table',
            view_filter_clause="WHERE state_code = 'US_CA'"))
        self.mock_client.query.assert_called()

    def test_create_or_update_table_from_view_no_view(self):
        """create_or_update_table_from_view does not run a query if the source
        view does not exist."""
        self.mock_client.get_table.side_effect = exceptions.NotFound('!')
        with self.assertLogs(level='WARNING'):
            self.assertIsNone(self.bq_client.export_view_to_table_async(
                view_dataset_ref=self.mock_dataset,
                view=self.mock_view,
                output_table_dataset_ref=self.mock_dataset,
                output_table_id='output_table',
                view_filter_clause="WHERE state_code = 'US_ND'"))
            self.mock_client.query.assert_not_called()

    def test_export_to_cloud_storage(self):
        """export_to_cloud_storage extracts the table corresponding to the
        view."""
        self.assertIsNotNone(self.bq_client.export_table_to_cloud_storage_async(
            source_table_dataset_ref=self.mock_dataset,
            source_table_id='source-table',
            destination_uri=f'gs://{self.mock_project_id}-bucket/destination_path.json'))
        self.mock_client.extract_table.assert_called()

    def test_export_to_cloud_storage_no_table(self):
        """export_to_cloud_storage does not extract from a table if the table
        does not exist."""
        self.mock_client.get_table.side_effect = exceptions.NotFound('!')
        with self.assertLogs(level='WARNING'):
            self.assertIsNone(self.bq_client.export_table_to_cloud_storage_async(
                source_table_dataset_ref=self.mock_dataset,
                source_table_id='source-table',
                destination_uri=f'gs://{self.mock_project_id}-bucket/destination_path.json'))
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

    def test_export_views_to_cloud_storage(self):
        """export_views_to_cloud_storage creates the table from the view and
        extracts the table"""
        bucket = self.mock_project_id + '-bucket'
        query_job = futures.Future()
        query_job.set_result([])
        extract_job = futures.Future()
        extract_job.set_result(None)
        self.mock_client.query.return_value = query_job
        self.mock_client.extract_table.return_value = extract_job
        self.bq_client.export_views_to_cloud_storage(
            self.mock_dataset, [ExportViewConfig(
                view=self.mock_view,
                view_filter_clause='WHERE x = y',
                intermediate_table_name=self.mock_table_id,
                output_uri=f'gs://{bucket}/view.json')])
        self.mock_client.query.assert_called()
        self.mock_client.extract_table.assert_called()

    def test_create_table_from_query(self):
        """Tests that the create_table_from_query function calls the function to create a table from a query."""
        self.bq_client.create_table_from_query_async(self.mock_dataset_id, self.mock_table_id,
                                                     query="SELECT * FROM some.fake.table")
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

    def test_insert_rows_into_table(self):
        self.mock_client.insert_rows.return_value = None

        self.bq_client.insert_rows_into_table(self.mock_dataset_id, self.mock_table_id, [{'col1': 1, 'col2': 'a'}])

        self.mock_client.get_table.assert_called()
        self.mock_client.insert_rows.assert_called()

    def test_insert_rows_into_table_with_errors(self):
        self.mock_client.insert_rows.return_value = [{'error1': 'An error'}]

        with self.assertRaises(ValueError):
            self.bq_client.insert_rows_into_table(self.mock_dataset_id, self.mock_table_id, [{'col1': 1, 'col2': 'a'}])

        self.mock_client.get_table.assert_called()
        self.mock_client.insert_rows.assert_called()

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
