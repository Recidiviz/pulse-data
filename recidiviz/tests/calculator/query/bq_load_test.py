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

        self.mock_load_job_patcher = mock.patch(
            'google.cloud.bigquery.job.LoadJob')
        self.mock_load_job = self.mock_load_job_patcher.start()
        self.mock_load_job.destination.return_value = self.mock_table

        Table = collections.namedtuple('Table', ['name'])
        self.tables_to_export = [Table('first_table'), Table('second_table')]
        self.mock_export_config = mock.Mock()
        self.mock_export_config.get_tables_to_export.return_value = self.tables_to_export

        self.mock_bq_client = create_autospec(BigQueryClient)

    def tearDown(self) -> None:
        self.mock_load_job_patcher.stop()
        self.project_id_patcher.stop()

    def test_start_table_load_table_load_called(self) -> None:
        """Test that start_table_load calls load_table_from_cloud_storage_async."""
        self.mock_export_config.get_dataset_ref.return_value = self.mock_dataset
        self.mock_export_config.get_gcs_export_uri_for_table.return_value = self.mock_export_uri
        self.mock_export_config.get_bq_schema_for_table.return_value = self.mock_table_schema

        bq_load.start_table_load(self.mock_bq_client, self.mock_table_id, self.mock_export_config)

        self.mock_bq_client.load_table_from_cloud_storage_async.assert_called()
        self.mock_bq_client.load_table_from_cloud_storage_async.assert_called_with(
            destination_dataset_ref=self.mock_dataset,
            destination_table_id=self.mock_table_id,
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


    @mock.patch('recidiviz.calculator.query.bq_load.wait_for_table_load')
    @mock.patch('recidiviz.calculator.query.bq_load.start_table_load')
    def test_start_table_load_and_wait(
            self, mock_start: mock.MagicMock, mock_wait: mock.MagicMock) -> None:
        """Test that start_table_load and wait_for_table_load are called."""
        mock_start.return_value = (self.mock_load_job, self.mock_table)
        mock_wait.return_value = True

        success = bq_load.start_table_load_and_wait(
            self.mock_bq_client, self.mock_table_id, self.mock_export_config)

        mock_start.assert_called()
        mock_wait.assert_called()
        self.assertTrue(success)


    @mock.patch('recidiviz.calculator.query.bq_load.wait_for_table_load')
    @mock.patch('recidiviz.calculator.query.bq_load.start_table_load')
    def test_start_table_load_and_wait_not_called(
            self, mock_start: mock.MagicMock, mock_wait: mock.MagicMock) -> None:
        """Test that start_table_load_and_wait doesn't call wait_for_table_load.
        Should be the case if start_table_load fails."""
        mock_start.return_value = None

        success = bq_load.start_table_load_and_wait(
            self.mock_bq_client, self.mock_table_id, self.mock_export_config)

        mock_start.assert_called()
        mock_wait.assert_not_called()
        self.assertFalse(success)


    @mock.patch('recidiviz.calculator.query.bq_load.wait_for_table_load')
    @mock.patch('recidiviz.calculator.query.bq_load.start_table_load')
    def test_load_all_tables_concurrently(
            self, mock_start: mock.MagicMock, mock_wait: mock.MagicMock) -> None:
        """Test that start_table_load THEN wait_for_table load are called."""
        start_load_jobs = [
            self.mock_load_job
            for _table in self.tables_to_export
        ]
        mock_start.side_effect = start_load_jobs

        mock_parent = mock.Mock()
        mock_parent.attach_mock(mock_start, 'start')
        mock_parent.attach_mock(mock_wait, 'wait')

        bq_load.load_all_tables_concurrently(
            self.mock_bq_client, self.mock_export_config)

        start_calls = [
            mock.call.start(self.mock_bq_client, table.name, self.mock_export_config)
            for table in self.tables_to_export
        ]
        wait_calls = [
            mock.call.wait(self.mock_bq_client, self.mock_load_job)
            for _table in self.tables_to_export
        ]
        mock_parent.assert_has_calls(start_calls + wait_calls)
