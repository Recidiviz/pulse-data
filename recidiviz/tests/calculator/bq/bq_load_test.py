# Recidiviz - a platform for tracking granular recidivism metrics in real time
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

from recidiviz.calculator.bq import bq_load


class BqLoadTest(unittest.TestCase):
    """Tests for bq_load.py."""


    def setUp(self):
        self.mock_project_id = 'fake-recidiviz-project'
        self.mock_dataset_id = 'fake-dataset'
        self.mock_table_id = 'test_table'
        self.mock_table_schema = [
            {'name': 'my_column', 'type': 'STRING', 'mode': 'NULLABLE'}]
        self.mock_export_uri = 'gs://fake-export-uri'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_dataset_id)
        self.mock_table = self.mock_dataset.table(self.mock_table_id)

        self.mock_load_job_patcher = mock.patch(
            'google.cloud.bigquery.job.LoadJob')
        self.mock_load_job = self.mock_load_job_patcher.start()

        Table = collections.namedtuple('Table', ['name'])
        export_config_values = {
            'gcs_export_uri.return_value': self.mock_export_uri,
            'TABLE_EXPORT_SCHEMA': {self.mock_table_id: self.mock_table_schema},
            'TABLES_TO_EXPORT': [Table('first_table'), Table('second_table')]
        }
        self.export_config_patcher = mock.patch(
            'recidiviz.calculator.bq.bq_load.export_config',
            **export_config_values)
        self.mock_export_config = self.export_config_patcher.start()

        self.bq_utils_patcher = mock.patch(
            'recidiviz.calculator.bq.bq_load.bq_utils')
        self.mock_bq_utils = self.bq_utils_patcher.start()


    def tearDown(self):
        self.mock_load_job_patcher.stop()
        self.export_config_patcher.stop()
        self.bq_utils_patcher.stop()


    def test_start_table_load_creates_dataset(self):
        """Test that start_table_load tries to create a parent dataset."""
        bq_load.start_table_load(self.mock_dataset, self.mock_table_id)
        self.mock_bq_utils.create_dataset_if_necessary.assert_called_with(
            self.mock_dataset)


    def test_start_table_load_fails_if_missing_table(self):
        """Test that start_table_load fails if its table is not defined."""
        with self.assertLogs(level='ERROR'):
            bq_load.start_table_load(self.mock_dataset, 'nonsense_table')


    def test_start_table_load_table_load_called(self):
        """Test that start_table_load calls load_table_from_uri."""
        bq_load.start_table_load(self.mock_dataset, self.mock_table_id)

        mock_client = self.mock_bq_utils.client.return_value
        mock_client.load_table_from_uri.assert_called_with(
            self.mock_export_uri,
            self.mock_dataset.table(self.mock_table_id),
            job_config=mock.ANY
        )


    def test_wait_for_table_load_calls_result(self):
        """Test that wait_for_table_load calls load_job.result()"""
        bq_load.wait_for_table_load(self.mock_load_job, self.mock_table)
        self.mock_load_job.result.assert_called()


    def test_wait_for_table_load_fail(self):
        """Test wait_for_table_load logs and exits if there is an error."""
        self.mock_load_job.result.side_effect = exceptions.NotFound('!')
        with self.assertLogs(level='ERROR'):
            bq_load.wait_for_table_load(self.mock_load_job, self.mock_table)


    @mock.patch('recidiviz.calculator.bq.bq_load.wait_for_table_load')
    @mock.patch('recidiviz.calculator.bq.bq_load.start_table_load')
    def test_start_table_load_and_wait(self, mock_start, mock_wait):
        """Test that start_table_load and wait_for_table_load are called."""
        mock_start.return_value = (self.mock_load_job, self.mock_table)

        bq_load.start_table_load_and_wait(self.mock_dataset, self.mock_table_id)

        mock_start.assert_called()
        mock_wait.assert_called()


    @mock.patch('recidiviz.calculator.bq.bq_load.wait_for_table_load')
    @mock.patch('recidiviz.calculator.bq.bq_load.start_table_load')
    def test_start_table_load_and_wait_not_called(self, mock_start, mock_wait):
        """Test that start_table_load_and_wait doesn't call wait_for_table_load.
        Should be the case if start_table_load fails."""
        mock_start.return_value = None

        bq_load.start_table_load_and_wait(self.mock_dataset, self.mock_table_id)

        mock_start.assert_called()
        mock_wait.assert_not_called()


    @mock.patch('recidiviz.calculator.bq.bq_load.wait_for_table_load')
    @mock.patch('recidiviz.calculator.bq.bq_load.start_table_load')
    def test_load_all_tables_concurrently(self, mock_start, mock_wait):
        """Test that start_table_load THEN wait_for_table load are called."""
        start_load_jobs = [
            (self.mock_load_job, self.mock_dataset.table(table.name))
            for table in self.mock_export_config.TABLES_TO_EXPORT
        ]
        mock_start.side_effect = start_load_jobs

        mock_parent = mock.Mock()
        mock_parent.attach_mock(mock_start, 'start')
        mock_parent.attach_mock(mock_wait, 'wait')

        bq_load.load_all_tables_concurrently(
            self.mock_dataset, self.mock_export_config.TABLES_TO_EXPORT)

        start_calls = [
            mock.call.start(self.mock_dataset, table.name)
            for table in self.mock_export_config.TABLES_TO_EXPORT
        ]
        wait_calls = [
            mock.call.wait(
                self.mock_load_job, self.mock_dataset.table(table.name))
            for table in self.mock_export_config.TABLES_TO_EXPORT
        ]
        mock_parent.assert_has_calls(start_calls + wait_calls)
