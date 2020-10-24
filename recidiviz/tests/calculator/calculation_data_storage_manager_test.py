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
"""Tests the calculation_data_storage_manager."""
import unittest
from unittest import mock
from mock import patch
from flask import Flask

from google.cloud import bigquery

from recidiviz.calculator import calculation_data_storage_manager
from recidiviz.calculator.calculation_data_storage_manager import calculation_data_storage_manager_blueprint


class CalculationDataStorageManagerTest(unittest.TestCase):
    """Tests for calculation_data_storage_manager.py."""

    def setUp(self):

        self.project_id = 'fake-recidiviz-project'
        self.mock_view_dataset_name = 'my_views_dataset'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.project_id, self.mock_view_dataset_name)

        self.project_id_patcher = patch('recidiviz.utils.metadata.project_id')
        self.project_id_patcher.start().return_value = self.project_id
        self.project_number_patcher = patch('recidiviz.utils.metadata.project_number')
        self.project_number_patcher.start().return_value = '123456789'

        self.bq_client_patcher = mock.patch(
            'recidiviz.calculator.calculation_data_storage_manager.BigQueryClientImpl')
        self.mock_client = self.bq_client_patcher.start().return_value

        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset
        self.mock_dataflow_tables = [
            bigquery.TableReference(self.mock_dataset, 'fake_table_1'),
            bigquery.TableReference(self.mock_dataset, 'fake_table_2')
        ]
        self.mock_client.list_tables.return_value = self.mock_dataflow_tables
        self.mock_client.project_id.return_value = self.project_id

        self.data_storage_config_patcher = mock.patch('recidiviz.calculator.calculation_data_storage_config')
        self.mock_data_storage_config = self.data_storage_config_patcher.start()

        self.fake_dataflow_metrics_dataset = 'fake_dataflow_metrics_dataset'
        self.fake_cold_storage_dataset = 'fake_cold_storage_dataset'
        self.fake_max_jobs = 10

        self.mock_data_storage_config.MAX_DAYS_IN_DATAFLOW_METRICS_TABLE.return_value = self.fake_max_jobs
        self.mock_data_storage_config.DATAFLOW_METRICS_COLD_STORAGE_DATASET = self.fake_cold_storage_dataset

        app = Flask(__name__)
        app.register_blueprint(calculation_data_storage_manager_blueprint)
        app.config['TESTING'] = True
        self.client = app.test_client()

    def tearDown(self):
        self.bq_client_patcher.stop()
        self.project_id_patcher.stop()
        self.project_number_patcher.stop()
        self.data_storage_config_patcher.stop()

    def test_move_old_dataflow_metrics_to_cold_storage(self):
        """Test that move_old_dataflow_metrics_to_cold_storage gets the list of tables to prune, calls the client to
        insert into the cold storage table, and calls the client to delete from the dataflow table."""
        calculation_data_storage_manager.move_old_dataflow_metrics_to_cold_storage()

        self.mock_client.list_tables.assert_called()
        self.mock_client.insert_into_table_from_table_async.assert_called()
        self.mock_client.delete_from_table_async.assert_called()

    def test_update_dataflow_metric_tables_schemas(self):
        """Test that update_dataflow_metric_tables_schemas calls the client to update the schemas of the metric
        tables."""
        calculation_data_storage_manager.update_dataflow_metric_tables_schemas()

        self.mock_client.add_missing_fields_to_schema.assert_called()

    def test_update_dataflow_metric_tables_schemas_create_table(self):
        """Test that update_dataflow_metric_tables_schemas calls the client to create a new table when the table
        does not yet exist."""
        self.mock_client.table_exists.return_value = False

        calculation_data_storage_manager.update_dataflow_metric_tables_schemas()

        self.mock_client.create_table_with_schema.assert_called()

    @patch('recidiviz.calculator.calculation_data_storage_manager.move_old_dataflow_metrics_to_cold_storage')
    def test_prune_old_dataflow_data(self, mock_move_metrics):
        """Tests that the move_old_dataflow_metrics_to_cold_storage function is called when the /prune_old_dataflow_data
        endpoint is hit."""
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/prune_old_dataflow_data', headers=headers)

        self.assertEqual(200, response.status_code)
        mock_move_metrics.assert_called()
