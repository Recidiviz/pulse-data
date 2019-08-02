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

# pylint: disable=protected-access, line-too-long
import unittest
from unittest import mock

from google.cloud import bigquery

from recidiviz.calculator.bq import bqview
from recidiviz.calculator.bq.dashboard import dashboard_export_manager
from recidiviz.calculator.bq.dashboard.dashboard_export_manager import DataType


class DashboardExportManagerTest(unittest.TestCase):
    """Tests for dashboard_export_manager.py."""

    def setUp(self):
        self.mock_project_id = 'fake-recidiviz-project'
        self.mock_dataset_name = 'base_dataset'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_dataset_name)

        self.client_patcher = mock.patch(
            'recidiviz.calculator.bq.export_manager.bq_utils.client')
        self.mock_client = self.client_patcher.start().return_value

        self.mock_view = bqview.BigQueryView(
            view_id='test_view',
            view_query='SELECT NULL LIMIT 0'
        )

        views_to_export = [self.mock_view]
        dashboard_export_config_values = {
            'STATES_TO_EXPORT': ['US_CA'],
            'STANDARD_VIEWS_TO_EXPORT': views_to_export,
            'DATAFLOW_VIEWS_TO_EXPORT': views_to_export
        }
        self.dashboard_export_config_patcher = mock.patch(
            'recidiviz.calculator.bq.dashboard.dashboard_export_manager.dashboard_export_config',
            **dashboard_export_config_values)
        self.mock_export_config = self.dashboard_export_config_patcher.start()

    def tearDown(self):
        self.client_patcher.stop()

    def test_export_dashboard_data_to_cloud_storage_standard(self):
        """Tests that both _export_views_to_table and
        _export_view_tables_to_cloud_storage are executed.
        """
        dashboard_export_manager.export_dashboard_data_to_cloud_storage(
            bucket='bucket', data_type=DataType.STANDARD.value
        )
        self.mock_client.query.assert_called()
        self.mock_client.extract_table.assert_called()

    def test_export_dashboard_data_to_cloud_storage_dataflow(self):
        """Tests that both _export_views_to_table and
        _export_view_tables_to_cloud_storage are executed.
        """
        dashboard_export_manager.export_dashboard_data_to_cloud_storage(
            bucket='bucket', data_type=DataType.DATAFLOW.value
        )
        self.mock_client.query.assert_called()
        self.mock_client.extract_table.assert_called()

    def test_export_dashboard_data_to_cloud_storage_invalid_data_type(self):
        """Tests that an error is logged and neither function is executed
        when the requested data_type is invalid.
        """
        with self.assertLogs(level='ERROR'):
            dashboard_export_manager.export_dashboard_data_to_cloud_storage(
                bucket='bucket', data_type='xxx'
            )
        self.mock_client.query.assert_not_called()
        self.mock_client.extract_table.assert_not_called()

    def test_export_views_to_table(self):
        """Tests that the views are queried."""
        dashboard_export_manager._export_views_to_tables(
            self.mock_dataset,
            self.mock_export_config.STANDARD_VIEWS_TO_EXPORT)
        self.mock_client.query.assert_called()

    def test_export_view_tables_to_cloud_storage(self):
        """Tests that the tables are extracted."""
        dashboard_export_manager._export_view_tables_to_cloud_storage(
            self.mock_dataset, self.mock_export_config.STANDARD_VIEWS_TO_EXPORT,
            'bucket')
        self.mock_client.extract_table.assert_called()
