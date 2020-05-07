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

"""Tests for export_manager.py."""

import unittest
from unittest import mock

from google.cloud import bigquery

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state import dashboard_export_manager


class DashboardExportManagerTest(unittest.TestCase):
    """Tests for dashboard_export_manager.py."""

    def setUp(self):
        project_id = 'fake-recidiviz-project'
        self.mock_dataset_name = 'base_dataset'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            project_id, self.mock_dataset_name)

        self.metadata_patcher = mock.patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = project_id

        self.client_patcher = mock.patch(
            'recidiviz.calculator.query.state.dashboard_export_manager.BigQueryClientImpl')
        self.mock_client = self.client_patcher.start().return_value

        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset

        self.mock_view = BigQueryView(dataset_id=self.mock_dataset.dataset_id,
                                      view_id='test_view',
                                      view_query_template='SELECT NULL LIMIT 0')

        self.views_to_export = [self.mock_view]
        dashboard_export_config_values = {
            'STATES_TO_EXPORT': ['US_CA'],
            'VIEWS_TO_EXPORT': self.views_to_export,
        }
        self.dashboard_export_config_patcher = mock.patch(
            'recidiviz.calculator.query.state.dashboard_export_manager.dashboard_export_config',
            **dashboard_export_config_values)
        self.mock_export_config = self.dashboard_export_config_patcher.start()

        self.views_to_update = {self.mock_dataset_name: self.views_to_export}

    def tearDown(self):
        self.client_patcher.stop()
        self.dashboard_export_config_patcher.stop()
        self.metadata_patcher.stop()

    @mock.patch('recidiviz.big_query.view_manager.create_dataset_and_update_views')
    @mock.patch('recidiviz.calculator.query.state.dashboard_export_manager.dataset_config')
    @mock.patch('recidiviz.calculator.query.state.dashboard_export_manager.view_config')
    def test_export_dashboard_data_to_cloud_storage(self, mock_view_config, mock_dataset_config, mock_view_manager):
        """Tests the table is created from the view and then extracted."""
        view_config_values = {
            'VIEWS_TO_UPDATE': self.views_to_update
        }

        dashboard_export_manager.export_dashboard_data_to_cloud_storage(bucket='bucket')

        mock_dataset_config.DASHBOARD_VIEWS_DATASET.return_value = 'dataset'
        mock_view_config.VIEWS_TO_UPDATE.return_value = view_config_values

        mock_view_manager.assert_called_with(mock_view_config.VIEWS_TO_UPDATE)

        self.mock_client.export_views_to_cloud_storage.assert_called()
