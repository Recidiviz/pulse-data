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

"""Tests for metric_view_export_manager.py."""

import unittest
from unittest import mock

from google.cloud import bigquery

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.metrics.export import metric_view_export_manager


class MetricViewExportManagerTest(unittest.TestCase):
    """Tests for metric_view_export_manager.py."""

    def setUp(self):
        project_id = 'fake-recidiviz-project'
        self.mock_dataset_name = 'base_dataset'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            project_id, self.mock_dataset_name)

        self.metadata_patcher = mock.patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = project_id

        self.client_patcher = mock.patch(
            'recidiviz.metrics.export.metric_view_export_manager.BigQueryClientImpl')
        self.mock_client = self.client_patcher.start().return_value

        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset

        self.mock_view_builder = MetricBigQueryViewBuilder(dataset_id=self.mock_dataset.dataset_id,
                                                           view_id='test_view',
                                                           view_query_template='SELECT NULL LIMIT 0',
                                                           dimensions=[])

        self.views_for_dataset = [self.mock_view_builder]

        self.views_to_export = {
            "dataset_id": {
                'US_XX': self.views_for_dataset,
            },
        }

        self.output_uri_template_for_dataset = {
            "dataset_id": "gs://{project_id}-dataset-location/subdirectory/{state_code}",
        }

        self.views_to_update = {self.mock_dataset_name: self.views_for_dataset}

        view_config_values = {
            'STATES_TO_EXPORT': ['US_CA'],
            'DATASETS_STATES_AND_VIEW_BUILDERS_TO_EXPORT': self.views_to_export,
            'OUTPUT_DIRECTORY_TEMPLATE_FOR_DATASET_EXPORT': self.output_uri_template_for_dataset,
            'VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE': self.views_to_update
        }
        self.view_export_config_patcher = mock.patch(
            'recidiviz.metrics.export.metric_view_export_manager.view_config',
            **view_config_values)
        self.mock_export_config = self.view_export_config_patcher.start()

    def tearDown(self):
        self.client_patcher.stop()
        self.view_export_config_patcher.stop()
        self.metadata_patcher.stop()

    @mock.patch('recidiviz.big_query.view_update_manager.create_dataset_and_update_views_for_view_builders')
    @mock.patch('recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter')
    def test_export_dashboard_data_to_cloud_storage(self, mock_view_exporter, mock_view_update_manager):
        """Tests the table is created from the view and then extracted."""
        metric_view_export_manager.export_view_data_to_cloud_storage(mock_view_exporter)

        mock_view_update_manager.assert_called()
        mock_view_exporter.export.assert_called()
