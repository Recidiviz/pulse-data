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

from recidiviz.big_query.export.big_query_view_exporter import ViewExportValidationError
from recidiviz.metrics.export.metric_export_config import ExportMetricBigQueryViewConfig, ExportMetricDatasetConfig
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.metrics.export import metric_view_export_manager

mock_state_code = 'US_XX'


class MetricViewExportManagerTest(unittest.TestCase):
    """Tests for metric_view_export_manager.py."""

    def setUp(self):
        self.mock_project_id = 'fake-recidiviz-project'
        self.mock_dataset_id = 'base_dataset'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_dataset_id)

        self.metadata_patcher = mock.patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.client_patcher = mock.patch(
            'recidiviz.metrics.export.metric_view_export_manager.BigQueryClientImpl')
        self.mock_client = self.client_patcher.start().return_value

        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset

        self.mock_view_builder = MetricBigQueryViewBuilder(dataset_id=self.mock_dataset.dataset_id,
                                                           view_id='test_view',
                                                           view_query_template='SELECT NULL LIMIT 0',
                                                           dimensions=[])

        self.views_for_dataset = [self.mock_view_builder]

        self.output_uri_template_for_dataset = {
            "dataset_id": "gs://{project_id}-dataset-location/subdirectory",
        }

        self.views_to_update = {self.mock_dataset_id: self.views_for_dataset}

        self.metric_dataset_export_configs = [
            ExportMetricDatasetConfig(
                dataset_id=self.mock_dataset_id,
                metric_view_builders_to_export=self.views_for_dataset,
                output_directory_uri_template="gs://{project_id}-dataset-location/subdirectory",
                state_code_filter=mock_state_code,
                export_name=None
            )
        ]

        view_config_values = {
            'OUTPUT_DIRECTORY_URI_TEMPLATE_FOR_DATASET_EXPORT': self.output_uri_template_for_dataset,
            'VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE': self.views_to_update,
            'METRIC_DATASET_EXPORT_CONFIGS': self.metric_dataset_export_configs
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
        metric_view_export_manager.export_view_data_to_cloud_storage(mock_state_code, mock_view_exporter)

        view = self.mock_view_builder.build()

        view_export_configs = [ExportMetricBigQueryViewConfig(
            view=view,
            view_filter_clause=" WHERE state_code = 'US_XX'",
            intermediate_table_name=f"{view.view_id}_table_US_XX",
            output_directory=GcsfsDirectoryPath.from_absolute_path(
                "gs://{project_id}-dataset-location/subdirectory/{state_code}".format(
                    project_id=self.mock_project_id,
                    state_code='US_XX',
                )
            )
        )]

        mock_view_update_manager.assert_called()
        mock_view_exporter.export_and_validate.assert_called_with(view_export_configs)

    @mock.patch('recidiviz.big_query.view_update_manager.create_dataset_and_update_views_for_view_builders')
    @mock.patch('recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter')
    def test_raise_exception_no_export_matched(self, mock_view_exporter, mock_view_update_manager):
        # pylint: disable=unused-argument
        """Tests the table is created from the view and then extracted."""
        with self.assertRaises(ValueError) as e:
            metric_view_export_manager.export_view_data_to_cloud_storage('US_YY', mock_view_exporter)
            self.assertEqual(str(e.exception),  'Export filter did not match any export configs:', ' US_YY')

    @mock.patch('recidiviz.metrics.export.metric_view_export_manager.view_config')
    @mock.patch('recidiviz.big_query.view_update_manager.create_dataset_and_update_views_for_view_builders')
    @mock.patch('recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter')
    def test_export_dashboard_data_to_cloud_storage_state_agnostic(self,
                                                                   mock_view_exporter,
                                                                   mock_view_update_manager,
                                                                   mock_view_config):
        """Tests the table is created from the view and then extracted, where the export is not state-specific."""

        state_agnostic_dataset_export_configs = [
            ExportMetricDatasetConfig(
                dataset_id='dataset_id',
                metric_view_builders_to_export=[self.mock_view_builder],
                output_directory_uri_template="gs://{project_id}-bucket-without-state-codes",
                state_code_filter=None,
                export_name=None
            ),
        ]

        mock_view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE = self.views_to_update
        mock_view_config.METRIC_DATASET_EXPORT_CONFIGS = state_agnostic_dataset_export_configs

        metric_view_export_manager.export_view_data_to_cloud_storage(export_job_filter=None,
                                                                     view_exporter=mock_view_exporter)

        view = self.mock_view_builder.build()

        view_export_configs = [ExportMetricBigQueryViewConfig(
            view=view,
            view_filter_clause=None,
            intermediate_table_name=f"{view.view_id}_table",
            output_directory=GcsfsDirectoryPath.from_absolute_path(
                "gs://{project_id}-bucket-without-state-codes".format(
                    project_id=self.mock_project_id,
                )
            )
        )]

        mock_view_update_manager.assert_called()
        mock_view_exporter.export_and_validate.assert_called_with(view_export_configs)


    @mock.patch('recidiviz.big_query.view_update_manager.create_dataset_and_update_views_for_view_builders')
    @mock.patch('recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter')
    def test_export_dashboard_data_to_cloud_storage_value_error(self, mock_view_exporter, mock_view_update_manager):
        """Tests the table is created from the view and then extracted."""

        mock_view_exporter.export_and_validate.side_effect = ValueError
        with self.assertRaises(ValueError):
            metric_view_export_manager.export_view_data_to_cloud_storage(mock_state_code, mock_view_exporter)

        view = self.mock_view_builder.build()

        view_export_configs = [ExportMetricBigQueryViewConfig(
            view=view,
            view_filter_clause=" WHERE state_code = 'US_XX'",
            intermediate_table_name=f"{view.view_id}_table_US_XX",
            output_directory=GcsfsDirectoryPath.from_absolute_path(
                "gs://{project_id}-dataset-location/subdirectory/{state_code}".format(
                    project_id=self.mock_project_id,
                    state_code='US_XX',
                )
            )
        )]

        mock_view_update_manager.assert_called()
        mock_view_exporter.export_and_validate.assert_called_with(view_export_configs)

    @mock.patch('recidiviz.big_query.view_update_manager.create_dataset_and_update_views_for_view_builders')
    @mock.patch('recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter')
    def test_export_dashboard_data_to_cloud_storage_validation_error(self,
                                                                     mock_view_exporter,
                                                                     mock_view_update_manager):
        """Tests the table is created from the view and then extracted."""

        mock_view_exporter.export_and_validate.side_effect = ViewExportValidationError

        # Should not throw
        metric_view_export_manager.export_view_data_to_cloud_storage(mock_state_code, mock_view_exporter)

        view = self.mock_view_builder.build()

        view_export_configs = [ExportMetricBigQueryViewConfig(
            view=view,
            view_filter_clause=" WHERE state_code = 'US_XX'",
            intermediate_table_name=f"{view.view_id}_table_US_XX",
            output_directory=GcsfsDirectoryPath.from_absolute_path(
                "gs://{project_id}-dataset-location/subdirectory/{state_code}".format(
                    project_id=self.mock_project_id,
                    state_code='US_XX',
                )
            )
        )]

        mock_view_update_manager.assert_called()
        mock_view_exporter.export_and_validate.assert_called_with(view_export_configs)
