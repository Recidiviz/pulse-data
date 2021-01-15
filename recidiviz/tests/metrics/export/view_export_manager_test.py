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

"""Tests for view_export_manager.py."""

import unittest
from unittest import mock

from google.cloud import bigquery

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.export.big_query_view_exporter import ViewExportValidationError
from recidiviz.big_query.export.export_query_config import ExportBigQueryViewConfig, ExportOutputFormatType
from recidiviz.big_query.view_update_manager import BigQueryViewNamespace
from recidiviz.metrics.export.export_config import ExportViewCollectionConfig
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.metrics.export import view_export_manager
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class ViewCollectionExportManagerTest(unittest.TestCase):
    """Tests for view_export_manager.py."""

    def setUp(self):
        self.mock_state_code = 'US_XX'
        self.mock_project_id = 'fake-recidiviz-project'
        self.mock_dataset_id = 'base_dataset'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_dataset_id)

        self.metadata_patcher = mock.patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.client_patcher = mock.patch(
            'recidiviz.metrics.export.view_export_manager.BigQueryClientImpl')
        self.mock_client = self.client_patcher.start().return_value

        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset

        self.mock_view_builder = SimpleBigQueryViewBuilder(dataset_id=self.mock_dataset.dataset_id,
                                                           view_id='test_view',
                                                           view_query_template='SELECT NULL LIMIT 0')
        self.mock_metric_view_builder = MetricBigQueryViewBuilder(dataset_id=self.mock_dataset.dataset_id,
                                                                  view_id='test_view',
                                                                  view_query_template='SELECT NULL LIMIT 0',
                                                                  dimensions=[])

        self.view_buidlers_for_dataset = [self.mock_view_builder, self.mock_metric_view_builder]

        self.output_uri_template_for_dataset = {
            "dataset_id": "gs://{project_id}-dataset-location/subdirectory",
        }

        self.views_to_update = {self.mock_dataset_id: self.view_buidlers_for_dataset}

        self.mock_export_name = 'MOCK_EXPORT_NAME'
        self.mock_big_query_view_namespace = BigQueryViewNamespace.STATE

        self.metric_dataset_export_configs = [
            ExportViewCollectionConfig(
                view_builders_to_export=self.view_buidlers_for_dataset,
                output_directory_uri_template="gs://{project_id}-dataset-location/subdirectory",
                state_code_filter=self.mock_state_code,
                export_name=self.mock_export_name,
                bq_view_namespace=self.mock_big_query_view_namespace,
            )
        ]

        export_config_values = {
            'OUTPUT_DIRECTORY_URI_TEMPLATE_FOR_DATASET_EXPORT': self.output_uri_template_for_dataset,
            'VIEW_COLLECTION_EXPORT_CONFIGS': self.metric_dataset_export_configs,
        }

        self.export_config_patcher = mock.patch(
            'recidiviz.metrics.export.view_export_manager.export_config', **export_config_values)
        self.mock_export_config = self.export_config_patcher.start()

        self.gcs_factory_patcher = mock.patch(
            'recidiviz.metrics.export.view_export_manager.GcsfsFactory.build')
        self.gcs_factory_patcher.start().return_value = FakeGCSFileSystem()

    def tearDown(self):
        self.client_patcher.stop()
        self.export_config_patcher.stop()
        self.metadata_patcher.stop()
        self.gcs_factory_patcher.stop()

    @mock.patch('recidiviz.big_query.view_update_manager.create_dataset_and_update_views_for_view_builders')
    @mock.patch('recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter')
    def test_export_dashboard_data_to_cloud_storage(self, mock_view_exporter, mock_view_update_manager):
        """Tests the table is created from the view and then extracted."""
        view_export_manager.export_view_data_to_cloud_storage(self.mock_state_code, mock_view_exporter)

        view = self.mock_view_builder.build()
        metric_view = self.mock_metric_view_builder.build()

        view_export_configs = [
            ExportBigQueryViewConfig(
                view=view,
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{view.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    "gs://{project_id}-dataset-location/subdirectory/{state_code}".format(
                        project_id=self.mock_project_id,
                        state_code='US_XX',
                    )
                ),
                export_output_formats=[ExportOutputFormatType.JSON],
            ),
            ExportBigQueryViewConfig(
                view=metric_view,
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{view.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    "gs://{project_id}-dataset-location/subdirectory/{state_code}".format(
                        project_id=self.mock_project_id,
                        state_code='US_XX',
                    )
                ),
                export_output_formats=[ExportOutputFormatType.JSON, ExportOutputFormatType.METRIC],
            ),
        ]

        mock_view_update_manager.assert_called()
        mock_view_exporter.export_and_validate.assert_has_calls([
            mock.call([]),  # CSV export
            mock.call([view_export_configs[1].pointed_to_staging_subdirectory()]),  # JSON export
            mock.call([conf.pointed_to_staging_subdirectory() for conf in view_export_configs]),  # METRIC export
        ], any_order=True)

    @mock.patch('recidiviz.big_query.view_update_manager.create_dataset_and_update_views_for_view_builders')
    @mock.patch('recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter')
    def test_raise_exception_no_export_matched(self, mock_view_exporter, mock_view_update_manager):
        # pylint: disable=unused-argument
        """Tests the table is created from the view and then extracted."""
        self.mock_export_config.NAMESPACE_TO_UPDATE_FOR_EXPORT_FILTER = {
            'US_YY': 'NAMESPACE'
        }

        with self.assertRaises(ValueError) as e:
            view_export_manager.export_view_data_to_cloud_storage('US_YY', mock_view_exporter)
            self.assertEqual(str(e.exception), 'Export filter did not match any export configs:', ' US_YY')

    @mock.patch('recidiviz.big_query.view_update_manager.create_dataset_and_update_views_for_view_builders')
    @mock.patch('recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter')
    def test_export_dashboard_data_to_cloud_storage_state_agnostic(self,
                                                                   mock_view_exporter,
                                                                   mock_view_update_manager):
        """Tests the table is created from the view and then extracted, where the export is not state-specific."""
        state_agnostic_dataset_export_configs = [
            ExportViewCollectionConfig(
                view_builders_to_export=self.view_buidlers_for_dataset,
                output_directory_uri_template="gs://{project_id}-bucket-without-state-codes",
                state_code_filter=None,
                export_name=self.mock_export_name,
                bq_view_namespace=self.mock_big_query_view_namespace,
            ),
        ]

        self.mock_export_config.VIEW_COLLECTION_EXPORT_CONFIGS = state_agnostic_dataset_export_configs

        view_export_manager.export_view_data_to_cloud_storage(export_job_filter=self.mock_export_name,
                                                              override_view_exporter=mock_view_exporter)

        view = self.mock_view_builder.build()
        metric_view = self.mock_metric_view_builder.build()

        view_export_configs = [
            ExportBigQueryViewConfig(
                view=view,
                view_filter_clause=None,
                intermediate_table_name=f"{view.view_id}_table",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    "gs://{project_id}-bucket-without-state-codes".format(
                        project_id=self.mock_project_id,
                    )
                ),
                export_output_formats=[ExportOutputFormatType.JSON],
            ),
            ExportBigQueryViewConfig(
                view=metric_view,
                view_filter_clause=None,
                intermediate_table_name=f"{view.view_id}_table",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    "gs://{project_id}-bucket-without-state-codes".format(
                        project_id=self.mock_project_id,
                    )
                ),
                export_output_formats=[ExportOutputFormatType.JSON, ExportOutputFormatType.METRIC],
            ),
        ]

        mock_view_update_manager.assert_called()
        mock_view_exporter.export_and_validate.assert_has_calls([
            mock.call([]),  # CSV export
            mock.call([view_export_configs[1].pointed_to_staging_subdirectory()]),  # JSON export
            mock.call([conf.pointed_to_staging_subdirectory() for conf in view_export_configs]),  # METRIC export
        ], any_order=True)

    @mock.patch('recidiviz.big_query.view_update_manager.create_dataset_and_update_views_for_view_builders')
    @mock.patch('recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter')
    def test_export_dashboard_data_to_cloud_storage_value_error(self, mock_view_exporter, mock_view_update_manager):
        """Tests the table is created from the view and then extracted."""

        mock_view_exporter.export_and_validate.side_effect = ValueError
        with self.assertRaises(ValueError):
            view_export_manager.export_view_data_to_cloud_storage(self.mock_state_code, mock_view_exporter)

        # Just the metric export is attempted and then the raise stops subsequent checks from happening
        mock_view_update_manager.assert_called_once()

    @mock.patch('recidiviz.big_query.view_update_manager.create_dataset_and_update_views_for_view_builders')
    @mock.patch('recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter')
    def test_export_dashboard_data_to_cloud_storage_validation_error(self,
                                                                     mock_view_exporter,
                                                                     mock_view_update_manager):
        """Tests the table is created from the view and then extracted."""

        mock_view_exporter.export_and_validate.side_effect = ViewExportValidationError

        # Should not throw
        view_export_manager.export_view_data_to_cloud_storage(self.mock_state_code, mock_view_exporter)

        # Just the metric export is attempted and then the raise stops subsequent checks from happening
        mock_view_update_manager.assert_called_once()

    @mock.patch('recidiviz.big_query.view_update_manager.create_dataset_and_update_views_for_view_builders')
    @mock.patch('recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter')
    def test_export_dashboard_data_to_cloud_storage_no_materialized_view_update(
            self, mock_view_exporter, mock_view_update_manager):
        """Tests the materialized views are not updated when update_materialized_views is False."""

        view_export_manager.export_view_data_to_cloud_storage(self.mock_state_code,
                                                              mock_view_exporter,
                                                              update_materialized_views=False)

        mock_view_update_manager.assert_not_called()
        mock_view_exporter.export_and_validate.assert_called()

    @mock.patch('recidiviz.metrics.export.view_export_manager.view_update_manager.VIEW_BUILDERS_BY_NAMESPACE')
    @mock.patch('recidiviz.big_query.view_update_manager.create_dataset_and_update_views_for_view_builders')
    @mock.patch('recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter')
    def test_export_dashboard_data_to_cloud_storage_update_all_views(self,
                                                                     mock_view_exporter,
                                                                     mock_view_update_manager,
                                                                     mock_view_builders_by_namespace):
        """Tests that all views in the namespace are updated before the export when the export name is in
        export_config.NAMESPACES_REQUIRING_FULL_UPDATE."""
        self.mock_export_config.NAMESPACES_REQUIRING_FULL_UPDATE = [self.mock_big_query_view_namespace]

        mock_view_builders_by_namespace.return_value = {
            self.mock_big_query_view_namespace: self.view_buidlers_for_dataset
        }

        view_export_manager.export_view_data_to_cloud_storage(self.mock_state_code, mock_view_exporter)

        mock_view_update_manager.assert_called_with(
            self.mock_big_query_view_namespace,
            mock_view_builders_by_namespace[self.mock_big_query_view_namespace],
            materialized_views_only=False)

    @mock.patch('recidiviz.metrics.export.view_export_manager.view_update_manager.VIEW_BUILDERS_BY_NAMESPACE')
    @mock.patch('recidiviz.big_query.view_update_manager.create_dataset_and_update_views_for_view_builders')
    @mock.patch('recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter')
    def test_export_dashboard_data_to_cloud_storage_update_materialized_views_only(self,
                                                                                   mock_view_exporter,
                                                                                   mock_view_update_manager,
                                                                                   mock_view_builders_by_namespace):
        """Tests that only materialized views in the namespace are updated before the export when the export name is not
        in export_config.NAMESPACES_REQUIRING_FULL_UPDATE."""
        self.mock_export_config.NAMESPACES_REQUIRING_FULL_UPDATE = ['OTHER_NAMESPACE']

        mock_view_builders_by_namespace.return_value = {
            self.mock_big_query_view_namespace: self.view_buidlers_for_dataset
        }

        view_export_manager.export_view_data_to_cloud_storage(self.mock_state_code, mock_view_exporter)

        mock_view_update_manager.assert_called_with(
            self.mock_big_query_view_namespace,
            mock_view_builders_by_namespace[self.mock_big_query_view_namespace],
            materialized_views_only=True)
