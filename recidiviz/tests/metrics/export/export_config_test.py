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
"""Tests the classes in the metric_export_config file."""
import unittest
from unittest import mock

from google.cloud import bigquery

from recidiviz.big_query.view_update_manager import BigQueryViewNamespace
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.metrics.export.export_config import ExportViewCollectionConfig, ExportMetricBigQueryViewConfig
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder


class TestExportViewCollectionConfig(unittest.TestCase):
    """Tests the functionality of the ExportViewCollectionConfig class."""

    def setUp(self):
        self.mock_project_id = 'fake-recidiviz-project'
        self.mock_dataset_id = 'base_dataset'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_dataset_id)

        self.metadata_patcher = mock.patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.mock_big_query_view_namespace = BigQueryViewNamespace.STATE

        self.mock_view_builder = MetricBigQueryViewBuilder(
            dataset_id=self.mock_dataset.dataset_id,
            view_id='test_view',
            view_query_template='SELECT NULL LIMIT 0',
            dimensions=[])

        self.views_for_dataset = [self.mock_view_builder]

    def tearDown(self):
        self.metadata_patcher.stop()

    def test_matches_filter(self):
        """Tests matches_filter function to ensure that state codes and export names correctly match"""
        state_dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket",
            state_code_filter='US_XX',
            export_name='EXPORT',
            bq_view_namespace=self.mock_big_query_view_namespace
        )
        self.assertTrue(state_dataset_export_config.matches_filter('US_XX'))

        dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket",
            state_code_filter=None,
            export_name='VALID_EXPORT_NAME',
            bq_view_namespace=self.mock_big_query_view_namespace
        )
        self.assertTrue(dataset_export_config.matches_filter('VALID_EXPORT_NAME'))
        self.assertFalse(dataset_export_config.matches_filter('INVALID_EXPORT_NAME'))

    def test_matches_filter_case_insensitive(self):
        """Tests matches_filter function with different cases to ensure state codes and export names correctly match"""
        state_dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket",
            state_code_filter='US_XX',
            export_name='OTHER_EXPORT',
            bq_view_namespace=self.mock_big_query_view_namespace
        )
        self.assertTrue(state_dataset_export_config.matches_filter('US_xx'))

        dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket",
            state_code_filter=None,
            export_name='VALID_EXPORT_NAME',
            bq_view_namespace=self.mock_big_query_view_namespace
        )
        self.assertTrue(dataset_export_config.matches_filter('valid_export_name'))

    def test_metric_export_state_agnostic(self):
        """Tests the export_configs_for_views_to_export function on the ExportViewCollectionConfig class when the
        export is state-agnostic."""
        state_agnostic_dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket-without-state-codes",
            state_code_filter=None,
            export_name='ALL_STATE_TEST_PRODUCT',
            bq_view_namespace=self.mock_big_query_view_namespace
        )

        view_configs_to_export = state_agnostic_dataset_export_config.export_configs_for_views_to_export(
            project_id=self.mock_project_id
        )

        expected_view = self.mock_view_builder.build()

        expected_view_export_configs = [ExportMetricBigQueryViewConfig(
            view=expected_view,
            view_filter_clause=None,
            intermediate_table_name=f"{expected_view.view_id}_table",
            output_directory=GcsfsDirectoryPath.from_absolute_path(
                state_agnostic_dataset_export_config.output_directory_uri_template.format(
                    project_id=self.mock_project_id,
                )
            )
        )]

        self.assertEqual(expected_view_export_configs, view_configs_to_export)

    def test_metric_export_state_specific(self):
        """Tests the export_configs_for_views_to_export function on the ExportViewCollectionConfig class when the
        export is state-specific."""
        specific_state_dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket",
            state_code_filter='US_XX',
            export_name='TEST_REPORT',
            bq_view_namespace=self.mock_big_query_view_namespace
        )

        view_configs_to_export = specific_state_dataset_export_config.export_configs_for_views_to_export(
            project_id=self.mock_project_id
        )

        expected_view = self.mock_view_builder.build()

        expected_view_export_configs = [ExportMetricBigQueryViewConfig(
            view=expected_view,
            view_filter_clause=" WHERE state_code = 'US_XX'",
            intermediate_table_name=f"{expected_view.view_id}_table_US_XX",
            output_directory=GcsfsDirectoryPath.from_absolute_path(
                f"gs://{self.mock_project_id}-bucket/US_XX"
            )
        )]

        self.assertEqual(expected_view_export_configs, view_configs_to_export)

    def test_metric_export_lantern_dashboard(self):
        """Tests the export_configs_for_views_to_export function on the ExportViewCollectionConfig class when the
            export is state-agnostic."""
        lantern_dashboard_dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket-without-state-codes",
            state_code_filter=None,
            export_name="TEST_EXPORT",
            bq_view_namespace=self.mock_big_query_view_namespace
        )

        view_configs_to_export = lantern_dashboard_dataset_export_config.export_configs_for_views_to_export(
            project_id=self.mock_project_id
        )

        expected_view = self.mock_view_builder.build()

        expected_view_export_configs = [ExportMetricBigQueryViewConfig(
            view=expected_view,
            view_filter_clause=None,
            intermediate_table_name=f"{expected_view.view_id}_table",
            output_directory=GcsfsDirectoryPath.from_absolute_path(
                lantern_dashboard_dataset_export_config.output_directory_uri_template.format(
                    project_id=self.mock_project_id,
                )
            )
        )]

        self.assertEqual(expected_view_export_configs, view_configs_to_export)

    def test_metric_export_lantern_dashboard_with_state(self):
        """Tests the export_configs_for_views_to_export function on the ExportViewCollectionConfig class when the
            export is state-specific."""
        lantern_dashboard_with_state_dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket",
            state_code_filter="US_XX",
            export_name="TEST_EXPORT",
            bq_view_namespace=self.mock_big_query_view_namespace
        )

        view_configs_to_export = lantern_dashboard_with_state_dataset_export_config.export_configs_for_views_to_export(
            project_id=self.mock_project_id
        )

        expected_view = self.mock_view_builder.build()

        expected_view_export_configs = [ExportMetricBigQueryViewConfig(
            view=expected_view,
            view_filter_clause=" WHERE state_code = 'US_XX'",
            intermediate_table_name=f"{expected_view.view_id}_table_US_XX",
            output_directory=GcsfsDirectoryPath.from_absolute_path(
                f"gs://{self.mock_project_id}-bucket/US_XX"
            )
        )]

        self.assertEqual(expected_view_export_configs, view_configs_to_export)
