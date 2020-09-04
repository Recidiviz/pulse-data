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

from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsDirectoryPath
from recidiviz.metrics.export.metric_export_config import ExportMetricDatasetConfig, ExportMetricBigQueryViewConfig
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder


class TestExportMetricDatasetConfig(unittest.TestCase):
    """Tests the functionality of the ExportMetricDatasetConfig class."""
    def setUp(self):
        self.mock_project_id = 'fake-recidiviz-project'
        self.mock_dataset_id = 'base_dataset'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_dataset_id)

        self.metadata_patcher = mock.patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.mock_view_builder = MetricBigQueryViewBuilder(
            dataset_id=self.mock_dataset.dataset_id,
            view_id='test_view',
            view_query_template='SELECT NULL LIMIT 0',
            dimensions=[])

        self.views_for_dataset = [self.mock_view_builder]

    def tearDown(self):
        self.metadata_patcher.stop()

    def test_metric_export_state_agnostic(self):
        """Tests the export_configs_for_views_to_export function on the ExportMetricDatasetConfig class when the
        export is state-agnostic."""
        state_agnostic_dataset_export_config = ExportMetricDatasetConfig(
            dataset_id='dataset_id',
            metric_view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket-without-state-codes",
            state_code_filter=None
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
        """Tests the export_configs_for_views_to_export function on the ExportMetricDatasetConfig class when the
        export is state-specific."""
        state_agnostic_dataset_export_config = ExportMetricDatasetConfig(
            dataset_id='dataset_id',
            metric_view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket",
            state_code_filter='US_XX'
        )

        view_configs_to_export = state_agnostic_dataset_export_config.export_configs_for_views_to_export(
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
