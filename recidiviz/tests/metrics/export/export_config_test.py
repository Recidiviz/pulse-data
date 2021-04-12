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
import importlib
import unittest
from unittest import mock

from google.cloud import bigquery

from recidiviz.big_query.export.export_query_config import ExportOutputFormatType
from recidiviz.big_query.view_update_manager import BigQueryViewNamespace
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.common.constants import states
from recidiviz.metrics.export.export_config import (
    ExportBigQueryViewConfig,
    ExportViewCollectionConfig,
    _VIEW_COLLECTION_EXPORT_CONFIGS,
    VIEW_COLLECTION_EXPORT_INDEX,
    ProductConfig,
    ProductStateConfig,
    PRODUCTS_CONFIG_PATH,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.tests.ingest import fixtures


class TestProductConfig(unittest.TestCase):
    """Tests the functionality of the ProductConfig class."""

    def test_product_configs_from_file(self) -> None:
        product_configs = ProductConfig.product_configs_from_file(
            fixtures.as_filepath("fixture_products.yaml")
        )
        expected_product_configs = [
            ProductConfig(
                name="Test Product",
                exports=["EXPORT", "OTHER_EXPORT"],
                states=[
                    ProductStateConfig(state_code="US_XX", environment="production"),
                    ProductStateConfig(state_code="US_WW", environment="staging"),
                ],
                environment=None,
            ),
            ProductConfig(
                name="Test State Agnostic Product",
                exports=["MOCK_EXPORT_NAME"],
                states=[],
                environment="staging",
            ),
            ProductConfig(
                name="Test Product Without Exports",
                exports=[],
                states=[ProductStateConfig(state_code="US_XX", environment="staging")],
                environment=None,
            ),
        ]

        self.assertEqual(expected_product_configs, product_configs)

    def test_product_configs_from_product_config_file(self) -> None:
        _product_configs = ProductConfig.product_configs_from_file(PRODUCTS_CONFIG_PATH)


class TestExportViewCollectionConfig(unittest.TestCase):
    """Tests the functionality of the ExportViewCollectionConfig class."""

    def setUp(self) -> None:
        self.mock_project_id = "fake-recidiviz-project"
        self.mock_dataset_id = "base_dataset"
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_dataset_id
        )

        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.mock_big_query_view_namespace = BigQueryViewNamespace.STATE

        self.mock_view_builder = MetricBigQueryViewBuilder(
            dataset_id=self.mock_dataset.dataset_id,
            view_id="test_view",
            description="test_view description",
            view_query_template="SELECT NULL LIMIT 0",
            dimensions=(),
        )

        self.views_for_dataset = [self.mock_view_builder]

        # Ensures StateCode.US_XX is properly loaded
        importlib.reload(states)

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_unique_export_names(self) -> None:
        self.assertEqual(
            len(_VIEW_COLLECTION_EXPORT_CONFIGS),
            len(VIEW_COLLECTION_EXPORT_INDEX.keys()),
        )

    def test_metric_export_state_agnostic(self) -> None:
        """Tests the export_configs_for_views_to_export function on the ExportViewCollectionConfig class when the
        export is state-agnostic."""
        state_agnostic_dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket-without-state-codes",
            export_name="ALL_STATE_TEST_PRODUCT",
            bq_view_namespace=self.mock_big_query_view_namespace,
        )

        view_configs_to_export = (
            state_agnostic_dataset_export_config.export_configs_for_views_to_export(
                project_id=self.mock_project_id,
            )
        )

        expected_view = self.mock_view_builder.build()

        expected_view_export_configs = [
            ExportBigQueryViewConfig(
                bq_view_namespace=self.mock_big_query_view_namespace,
                view=expected_view,
                view_filter_clause=None,
                intermediate_table_name=f"{expected_view.view_id}_table",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    state_agnostic_dataset_export_config.output_directory_uri_template.format(
                        project_id=self.mock_project_id,
                    )
                ),
                export_output_formats=[
                    ExportOutputFormatType.JSON,
                    ExportOutputFormatType.METRIC,
                ],
            )
        ]

        self.assertEqual(expected_view_export_configs, view_configs_to_export)

    def test_metric_export_state_specific(self) -> None:
        """Tests the export_configs_for_views_to_export function on the ExportViewCollectionConfig class when the
        export is state-specific."""
        specific_state_dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket",
            export_name="STATE_SPECIFIC_PRODUCT_EXPORT",
            bq_view_namespace=self.mock_big_query_view_namespace,
        )

        mock_export_job_filter = "US_XX"

        view_configs_to_export = (
            specific_state_dataset_export_config.export_configs_for_views_to_export(
                project_id=self.mock_project_id,
                state_code_filter=mock_export_job_filter,
            )
        )

        expected_view = self.mock_view_builder.build()

        expected_view_export_configs = [
            ExportBigQueryViewConfig(
                bq_view_namespace=self.mock_big_query_view_namespace,
                view=expected_view,
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{expected_view.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-bucket/US_XX"
                ),
                export_output_formats=[
                    ExportOutputFormatType.JSON,
                    ExportOutputFormatType.METRIC,
                ],
            )
        ]

        self.assertEqual(expected_view_export_configs, view_configs_to_export)

    def test_metric_export_lantern_dashboard(self) -> None:
        """Tests the export_configs_for_views_to_export function on the ExportViewCollectionConfig class when the
        export is state-agnostic."""
        lantern_dashboard_dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket-without-state-codes",
            export_name="TEST_EXPORT",
            bq_view_namespace=self.mock_big_query_view_namespace,
        )

        view_configs_to_export = (
            lantern_dashboard_dataset_export_config.export_configs_for_views_to_export(
                project_id=self.mock_project_id,
            )
        )

        expected_view = self.mock_view_builder.build()

        expected_view_export_configs = [
            ExportBigQueryViewConfig(
                bq_view_namespace=self.mock_big_query_view_namespace,
                view=expected_view,
                view_filter_clause=None,
                intermediate_table_name=f"{expected_view.view_id}_table",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    lantern_dashboard_dataset_export_config.output_directory_uri_template.format(
                        project_id=self.mock_project_id,
                    )
                ),
                export_output_formats=[
                    ExportOutputFormatType.JSON,
                    ExportOutputFormatType.METRIC,
                ],
            )
        ]

        self.assertEqual(expected_view_export_configs, view_configs_to_export)

    def test_metric_export_lantern_dashboard_with_state(self) -> None:
        """Tests the export_configs_for_views_to_export function on the ExportViewCollectionConfig class when the
        export is state-specific."""
        lantern_dashboard_with_state_dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket",
            export_name="TEST_EXPORT",
            bq_view_namespace=self.mock_big_query_view_namespace,
        )

        mock_export_job_filter = "US_XX"

        view_configs_to_export = lantern_dashboard_with_state_dataset_export_config.export_configs_for_views_to_export(
            project_id=self.mock_project_id, state_code_filter=mock_export_job_filter
        )

        expected_view = self.mock_view_builder.build()

        expected_view_export_configs = [
            ExportBigQueryViewConfig(
                bq_view_namespace=self.mock_big_query_view_namespace,
                view=expected_view,
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{expected_view.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-bucket/US_XX"
                ),
                export_output_formats=[
                    ExportOutputFormatType.JSON,
                    ExportOutputFormatType.METRIC,
                ],
            )
        ]

        self.assertEqual(expected_view_export_configs, view_configs_to_export)
