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

from recidiviz.big_query.export.export_query_config import (
    ExportOutputFormatType,
    ExportValidationType,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.metrics.export.export_config import (
    _VIEW_COLLECTION_EXPORT_CONFIGS,
    VIEW_COLLECTION_EXPORT_INDEX,
    ExportBigQueryViewConfig,
    ExportViewCollectionConfig,
)
from recidiviz.metrics.export.products.product_configs import ProductConfigs
from recidiviz.metrics.export.view_export_manager import get_delegate_export_map
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.utils.environment import (
    ALL_GCP_PROJECTS,
    DATA_PLATFORM_GCP_PROJECTS,
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
)


def strip_each_line(text: str) -> str:
    return "\n".join([line.strip() for line in text.splitlines()])


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

        self.mock_view_builder = MetricBigQueryViewBuilder(
            dataset_id=self.mock_dataset.dataset_id,
            view_id="test_view",
            description="test_view description",
            view_query_template="SELECT NULL LIMIT 0",
            dimensions=(),
        )

        self.views_for_dataset = [self.mock_view_builder]

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_unique_export_names(self) -> None:
        self.assertEqual(
            len(_VIEW_COLLECTION_EXPORT_CONFIGS),
            len(VIEW_COLLECTION_EXPORT_INDEX.keys()),
        )

    @mock.patch.object(ExportViewCollectionConfig, "output_directory")
    def test_metric_export_validations_match_formats(
        self, mock_output_directory: mock.MagicMock
    ) -> None:
        mock_output_directory.return_value = GcsfsDirectoryPath(
            bucket_name="test_bucket"
        )
        gcsfs = FakeGCSFileSystem()
        for config_collection in _VIEW_COLLECTION_EXPORT_CONFIGS:
            configs = config_collection.export_configs_for_views_to_export()

            try:
                get_delegate_export_map(
                    gcsfs_client=gcsfs,
                    export_name=config_collection.export_name,
                    export_configs=configs,
                )
            except Exception as e:
                self.fail(
                    f"Export configured with validation not matching its export type: {e}"
                )

    def test_metric_export_all_state_specific_export_views_materialized(self) -> None:
        product_configs = ProductConfigs.from_file()
        for config_collection in _VIEW_COLLECTION_EXPORT_CONFIGS:
            configs = product_configs.get_export_configs_for_job_filter(
                config_collection.export_name
            )
            if len(configs) <= 1:
                # If we only do one export for this product, we don't care if the views
                # are materialized from a cost-optimization perspective
                continue
            unmaterialized_views = {
                vb.address
                for vb in config_collection.view_builders_to_export
                if not vb.materialized_address
            }
            if unmaterialized_views:
                raise ValueError(
                    f"Exported views must be materialized for performance "
                    f"reasons. Found views in export [{config_collection.export_name}] which are not "
                    f"materialized: {unmaterialized_views}"
                )

    def test_metric_export_state_agnostic(self) -> None:
        """Tests the export_configs_for_views_to_export function on the ExportViewCollectionConfig class when the
        export is state-agnostic."""
        state_agnostic_dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket-without-state-codes",
            export_name="ALL_STATE_TEST_PRODUCT",
        )

        view_configs_to_export = (
            state_agnostic_dataset_export_config.export_configs_for_views_to_export()
        )

        expected_view = self.mock_view_builder.build()

        expected_view_export_configs = [
            ExportBigQueryViewConfig(
                view=expected_view,
                view_filter_clause=None,
                intermediate_table_name="ALL_STATE_TEST_PRODUCT_base_dataset_test_view_table",
                output_directory=state_agnostic_dataset_export_config.output_directory,
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                    ExportOutputFormatType.METRIC: [ExportValidationType.OPTIMIZED],
                },
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
        )

        mock_export_job_filter = "US_XX"

        view_configs_to_export = (
            specific_state_dataset_export_config.export_configs_for_views_to_export(
                state_code_filter=mock_export_job_filter,
            )
        )

        expected_view = self.mock_view_builder.build()

        expected_view_export_configs = [
            ExportBigQueryViewConfig(
                view=expected_view,
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name="STATE_SPECIFIC_PRODUCT_EXPORT_base_dataset_test_view_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-bucket/US_XX"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                    ExportOutputFormatType.METRIC: [ExportValidationType.OPTIMIZED],
                },
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
        )

        view_configs_to_export = (
            lantern_dashboard_dataset_export_config.export_configs_for_views_to_export()
        )

        expected_view = self.mock_view_builder.build()

        expected_view_export_configs = [
            ExportBigQueryViewConfig(
                view=expected_view,
                view_filter_clause=None,
                intermediate_table_name="TEST_EXPORT_base_dataset_test_view_table",
                output_directory=lantern_dashboard_dataset_export_config.output_directory,
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                    ExportOutputFormatType.METRIC: [ExportValidationType.OPTIMIZED],
                },
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
        )

        mock_state_code = "US_XX"

        view_configs_to_export = lantern_dashboard_with_state_dataset_export_config.export_configs_for_views_to_export(
            state_code_filter=mock_state_code
        )

        expected_view = self.mock_view_builder.build()

        expected_view_export_configs = [
            ExportBigQueryViewConfig(
                view=expected_view,
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name="TEST_EXPORT_base_dataset_test_view_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-bucket/US_XX"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                    ExportOutputFormatType.METRIC: [ExportValidationType.OPTIMIZED],
                },
            )
        ]

        self.assertEqual(expected_view_export_configs, view_configs_to_export)

    def test_metric_export_remapped_columns(self) -> None:
        """Tests the export_configs_for_views_to_export function on the ExportViewCollectionConfig class when the
        export is state-specific."""
        lantern_dashboard_with_state_dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket",
            export_name="TEST_EXPORT",
            export_override_state_codes={
                "US_XX": "US_XY",
                "US_ZZ": "US_ZY",
            },
        )

        mock_state_code = "US_XX"

        view_configs_to_export = lantern_dashboard_with_state_dataset_export_config.export_configs_for_views_to_export(
            state_code_filter=mock_state_code
        )
        view_config_to_export = view_configs_to_export[0]
        assert view_config_to_export is not None

        self.assertEqual(
            strip_each_line(view_config_to_export.query),
            strip_each_line(
                """
                 WITH base_data AS (
                    SELECT * FROM `fake-recidiviz-project.base_dataset.test_view_materialized`  WHERE state_code = 'US_XX'
                )
                SELECT CASE
                WHEN state_code = 'US_XX' THEN 'US_XY'
                ELSE state_code END AS state_code, * EXCEPT (state_code)
                FROM base_data
                """
            ),
        )

    def test_metric_export_override_state_code_destination(self) -> None:
        """Tests the export_configs_for_views_to_export function on the ExportViewCollectionConfig class when the
        export is state-specific."""
        lantern_dashboard_with_state_dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket",
            export_name="TEST_EXPORT",
            export_override_state_codes={
                "US_XX": "US_XY",
                "US_ZZ": "US_ZY",
            },
        )

        mock_state_code = "US_XX"

        view_configs_to_export = lantern_dashboard_with_state_dataset_export_config.export_configs_for_views_to_export(
            state_code_filter=mock_state_code
        )
        view_config_to_export = view_configs_to_export[0]
        assert view_config_to_export is not None

        # US_XX is exported to the US_XY/ directory
        self.assertEqual(
            GcsfsDirectoryPath(
                bucket_name="fake-recidiviz-project-bucket", relative_path="US_XY/"
            ),
            view_config_to_export.output_directory,
        )

        # Intermediate table name includes the overridden state code
        self.assertEqual(
            view_config_to_export.intermediate_table_name,
            "TEST_EXPORT_base_dataset_test_view_table_US_XY",
        )

    def test_metric_export_sandbox_prefix(self) -> None:
        """Tests the export_configs_for_views_to_export function on the ExportViewCollectionConfig class when the
        export includes a sandbox_prefix."""
        lantern_dashboard_with_state_dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket",
            export_name="TEST_EXPORT",
        )

        mock_state_code = "US_XX"

        view_configs_to_export = lantern_dashboard_with_state_dataset_export_config.export_configs_for_views_to_export(
            state_code_filter=mock_state_code,
            gcs_output_sandbox_subdir="my_prefix",
        )
        view_config_to_export = view_configs_to_export[0]
        assert view_config_to_export is not None

        self.assertEqual(
            GcsfsDirectoryPath(
                bucket_name="fake-recidiviz-project-bucket",
                relative_path="sandbox/my_prefix/US_XX/",
            ),
            view_config_to_export.output_directory,
        )

        self.assertEqual(
            view_config_to_export.intermediate_table_name,
            "TEST_EXPORT_base_dataset_test_view_table_US_XX",
        )

    def test_metric_export_override_output_project(self) -> None:
        """Tests the export_configs_for_views_to_export function on the ExportViewCollectionConfig class when the
        export destination is not in a data platform project."""
        lantern_dashboard_with_state_dataset_export_config = ExportViewCollectionConfig(
            view_builders_to_export=self.views_for_dataset,
            output_directory_uri_template="gs://{project_id}-bucket",
            export_name="TEST_EXPORT",
            output_project_by_data_project={
                self.mock_project_id: "test-project",
            },
        )

        mock_state_code = "US_XX"

        view_configs_to_export = lantern_dashboard_with_state_dataset_export_config.export_configs_for_views_to_export(
            state_code_filter=mock_state_code
        )
        view_config_to_export = view_configs_to_export[0]
        assert view_config_to_export is not None

        # Output project id is updated
        self.assertEqual(
            GcsfsDirectoryPath(
                bucket_name="test-project-bucket", relative_path="US_XX/"
            ),
            view_config_to_export.output_directory,
        )


class TestExportViewCollectionConfigOutputProjectDict(unittest.TestCase):
    """Tests the functionality of the ExportViewCollectionConfig class."""

    def setUp(self) -> None:
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_metric_exports_with_data_project_dict_uses_valid_projects(self) -> None:
        for export_name, config in VIEW_COLLECTION_EXPORT_INDEX.items():
            if config.output_project_by_data_project is None:
                continue

            for (
                data_project,
                output_project,
            ) in config.output_project_by_data_project.items():
                if data_project not in DATA_PLATFORM_GCP_PROJECTS:
                    raise ValueError(
                        f"{export_name} export config has an invalid key in the "
                        f"output_project_by_data_project field. Keys should be one of "
                        f"{','.join(DATA_PLATFORM_GCP_PROJECTS)}"
                    )

                if output_project not in ALL_GCP_PROJECTS:
                    raise ValueError(
                        f"{export_name} export config has an invalid value in the "
                        f"output_project_by_data_project field. Values should be one of "
                        f"{','.join(ALL_GCP_PROJECTS)}"
                    )

    def test_metric_exports_with_data_project_dict_has_valid_staging_output(
        self,
    ) -> None:
        self.mock_project_id_fn.return_value = GCP_PROJECT_STAGING
        for _, config in VIEW_COLLECTION_EXPORT_INDEX.items():
            if config.output_project_by_data_project is None:
                continue

            self.assertIsInstance(config.output_directory, GcsfsDirectoryPath)

    def test_metric_exports_with_data_project_dict_has_valid_production_output(
        self,
    ) -> None:
        self.mock_project_id_fn.return_value = GCP_PROJECT_PRODUCTION
        for _, config in VIEW_COLLECTION_EXPORT_INDEX.items():
            if config.output_project_by_data_project is None:
                continue

            self.assertIsInstance(config.output_directory, GcsfsDirectoryPath)
