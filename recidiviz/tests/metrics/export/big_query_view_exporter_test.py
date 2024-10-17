# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Implements tests for BigQueryViewExporter."""
import unittest

import mock
from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.export.big_query_view_export_validator import (
    BigQueryViewExportValidator,
)
from recidiviz.big_query.export.big_query_view_exporter import (
    CSVBigQueryViewExporter,
    HeaderlessCSVBigQueryViewExporter,
    JsonLinesBigQueryViewExporter,
)
from recidiviz.big_query.export.export_query_config import (
    ExportBigQueryViewConfig,
    ExportOutputFormatType,
    ExportQueryConfig,
    ExportValidationType,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath


class BigQueryViewExporterTest(unittest.TestCase):
    """Implements tests for BigQueryViewExporter."""

    def setUp(self) -> None:
        self.mock_bq_client = mock.create_autospec(BigQueryClient)
        self.mock_validator = mock.create_autospec(BigQueryViewExportValidator)

        self.mock_project_id = "fake-project"

        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.view_builder = SimpleBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="test_view",
            description="test_view description",
            view_query_template="SELECT NULL LIMIT 0",
        )
        self.second_view_builder = SimpleBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="test_view_2",
            description="test_view_2 description",
            view_query_template="SELECT NULL LIMIT 0",
        )
        self.view_export_configs = [
            ExportBigQueryViewConfig(
                view=self.view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                    ExportOutputFormatType.HEADERLESS_CSV: [
                        ExportValidationType.NON_EMPTY_COLUMNS
                    ],
                },
            ),
            ExportBigQueryViewConfig(
                view=self.second_view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.second_view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats_and_validations={
                    # Note that the use of `ExportViewCollectionConfig` ensures that
                    # export_output_formats_and_validations is the same in each view for a group of
                    # views being exported together.
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                    ExportOutputFormatType.HEADERLESS_CSV: [
                        ExportValidationType.NON_EMPTY_COLUMNS
                    ],
                },
            ),
        ]

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_csv_export_format(self) -> None:
        file_paths = [
            [GcsfsFilePath.from_absolute_path("gs://bucket/export-1.csv")],
            [GcsfsFilePath.from_absolute_path("gs://bucket/export-2.csv")],
        ]

        exporter = CSVBigQueryViewExporter(self.mock_bq_client, self.mock_validator)
        view_export_configs = [
            ExportBigQueryViewConfig(
                view=self.view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                    ExportOutputFormatType.CSV: [
                        ExportValidationType.NON_EMPTY_COLUMNS
                    ],
                },
            ),
            ExportBigQueryViewConfig(
                view=self.second_view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.second_view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats_and_validations={
                    # Note that the use of `ExportViewCollectionConfig` ensures that
                    # export_output_formats_and_validations is the same in each view for a group of
                    # views being exported together.
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                    ExportOutputFormatType.CSV: [
                        ExportValidationType.NON_EMPTY_COLUMNS
                    ],
                },
            ),
        ]

        first_output_path = view_export_configs[0].output_path("csv")
        second_output_path = view_export_configs[1].output_path("csv")
        file_paths = [[first_output_path], [second_output_path]]

        export_query_configs = [
            config.as_export_query_config(output_format=bigquery.DestinationFormat.CSV)
            for config in view_export_configs
        ]
        self.mock_bq_client.export_query_results_to_cloud_storage.return_value = zip(
            export_query_configs, file_paths
        )

        # Act
        exporter.export(view_export_configs)

        # Assert export was called for both export configs with the correct CSV format
        self.mock_bq_client.export_query_results_to_cloud_storage.assert_called_with(
            export_configs=[
                ExportQueryConfig(
                    query="SELECT * FROM `fake-project.test_dataset.test_view`  WHERE state_code = 'US_XX'",
                    query_parameters=[],
                    intermediate_dataset_id="test_dataset",
                    intermediate_table_name="test_view_table_US_XX",
                    output_uri=first_output_path.uri(),
                    output_format="CSV",
                ),
                ExportQueryConfig(
                    query="SELECT * FROM `fake-project.test_dataset.test_view_2`  WHERE state_code = 'US_XX'",
                    query_parameters=[],
                    intermediate_dataset_id="test_dataset",
                    intermediate_table_name="test_view_2_table_US_XX",
                    output_uri=second_output_path.uri(),
                    output_format="CSV",
                ),
            ],
            print_header=True,
            use_query_cache=True,
        )

    def test_csv_throws(self) -> None:
        exporter = CSVBigQueryViewExporter(self.mock_bq_client, self.mock_validator)
        view_export_configs = [
            ExportBigQueryViewConfig(
                view=self.view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                    ExportOutputFormatType.HEADERLESS_CSV: [],
                },
            ),
            ExportBigQueryViewConfig(
                view=self.second_view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.second_view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS]
                },
            ),
        ]

        with self.assertRaises(ValueError):
            exporter.export(view_export_configs)

    def test_headerless_csv_export_format(self) -> None:
        first_output_path = self.view_export_configs[0].output_path("csv")
        second_output_path = self.view_export_configs[1].output_path("csv")
        file_paths = [[first_output_path], [second_output_path]]

        export_query_configs = [
            config.as_export_query_config(output_format=bigquery.DestinationFormat.CSV)
            for config in self.view_export_configs
        ]
        self.mock_bq_client.export_query_results_to_cloud_storage.return_value = zip(
            export_query_configs, file_paths
        )

        exporter = HeaderlessCSVBigQueryViewExporter(
            self.mock_bq_client, self.mock_validator
        )

        # Act
        exporter.export(self.view_export_configs)

        # Assert exports occurred to out first / second output paths in CSV format with no header
        self.mock_bq_client.export_query_results_to_cloud_storage.assert_called_with(
            export_configs=[
                ExportQueryConfig(
                    query="SELECT * FROM `fake-project.test_dataset.test_view`  WHERE state_code = 'US_XX'",
                    query_parameters=[],
                    intermediate_dataset_id="test_dataset",
                    intermediate_table_name="test_view_table_US_XX",
                    output_uri=first_output_path.uri(),
                    output_format="CSV",
                ),
                ExportQueryConfig(
                    query="SELECT * FROM `fake-project.test_dataset.test_view_2`  WHERE state_code = 'US_XX'",
                    query_parameters=[],
                    intermediate_dataset_id="test_dataset",
                    intermediate_table_name="test_view_2_table_US_XX",
                    output_uri=second_output_path.uri(),
                    output_format="CSV",
                ),
            ],
            print_header=False,
            use_query_cache=True,
        )

    def test_headerless_csv_throws(self) -> None:
        exporter = HeaderlessCSVBigQueryViewExporter(
            self.mock_bq_client, self.mock_validator
        )
        view_export_configs = [
            ExportBigQueryViewConfig(
                view=self.view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                    ExportOutputFormatType.CSV: [],
                },
            ),
            ExportBigQueryViewConfig(
                view=self.second_view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.second_view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS]
                },
            ),
        ]

        with self.assertRaises(ValueError):
            exporter.export(view_export_configs)

    def test_json_export_format(self) -> None:
        file_paths = [
            [GcsfsFilePath.from_absolute_path("gs://bucket/export-1.json")],
            [GcsfsFilePath.from_absolute_path("gs://bucket/export-2.json")],
        ]

        export_query_configs = [
            config.as_export_query_config(
                output_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
            )
            for config in self.view_export_configs
        ]
        self.mock_bq_client.export_query_results_to_cloud_storage.return_value = zip(
            export_query_configs, file_paths
        )

        exporter = JsonLinesBigQueryViewExporter(
            self.mock_bq_client, self.mock_validator
        )
        export_config_and_paths = exporter.export(self.view_export_configs)

        self.assertEqual(len(export_config_and_paths), len(self.view_export_configs))

        exported_paths = [
            gcs_paths for _export_config, gcs_paths in export_config_and_paths
        ]

        self.assertEqual(exported_paths, file_paths)

    def test_json_throws(self) -> None:
        exporter = JsonLinesBigQueryViewExporter(
            self.mock_bq_client, self.mock_validator
        )
        view_export_configs = [
            ExportBigQueryViewConfig(
                view=self.view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                    ExportOutputFormatType.HEADERLESS_CSV: [],
                },
            ),
            ExportBigQueryViewConfig(
                view=self.second_view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.second_view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.METRIC: [ExportValidationType.OPTIMIZED]
                },
            ),
        ]

        with self.assertRaises(ValueError):
            exporter.export(view_export_configs)
