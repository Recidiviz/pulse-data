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

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_view import (
    SimpleBigQueryViewBuilder,
)
from recidiviz.view_registry.namespaces import BigQueryViewNamespace
from recidiviz.big_query.export.big_query_view_export_validator import (
    BigQueryViewExportValidator,
)
from recidiviz.big_query.export.big_query_view_exporter import (
    CSVBigQueryViewExporter,
    JsonLinesBigQueryViewExporter,
)
from recidiviz.big_query.export.export_query_config import (
    ExportBigQueryViewConfig,
    ExportOutputFormatType,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath


class BigQueryViewExporterTest(unittest.TestCase):
    """Implements tests for BigQueryViewExporter."""

    def setUp(self) -> None:
        self.mock_bq_client = mock.create_autospec(BigQueryClient)
        self.mock_validator = mock.create_autospec(BigQueryViewExportValidator)

        self.mock_project_id = "fake-project"

        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.mock_bq_view_namespace = BigQueryViewNamespace.STATE

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
                bq_view_namespace=self.mock_bq_view_namespace,
                view=self.view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    "gs://{project_id}-dataset-location/subdirectory/{state_code}".format(
                        project_id=self.mock_project_id,
                        state_code="US_XX",
                    )
                ),
                export_output_formats=[
                    ExportOutputFormatType.JSON,
                    ExportOutputFormatType.HEADERLESS_CSV,
                ],
            ),
            ExportBigQueryViewConfig(
                bq_view_namespace=self.mock_bq_view_namespace,
                view=self.second_view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.second_view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    "gs://{project_id}-dataset-location/subdirectory/{state_code}".format(
                        project_id=self.mock_project_id,
                        state_code="US_XX",
                    )
                ),
                export_output_formats=[
                    ExportOutputFormatType.JSON,
                    ExportOutputFormatType.CSV,
                ],
            ),
        ]

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_csv_export_format(self) -> None:
        exporter = CSVBigQueryViewExporter(self.mock_bq_client, self.mock_validator)
        export_config_and_paths = exporter.export(self.view_export_configs)

        self.assertEqual(len(export_config_and_paths), len(self.view_export_configs))

        for _export_config, gcs_path in export_config_and_paths:
            self.assertTrue(
                gcs_path.file_name.endswith(".csv"),
                msg=f"GCS output file {gcs_path.abs_path()} is not a CSV as expected.",
            )

    def test_csv_throws(self) -> None:
        exporter = CSVBigQueryViewExporter(self.mock_bq_client, self.mock_validator)
        view_export_configs = [
            ExportBigQueryViewConfig(
                bq_view_namespace=self.mock_bq_view_namespace,
                view=self.view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    "gs://{project_id}-dataset-location/subdirectory/{state_code}".format(
                        project_id=self.mock_project_id,
                        state_code="US_XX",
                    )
                ),
                export_output_formats=[
                    ExportOutputFormatType.JSON,
                    ExportOutputFormatType.HEADERLESS_CSV,
                ],
            ),
            ExportBigQueryViewConfig(
                bq_view_namespace=self.mock_bq_view_namespace,
                view=self.second_view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.second_view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    "gs://{project_id}-dataset-location/subdirectory/{state_code}".format(
                        project_id=self.mock_project_id,
                        state_code="US_XX",
                    )
                ),
                export_output_formats=[ExportOutputFormatType.JSON],
            ),
        ]

        with self.assertRaises(ValueError):
            exporter.export(view_export_configs)

    def test_json_export_format(self) -> None:
        exporter = JsonLinesBigQueryViewExporter(
            self.mock_bq_client, self.mock_validator
        )
        export_config_and_paths = exporter.export(self.view_export_configs)

        self.assertEqual(len(export_config_and_paths), len(self.view_export_configs))

        for _export_config, gcs_path in export_config_and_paths:
            self.assertTrue(
                gcs_path.file_name.endswith(".json"),
                msg=f"GCS output file {gcs_path.abs_path()} is not a .json file as expected.",
            )

    def test_json_throws(self) -> None:
        exporter = JsonLinesBigQueryViewExporter(
            self.mock_bq_client, self.mock_validator
        )
        view_export_configs = [
            ExportBigQueryViewConfig(
                bq_view_namespace=self.mock_bq_view_namespace,
                view=self.view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    "gs://{project_id}-dataset-location/subdirectory/{state_code}".format(
                        project_id=self.mock_project_id,
                        state_code="US_XX",
                    )
                ),
                export_output_formats=[
                    ExportOutputFormatType.JSON,
                    ExportOutputFormatType.HEADERLESS_CSV,
                ],
            ),
            ExportBigQueryViewConfig(
                bq_view_namespace=self.mock_bq_view_namespace,
                view=self.second_view_builder.build(),
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.second_view_builder.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    "gs://{project_id}-dataset-location/subdirectory/{state_code}".format(
                        project_id=self.mock_project_id,
                        state_code="US_XX",
                    )
                ),
                export_output_formats=[ExportOutputFormatType.METRIC],
            ),
        ]

        with self.assertRaises(ValueError):
            exporter.export(view_export_configs)
