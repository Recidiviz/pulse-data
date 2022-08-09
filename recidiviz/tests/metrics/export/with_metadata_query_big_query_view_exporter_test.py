# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Implements tests for WithMetadataQueryBigQueryViewExporter."""

import unittest

import mock
from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.export.big_query_view_export_validator import (
    BigQueryViewExportValidator,
)
from recidiviz.big_query.export.export_query_config import (
    ExportBigQueryViewConfig,
    ExportOutputFormatType,
)
from recidiviz.big_query.with_metadata_query_big_query_view import (
    WithMetadataQueryBigQueryViewBuilder,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.metrics.export.with_metadata_query_big_query_view_exporter import (
    WithMetadataQueryBigQueryViewExporter,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class WithMetadataQueryBigQueryViewExporterTest(unittest.TestCase):
    """Implements tests for WithMetadataQueryBigQueryViewExporter."""

    def setUp(self) -> None:
        self.mock_bq_client = mock.create_autospec(BigQueryClient)
        self.mock_validator = mock.create_autospec(BigQueryViewExportValidator)

        self.mock_project_id = "fake-project"

        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.fs = FakeGCSFileSystem()
        self.fs_patcher = mock.patch.object(GcsfsFactory, "build", return_value=self.fs)
        self.fs_patcher.start()

        self.view_builder = WithMetadataQueryBigQueryViewBuilder(
            delegate=SimpleBigQueryViewBuilder(
                dataset_id="test_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="SELECT NULL LIMIT 0",
            ),
            metadata_query="SELECT 'test' as col, 'US_XX' as state_code",
        )
        view = self.view_builder.build()
        self.second_view_builder = WithMetadataQueryBigQueryViewBuilder(
            delegate=SimpleBigQueryViewBuilder(
                dataset_id="test_dataset",
                view_id="test_view_2",
                description="test_view_2 description",
                view_query_template="SELECT NULL LIMIT 0",
            ),
            metadata_query="SELECT 'test2' as {col}, 'US_YY' as state_code",
            col="col_name",
        )
        second_view = self.second_view_builder.build()
        self.view_export_configs = [
            ExportBigQueryViewConfig(
                view=view,
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{view.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats=[
                    ExportOutputFormatType.HEADERLESS_CSV_WITH_METADATA,
                ],
            ),
            ExportBigQueryViewConfig(
                view=second_view,
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{second_view.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats=[
                    ExportOutputFormatType.HEADERLESS_CSV_WITH_METADATA,
                ],
            ),
        ]
        self.output_dirs = [
            config.output_directory for config in self.view_export_configs
        ]

    def tearDown(self) -> None:
        self.metadata_patcher.stop()
        self.fs_patcher.stop()

    def test_export_with_metadata(self) -> None:
        return_values = [
            [bigquery.Row(("test", "US_XX"), {"col": 0, "state_code": 1})],
            [bigquery.Row(("test2", "US_YY"), {"col_name": 0, "state_code": 1})],
        ]
        self.mock_bq_client.run_query_async.side_effect = return_values

        exporter = WithMetadataQueryBigQueryViewExporter(
            self.mock_bq_client, self.mock_validator
        )
        export_config_and_paths = exporter.export(self.view_export_configs)
        self.assertEqual(len(export_config_and_paths), len(self.view_export_configs))

        expected_metadata = [
            {"col": "test", "state_code": "US_XX"},
            {"col_name": "test2", "state_code": "US_YY"},
        ]
        for i, (_export_config, gcs_path) in enumerate(export_config_and_paths):
            self.assertTrue(
                gcs_path.file_name.endswith(".csv"),
                msg=f"GCS output file {gcs_path.abs_path()} is not a CSV as expected.",
            )
            self.assertEqual(self.fs.get_metadata(gcs_path), expected_metadata[i])

    def test_export_no_metadata_results(self) -> None:
        self.mock_bq_client.run_query_async.return_value = []

        exporter = WithMetadataQueryBigQueryViewExporter(
            self.mock_bq_client, self.mock_validator
        )
        export_config_and_paths = exporter.export(self.view_export_configs)

        self.assertEqual(len(export_config_and_paths), len(self.view_export_configs))

        for _export_config, gcs_path in export_config_and_paths:
            self.assertTrue(
                gcs_path.file_name.endswith(".csv"),
                msg=f"GCS output file {gcs_path.abs_path()} is not a CSV as expected.",
            )
            self.assertFalse(self.fs.get_metadata(gcs_path))
