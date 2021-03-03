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

"""Tests for optimized_metric_big_query_view_export_validator.py."""

import unittest

from mock import create_autospec, patch, call

from recidiviz.big_query.export.export_query_config import ExportBigQueryViewConfig
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.metrics.export.optimized_metric_big_query_view_export_validator import (
    OptimizedMetricBigQueryViewExportValidator,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder


class ValidateTest(unittest.TestCase):
    """Tests for optimized_metric_big_query_view_export_validator.validate"""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "project-id"

        metric_view_one = MetricBigQueryViewBuilder(
            dataset_id="dataset",
            view_id="view1",
            view_query_template="select * from table",
            dimensions=["a", "b", "c"],
        ).build()

        export_config_one_staging = ExportBigQueryViewConfig(
            view=metric_view_one,
            view_filter_clause="WHERE state_code = 'US_XX'",
            intermediate_table_name="intermediate_table",
            output_directory=GcsfsDirectoryPath.from_absolute_path(
                "gs://bucket1/staging/US_XX"
            ),
        )

        metric_view_two = MetricBigQueryViewBuilder(
            dataset_id="dataset",
            view_id="view2",
            view_query_template="select * from view2",
            dimensions=["d", "e", "f"],
        ).build()

        export_config_two_staging = ExportBigQueryViewConfig(
            view=metric_view_two,
            view_filter_clause="WHERE state_code = 'US_XX'",
            intermediate_table_name="intermediate_table2",
            output_directory=GcsfsDirectoryPath.from_absolute_path(
                "gs://bucket2/staging/US_XX"
            ),
        )

        self.staging_paths = [
            export_config_one_staging.output_path("txt"),
            export_config_two_staging.output_path("txt"),
        ]

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_validate_success(self):
        mock_fs = create_autospec(DirectIngestGCSFileSystem)

        mock_fs.get_metadata.return_value = {"total_data_points": "5"}

        validator = OptimizedMetricBigQueryViewExportValidator(mock_fs)
        for path in self.staging_paths:
            result = validator.validate(path)
            self.assertTrue(result)

        mock_fs.assert_has_calls(
            [
                call.get_metadata(self.staging_paths[0]),
                call.get_metadata(self.staging_paths[1]),
            ]
        )

    def test_validate_failure(self):
        mock_fs = create_autospec(DirectIngestGCSFileSystem)

        mock_fs.get_metadata.side_effect = [
            {"total_data_points": "5"},
            {"total_data_points": "0"},
        ]

        validator = OptimizedMetricBigQueryViewExportValidator(mock_fs)

        self.assertTrue(validator.validate(self.staging_paths[0]))
        self.assertFalse(validator.validate(self.staging_paths[1]))

        mock_fs.assert_has_calls(
            [
                call.get_metadata(self.staging_paths[0]),
                call.get_metadata(self.staging_paths[1]),
            ]
        )

    def test_validate_not_integer(self):
        mock_fs = create_autospec(DirectIngestGCSFileSystem)

        mock_fs.get_metadata.return_value = {"total_data_points": "HELLO WORLD"}

        validator = OptimizedMetricBigQueryViewExportValidator(mock_fs)
        for path in self.staging_paths:
            result = validator.validate(path)
            self.assertFalse(result)

        # We failed before validating the second path
        mock_fs.assert_has_calls([call.get_metadata(self.staging_paths[0])])

    def test_validate_no_metadata(self):
        mock_fs = create_autospec(DirectIngestGCSFileSystem)

        mock_fs.get_metadata.return_value = None

        validator = OptimizedMetricBigQueryViewExportValidator(mock_fs)
        for path in self.staging_paths:
            result = validator.validate(path)
            self.assertFalse(result)

        # We failed before validating the second path
        mock_fs.assert_has_calls([call.get_metadata(self.staging_paths[0])])
