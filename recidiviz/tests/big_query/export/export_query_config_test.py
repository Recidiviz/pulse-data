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
"""Tests for our export configs."""
import unittest
from mock import patch

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.export.export_query_config import ExportBigQueryViewConfig
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath


class TestPointingAtStagingSubdirectory(unittest.TestCase):
    """Tests for string manipulation with the staging directory."""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "project-id"

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def config_with_path(self, path: str) -> ExportBigQueryViewConfig:
        return ExportBigQueryViewConfig(
            view=SimpleBigQueryViewBuilder(
                dataset_id="test_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="you know",
            ).build(),
            view_filter_clause="WHERE state_code = 'US_XX'",
            intermediate_table_name="tubular",
            output_directory=GcsfsDirectoryPath.from_absolute_path(f"gs://{path}"),
        )

    def test_happy_path(self) -> None:
        pointed_at_staging_file = GcsfsFilePath.from_directory_and_file_name(
            self.config_with_path("gnarly")
            .pointed_to_staging_subdirectory()
            .output_directory,
            "foo.txt",
        )
        self.assertEqual(pointed_at_staging_file.abs_path(), "gnarly/staging/foo.txt")

        self.assertEqual(
            ExportBigQueryViewConfig.revert_staging_path_to_original(
                pointed_at_staging_file
            ),
            GcsfsFilePath.from_absolute_path("gs://gnarly/foo.txt"),
        )

    def test_filename_has_staging(self) -> None:
        pointed_at_staging_file = GcsfsFilePath.from_directory_and_file_name(
            self.config_with_path("gnarly")
            .pointed_to_staging_subdirectory()
            .output_directory,
            "staging_results.txt",
        )
        self.assertEqual(
            pointed_at_staging_file.abs_path(), "gnarly/staging/staging_results.txt"
        )

        self.assertEqual(
            ExportBigQueryViewConfig.revert_staging_path_to_original(
                pointed_at_staging_file
            ),
            GcsfsFilePath.from_absolute_path("gs://gnarly/staging_results.txt"),
        )

    def test_noop_without_staging(self) -> None:
        not_pointed_at_staging_file = GcsfsFilePath.from_directory_and_file_name(
            self.config_with_path("gnarly").output_directory, "staging_results.txt"
        )
        self.assertEqual(
            ExportBigQueryViewConfig.revert_staging_path_to_original(
                not_pointed_at_staging_file
            ),
            GcsfsFilePath.from_absolute_path("gs://gnarly/staging_results.txt"),
        )

    def test_nested_folders(self) -> None:
        pointed_at_staging_file = GcsfsFilePath.from_directory_and_file_name(
            self.config_with_path("gnarly/subdirectory/US_MO")
            .pointed_to_staging_subdirectory()
            .output_directory,
            "foo.txt",
        )
        self.assertEqual(
            pointed_at_staging_file.abs_path(),
            "gnarly/staging/subdirectory/US_MO/foo.txt",
        )

        self.assertEqual(
            ExportBigQueryViewConfig.revert_staging_path_to_original(
                pointed_at_staging_file
            ),
            GcsfsFilePath.from_absolute_path("gs://gnarly/subdirectory/US_MO/foo.txt"),
        )
