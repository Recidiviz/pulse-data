# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for the MoveFilesToDeprecatedController class"""
import unittest
from datetime import date

from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.ingest.operations.deprecate_raw_data import (
    MoveFilesToDeprecatedController,
)


class MoveFilesToDeprecatedControllerTest(unittest.TestCase):
    """Tests that constructed MoveFilesToDeprecatedController objects have expected attrs"""

    def setUp(self) -> None:
        self.project_id = "recidiviz-staging"
        self.expected_bucket = "recidiviz-staging-direct-ingest-state-storage"
        self.ingest_instance = DirectIngestInstance("PRIMARY")
        self.today = date.today().strftime("%Y-%m-%d")

    def test_controller_init_paths_are_correct_no_dates_or_filters(self) -> None:
        controller = MoveFilesToDeprecatedController(
            region_code="us_mi",
            project_id=self.project_id,
            ingest_instance=self.ingest_instance,
            start_date_bound=None,
            end_date_bound=None,
            dry_run=True,
            file_tag_filters=[],
            file_tag_regex=None,
            skip_prompts=True,
        )
        start_path = controller.region_storage_dir_path
        end_path = controller.deprecated_region_storage_dir_path

        self.assertEqual(self.expected_bucket, start_path.bucket_name)
        self.assertEqual(self.expected_bucket, end_path.bucket_name)
        self.assertEqual("us_mi/", start_path.relative_path)
        self.assertEqual(
            f"us_mi/deprecated/deprecated_on_{self.today}/", end_path.relative_path
        )

    def test_controller_init_paths_are_correct_with_dates_no_filters(self) -> None:
        """
        Start and end dates are used to search for all subdirectories in the date range,
        so the relative paths should be the same as the last test.
        """
        controller = MoveFilesToDeprecatedController(
            region_code="us_mi",
            project_id=self.project_id,
            ingest_instance=self.ingest_instance,
            start_date_bound="2023-10-30",
            end_date_bound="2023-10-30",
            dry_run=True,
            file_tag_filters=[],
            file_tag_regex=None,
            skip_prompts=True,
        )
        start_path = controller.region_storage_dir_path
        end_path = controller.deprecated_region_storage_dir_path

        self.assertEqual(self.expected_bucket, start_path.bucket_name)
        self.assertEqual(self.expected_bucket, end_path.bucket_name)
        self.assertEqual("us_mi/", start_path.relative_path)
        self.assertEqual(
            f"us_mi/deprecated/deprecated_on_{self.today}/", end_path.relative_path
        )
