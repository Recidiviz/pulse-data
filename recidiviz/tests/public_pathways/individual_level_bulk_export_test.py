# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for individual_level_bulk_export.py."""
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.public_pathways.individual_level_bulk_export import (
    BULK_EXPORT_MONTHS,
    _trailing_months,
    bulk_export_filename,
    bulk_export_gcs_path,
)


class TestTrailingMonths(unittest.TestCase):
    """Tests for _trailing_months."""

    def test_returns_months_in_chronological_order_ending_with_end_date(self) -> None:
        months = _trailing_months(end_date=date(2022, 3, 15), num_months=3)
        self.assertEqual([(2022, 1), (2022, 2), (2022, 3)], months)

    def test_wraps_across_year_boundary(self) -> None:
        months = _trailing_months(end_date=date(2022, 2, 1), num_months=4)
        self.assertEqual([(2021, 11), (2021, 12), (2022, 1), (2022, 2)], months)

    def test_returns_num_months_entries(self) -> None:
        months = _trailing_months(
            end_date=date(2022, 1, 1), num_months=BULK_EXPORT_MONTHS
        )
        self.assertEqual(BULK_EXPORT_MONTHS, len(months))
        self.assertEqual((2022, 1), months[-1])


class TestBulkExportFilename(unittest.TestCase):
    """Tests for bulk_export_filename."""

    def test_filename_includes_state_and_last_updated(self) -> None:
        self.assertEqual(
            "us_ny_individual_level_data_last_5_years_2022-08-03.zip",
            bulk_export_filename(
                state_code=StateCode.US_NY, last_updated=date(2022, 8, 3)
            ),
        )


class TestBulkExportGcsPath(unittest.TestCase):
    """Tests for bulk_export_gcs_path."""

    @patch(
        "recidiviz.public_pathways.individual_level_bulk_export.gcp_metadata.project_id",
        MagicMock(return_value="test-project"),
    )
    def test_path_is_scoped_to_project_and_state(self) -> None:
        gcs_path = bulk_export_gcs_path(state_code=StateCode.US_NY)
        self.assertEqual("test-project-public-pathways-data", gcs_path.bucket_name)
        self.assertEqual("individual_level_bulk_exports/us_ny.zip", gcs_path.blob_name)
