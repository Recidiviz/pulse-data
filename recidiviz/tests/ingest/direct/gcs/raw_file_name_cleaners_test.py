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
"""Tests for raw_file_name_cleaners.py."""
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gcs.raw_file_name_cleaners import clean_raw_file_name


class TestCleanRawFileName(TestCase):
    """Tests for clean_raw_file_name."""

    def test_us_nyc_strips_embedded_timestamp(self) -> None:
        self.assertEqual(
            "PIC_FEED.csv",
            clean_raw_file_name(StateCode.US_NYC.value, "PIC_FEED_081920260030.csv"),
        )
        self.assertEqual(
            "PROGRAMS_FEED.csv",
            clean_raw_file_name(
                StateCode.US_NYC.value, "PROGRAMS_FEED_082720260030.csv"
            ),
        )

    def test_us_nyc_region_code_is_case_insensitive(self) -> None:
        self.assertEqual(
            "PIC_FEED.csv",
            clean_raw_file_name("us_nyc", "PIC_FEED_081920260030.csv"),
        )

    def test_us_nyc_name_without_timestamp_is_unchanged(self) -> None:
        self.assertEqual(
            "PIC_FEED.csv",
            clean_raw_file_name(StateCode.US_NYC.value, "PIC_FEED.csv"),
        )

    def test_us_nyc_does_not_strip_short_numeric_suffix(self) -> None:
        # Only an exact 12-digit trailing stamp is considered; a shorter numeric
        # suffix is left intact.
        self.assertEqual(
            "PIC_FEED_12.csv",
            clean_raw_file_name(StateCode.US_NYC.value, "PIC_FEED_12.csv"),
        )

    def test_us_nyc_does_not_strip_invalid_date(self) -> None:
        # A 12-digit run that is not a valid MMDDYYYYHHMM datetime is not a timestamp,
        # so the name is left untouched.
        self.assertEqual(
            "PIC_FEED_999999999999.csv",
            clean_raw_file_name(StateCode.US_NYC.value, "PIC_FEED_999999999999.csv"),
        )

    def test_region_without_cleaner_is_unchanged(self) -> None:
        self.assertEqual(
            "PIC_FEED_081920260030.csv",
            clean_raw_file_name(StateCode.US_XX.value, "PIC_FEED_081920260030.csv"),
        )
