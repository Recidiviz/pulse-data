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
"""Tests for raw_file_path_transformers.py."""
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gcs.raw_file_path_transformers import (
    transform_raw_file_path,
)


class TestTransformRawFilePath(TestCase):
    """Tests for transform_raw_file_path."""

    def test_us_nyc_strips_embedded_timestamp(self) -> None:
        self.assertEqual(
            "PIC_FEED.csv",
            transform_raw_file_path(
                StateCode.US_NYC.value, "PIC_FEED_081920260030.csv"
            ),
        )
        self.assertEqual(
            "PROGRAMS_FEED.csv",
            transform_raw_file_path(
                StateCode.US_NYC.value, "PROGRAMS_FEED_082720260030.csv"
            ),
        )

    def test_us_nyc_region_code_is_case_insensitive(self) -> None:
        self.assertEqual(
            "PIC_FEED.csv",
            transform_raw_file_path("us_nyc", "PIC_FEED_081920260030.csv"),
        )

    def test_us_nyc_name_without_timestamp_is_unchanged(self) -> None:
        self.assertEqual(
            "PIC_FEED.csv",
            transform_raw_file_path(StateCode.US_NYC.value, "PIC_FEED.csv"),
        )

    def test_us_nyc_does_not_strip_short_numeric_suffix(self) -> None:
        # Only an exact 12-digit trailing stamp is considered; a shorter numeric
        # suffix is left intact.
        self.assertEqual(
            "PIC_FEED_12.csv",
            transform_raw_file_path(StateCode.US_NYC.value, "PIC_FEED_12.csv"),
        )

    def test_us_nyc_does_not_strip_invalid_date(self) -> None:
        # A 12-digit run that is not a valid MMDDYYYYHHMM datetime is not a timestamp,
        # so the name is left untouched.
        self.assertEqual(
            "PIC_FEED_999999999999.csv",
            transform_raw_file_path(
                StateCode.US_NYC.value, "PIC_FEED_999999999999.csv"
            ),
        )

    def test_region_without_transformer_is_unchanged(self) -> None:
        self.assertEqual(
            "PIC_FEED_081920260030.csv",
            transform_raw_file_path(StateCode.US_XX.value, "PIC_FEED_081920260030.csv"),
        )

    def test_us_tn_cashless_subfolder_is_flattened(self) -> None:
        self.assertEqual(
            "FOO.csv",
            transform_raw_file_path(StateCode.US_TN.value, "tn-cashless-sftp/FOO.csv"),
        )

    def test_us_tn_micase_dated_subfolder_is_flattened(self) -> None:
        self.assertEqual(
            "FOO.csv",
            transform_raw_file_path(
                StateCode.US_TN.value, "tn-micase-sftp/2026-01-01/FOO.csv"
            ),
        )

    def test_us_tn_region_code_is_case_insensitive(self) -> None:
        self.assertEqual(
            "FOO.csv",
            transform_raw_file_path("us_tn", "tn-cashless-sftp/FOO.csv"),
        )

    def test_us_tn_root_level_file_is_unchanged(self) -> None:
        self.assertEqual(
            "FOO.csv",
            transform_raw_file_path(StateCode.US_TN.value, "FOO.csv"),
        )

    def test_us_tn_unrecognized_subfolder_is_unchanged(self) -> None:
        self.assertEqual(
            "tn-other-vendor/FOO.csv",
            transform_raw_file_path(StateCode.US_TN.value, "tn-other-vendor/FOO.csv"),
        )

    def test_us_tn_micase_subfolder_without_date_is_unchanged(self) -> None:
        self.assertEqual(
            "tn-micase-sftp/FOO.csv",
            transform_raw_file_path(StateCode.US_TN.value, "tn-micase-sftp/FOO.csv"),
        )
