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
"""Tests for filename_parts.py."""
import datetime
from unittest import TestCase

import pytz

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.filename_parts import (
    filename_parts_from_path,
    is_path_normalized,
)
from recidiviz.ingest.direct.types.errors import DirectIngestError


class TestFilenamePartsFromPath(TestCase):
    """Tests for filename_parts.py."""

    def test_filename_parts_from_path_invalid_filename(self) -> None:
        with self.assertRaises(DirectIngestError):
            filename_parts_from_path(
                GcsfsFilePath.from_absolute_path("bucket/us_ca_sf/elite_offenders.csv")
            )

    def test_filename_parts_from_path_raw_file_type(self) -> None:
        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                "bucket-us-nd/unprocessed_2019-08-07T22:09:18:770655_"
                "raw_elite_offenders.csv"
            )
        )

        self.assertEqual(parts.processed_state, "unprocessed")
        self.assertEqual(parts.extension, "csv")
        self.assertEqual(parts.file_tag, "elite_offenders")
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime(2019, 8, 7, 22, 9, 18, 770655, tzinfo=pytz.UTC),
        )
        self.assertEqual(parts.date_str, "2019-08-07")

    def test_filename_parts_from_path_raw_file_type_with_numbers_in_file_tag(
        self,
    ) -> None:
        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                "bucket-us-mo/unprocessed_2019-09-07T00:09:18:770655_"
                "raw_tak001_offender_identification.csv"
            )
        )

        self.assertEqual(parts.processed_state, "unprocessed")
        self.assertEqual(parts.extension, "csv")
        self.assertEqual(parts.file_tag, "tak001_offender_identification")
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime(2019, 9, 7, 0, 9, 18, 770655, tzinfo=pytz.UTC),
        )
        self.assertEqual(parts.date_str, "2019-09-07")

    def test_filename_parts_from_path_raw_file_type_with_independent_numbers_in_file_tag(
        self,
    ) -> None:
        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                "bucket-us-mo/unprocessed_2021-09-21T00:00:00:000000_raw_CIS_100_CLIENT.csv"
            )
        )

        self.assertEqual(parts.processed_state, "unprocessed")
        self.assertEqual(parts.extension, "csv")
        self.assertEqual(parts.file_tag, "CIS_100_CLIENT")
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime(2021, 9, 21, tzinfo=pytz.UTC),
        )
        self.assertEqual(parts.date_str, "2021-09-21")

    def test_filename_parts_from_path_raw_file_type_with_leading_underscore(
        self,
    ) -> None:
        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                "bucket-us-mo/unprocessed_2023-04-01T00:00:00:000000_raw__HEARING_DATES.csv"
            )
        )

        self.assertEqual(parts.processed_state, "unprocessed")
        self.assertEqual(parts.extension, "csv")
        self.assertEqual(parts.file_tag, "_HEARING_DATES")
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime(2023, 4, 1, tzinfo=pytz.UTC),
        )
        self.assertEqual(parts.date_str, "2023-04-01")

    def test_filename_parts_from_path_raw_file_type_suffix(
        self,
    ) -> None:
        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                "bucket-us-tn/unprocessed_2022-03-24T06:02:28:607028_raw_ContactNoteType-1.csv"
            )
        )

        self.assertEqual(parts.processed_state, "unprocessed")
        self.assertEqual(parts.extension, "csv")
        self.assertEqual(parts.file_tag, "ContactNoteType")
        self.assertEqual(parts.filename_suffix, "1")
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime(2022, 3, 24, 6, 2, 28, 607028, tzinfo=pytz.UTC),
        )
        self.assertEqual(parts.date_str, "2022-03-24")

    def test_shouldnt_parse_timestamp_with_underscores(self) -> None:
        path = GcsfsFilePath.from_absolute_path(
            "bucket-us-tn/unprocessed_2024-09-13T17_19_55_995516_raw_ContactNoteType.csv"
        )
        with self.assertRaisesRegex(DirectIngestError, "Could not parse"):
            filename_parts_from_path(path)

        self.assertFalse(is_path_normalized(path))
