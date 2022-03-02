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

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
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
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.RAW_DATA)
        self.assertEqual(parts.file_tag, "elite_offenders")
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime.fromisoformat("2019-08-07T22:09:18:770655"),
        )
        self.assertEqual(parts.date_str, "2019-08-07")
        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)

    def test_filename_parts_from_path_ingest_view_file_type_no_split_file(self) -> None:
        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                "bucket-us-nd/processed_2019-09-07T00:09:18:770655_"
                "ingest_view_elite_offenders.csv"
            )
        )
        self.assertEqual(parts.processed_state, "processed")
        self.assertEqual(parts.extension, "csv")
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.INGEST_VIEW)
        self.assertEqual(parts.file_tag, "elite_offenders")
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime.fromisoformat("2019-09-07T00:09:18:770655"),
        )
        self.assertEqual(parts.date_str, "2019-09-07")
        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)

    def test_filename_parts_from_path_ingest_view_file_type_with_filename_suffix(
        self,
    ) -> None:
        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                "bucket-us-nd/processed_2019-09-07T00:09:18:770655_"
                "ingest_view_elite_offenders_1split.csv"
            )
        )

        self.assertEqual(parts.processed_state, "processed")
        self.assertEqual(parts.extension, "csv")
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.INGEST_VIEW)
        self.assertEqual(parts.file_tag, "elite_offenders")
        self.assertEqual(parts.filename_suffix, "1split")
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime.fromisoformat("2019-09-07T00:09:18:770655"),
        )
        self.assertEqual(parts.date_str, "2019-09-07")
        # Needs the actual file_split suffix to be a file split
        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)

    def test_filename_parts_from_path_ingest_view_file_type_with_file_split_no_size(
        self,
    ) -> None:
        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                "bucket-us-nd/processed_2019-09-07T00:09:18:770655_"
                "ingest_view_elite_offenders_002_file_split.csv"
            )
        )

        self.assertEqual(parts.processed_state, "processed")
        self.assertEqual(parts.extension, "csv")
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.INGEST_VIEW)
        self.assertEqual(parts.file_tag, "elite_offenders")
        self.assertEqual(parts.filename_suffix, "002_file_split")
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime.fromisoformat("2019-09-07T00:09:18:770655"),
        )
        self.assertEqual(parts.date_str, "2019-09-07")

        self.assertEqual(parts.is_file_split, True)
        self.assertEqual(parts.file_split_size, None)

    def test_filename_parts_from_path_ingest_view_file_type_with_file_split_and_size(
        self,
    ) -> None:
        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                "bucket-us-nd/processed_2019-09-07T00:09:18:770655_"
                "ingest_view_elite_offenders_002_file_split_size300.csv"
            )
        )

        self.assertEqual(parts.processed_state, "processed")
        self.assertEqual(parts.extension, "csv")
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.INGEST_VIEW)
        self.assertEqual(parts.file_tag, "elite_offenders")
        self.assertEqual(parts.filename_suffix, "002_file_split_size300")
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime.fromisoformat("2019-09-07T00:09:18:770655"),
        )
        self.assertEqual(parts.date_str, "2019-09-07")

        self.assertEqual(parts.is_file_split, True)
        self.assertEqual(parts.file_split_size, 300)

    def test_filename_parts_from_path_ingest_view_file_type_with_date_filename_suffix(
        self,
    ) -> None:
        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                "bucket-us-nd/processed_2019-09-07T00:09:18:770655_"
                "ingest_view_BrazosCounty_2019_09_25.csv"
            )
        )

        self.assertEqual(parts.processed_state, "processed")
        self.assertEqual(parts.extension, "csv")
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.INGEST_VIEW)
        self.assertEqual(parts.file_tag, "BrazosCounty")
        self.assertEqual(parts.filename_suffix, "2019_09_25")
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime.fromisoformat("2019-09-07T00:09:18:770655"),
        )
        self.assertEqual(parts.date_str, "2019-09-07")

        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)

    def test_filename_parts_from_path_ingest_view_file_type_with_date_filename_suffix_and_file_split_size(
        self,
    ) -> None:
        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                "bucket-us-nd/processed_2019-09-07T00:09:18:770655_"
                "ingest_view_BrazosCounty_2019_09_25_002_file_split_size300.csv"
            )
        )

        self.assertEqual(parts.processed_state, "processed")
        self.assertEqual(parts.extension, "csv")
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.INGEST_VIEW)
        self.assertEqual(parts.file_tag, "BrazosCounty")
        self.assertEqual(parts.filename_suffix, "2019_09_25_002_file_split_size300")
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime.fromisoformat("2019-09-07T00:09:18:770655"),
        )
        self.assertEqual(parts.date_str, "2019-09-07")

        self.assertEqual(parts.is_file_split, True)
        self.assertEqual(parts.file_split_size, 300)

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
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.RAW_DATA)
        self.assertEqual(parts.file_tag, "tak001_offender_identification")
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime.fromisoformat("2019-09-07T00:09:18:770655"),
        )
        self.assertEqual(parts.date_str, "2019-09-07")

        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)

    def test_filename_parts_from_path_ingest_view_file_type_with_numbers_in_file_tag_and_file_split(
        self,
    ) -> None:
        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                "bucket-us-mo/unprocessed_2019-09-07T00:09:18:770655_"
                "ingest_view_tak001_offender_identification_002_file_split_size300.csv"
            )
        )

        self.assertEqual(parts.processed_state, "unprocessed")
        self.assertEqual(parts.extension, "csv")
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.INGEST_VIEW)
        self.assertEqual(parts.file_tag, "tak001_offender_identification")
        self.assertEqual(parts.filename_suffix, "002_file_split_size300")
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime.fromisoformat("2019-09-07T00:09:18:770655"),
        )
        self.assertEqual(parts.date_str, "2019-09-07")

        self.assertEqual(parts.is_file_split, True)
        self.assertEqual(parts.file_split_size, 300)

    def test_filename_parts_from_path_ingest_view_file_type_with_file_split_parts(
        self,
    ) -> None:
        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                "storage_bucket/raw/2020/04/29/processed_2020-04-29T18:02:41:789323_ingest_view_test_file-(1).csv"
            )
        )

        self.assertEqual(parts.processed_state, "processed")
        self.assertEqual(parts.extension, "csv")
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.INGEST_VIEW)
        self.assertEqual(parts.file_tag, "test_file")
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime.fromisoformat("2020-04-29T18:02:41:789323"),
        )
        self.assertEqual(parts.date_str, "2020-04-29")

        self.assertEqual(parts.is_file_split, False)

    def test_filename_parts_from_path_ingest_view_file_type_with_file_split_parts_and_numbers_in_file_tag(
        self,
    ) -> None:
        parts = filename_parts_from_path(
            GcsfsFilePath.from_absolute_path(
                "bucket-us-mo/unprocessed_2019-09-07T00:09:18:770655_"
                "ingest_view_tak001_offender_identification_002_file_split_size300-(5).csv"
            )
        )

        self.assertEqual(parts.processed_state, "unprocessed")
        self.assertEqual(parts.extension, "csv")
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.INGEST_VIEW)
        self.assertEqual(parts.file_tag, "tak001_offender_identification")
        self.assertEqual(parts.filename_suffix, "002_file_split_size300")
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime.fromisoformat("2019-09-07T00:09:18:770655"),
        )
        self.assertEqual(parts.date_str, "2019-09-07")

        self.assertEqual(parts.is_file_split, True)
        self.assertEqual(parts.file_split_size, 300)

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
        self.assertEqual(parts.file_type, GcsfsDirectIngestFileType.RAW_DATA)
        self.assertEqual(parts.file_tag, "CIS_100_CLIENT")
        self.assertEqual(parts.filename_suffix, None)
        self.assertEqual(
            parts.utc_upload_datetime,
            datetime.datetime.fromisoformat("2021-09-21T00:00:00:000000"),
        )
        self.assertEqual(parts.date_str, "2021-09-21")

        self.assertEqual(parts.is_file_split, False)
        self.assertEqual(parts.file_split_size, None)
