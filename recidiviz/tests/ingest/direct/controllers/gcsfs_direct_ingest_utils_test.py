# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for gcsfs_direct_ingest_utils.py."""
import datetime
from unittest import TestCase

from mock import Mock, patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
    GcsfsIngestViewExportArgs,
    filename_parts_from_path,
    gcsfs_direct_ingest_bucket_for_region,
    gcsfs_direct_ingest_storage_directory_path_for_region,
    gcsfs_sftp_download_bucket_path_for_region,
)
from recidiviz.ingest.direct.errors import DirectIngestError


class GcsfsDirectIngestUtilsTest(TestCase):
    """Tests for gcsfs_direct_ingest_utils.py."""

    @patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-123"))
    def test_get_county_storage_directory_path(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code="us_tx_brazos",
                system_level=SystemLevel.COUNTY,
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).abs_path(),
            "recidiviz-123-direct-ingest-county-storage/us_tx_brazos",
        )

    @patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-123"))
    def test_get_county_storage_directory_path_secondary(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code="us_tx_brazos",
                system_level=SystemLevel.COUNTY,
                ingest_instance=DirectIngestInstance.SECONDARY,
            ).abs_path(),
            "recidiviz-123-direct-ingest-county-storage-secondary/us_tx_brazos",
        )

    @patch(
        "recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-staging")
    )
    def test_get_state_storage_directory_path(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code="us_nd",
                system_level=SystemLevel.STATE,
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).abs_path(),
            "recidiviz-staging-direct-ingest-state-storage/us_nd",
        )

    @patch(
        "recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-staging")
    )
    def test_get_state_storage_directory_path_secondary(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code="us_nd",
                system_level=SystemLevel.STATE,
                ingest_instance=DirectIngestInstance.SECONDARY,
            ).abs_path(),
            "recidiviz-staging-direct-ingest-state-storage-secondary/us_nd",
        )

    @patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-123"))
    def test_get_county_storage_directory_path_raw(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code="us_tx_brazos",
                system_level=SystemLevel.COUNTY,
                ingest_instance=DirectIngestInstance.PRIMARY,
                file_type=GcsfsDirectIngestFileType.RAW_DATA,
            ).abs_path(),
            "recidiviz-123-direct-ingest-county-storage/us_tx_brazos/raw",
        )

    @patch(
        "recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-staging")
    )
    def test_get_state_storage_directory_path_file_type_raw(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code="us_nd",
                system_level=SystemLevel.STATE,
                ingest_instance=DirectIngestInstance.SECONDARY,
                file_type=GcsfsDirectIngestFileType.RAW_DATA,
            ).abs_path(),
            "recidiviz-staging-direct-ingest-state-storage-secondary/us_nd/raw",
        )

    @patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-123"))
    def test_get_county_ingest_bucket_path_for_region(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_bucket_for_region(
                region_code="us_tx_brazos",
                system_level=SystemLevel.COUNTY,
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).abs_path(),
            "recidiviz-123-direct-ingest-county-us-tx-brazos",
        )

    @patch(
        "recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-staging")
    )
    def test_get_state_ingest_bucket_path_for_region(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_bucket_for_region(
                region_code="us_nd",
                system_level=SystemLevel.STATE,
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).abs_path(),
            "recidiviz-staging-direct-ingest-state-us-nd",
        )

    @patch(
        "recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-staging")
    )
    def test_get_state_ingest_bucket_path_for_region_secondary(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_bucket_for_region(
                region_code="us_nd",
                system_level=SystemLevel.STATE,
                ingest_instance=DirectIngestInstance.SECONDARY,
            ).abs_path(),
            "recidiviz-staging-direct-ingest-state-us-nd-secondary",
        )

    @patch(
        "recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-staging")
    )
    def test_gcsfs_sftp_download_directory_path_for_region(self) -> None:
        self.assertEqual(
            gcsfs_sftp_download_bucket_path_for_region("us_nd", SystemLevel.STATE),
            GcsfsBucketPath("recidiviz-staging-direct-ingest-state-us-nd-sftp"),
        )

    @patch(
        "recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-staging")
    )
    def test_gcsfs_sftp_download_directory_path_fails_for_county(self) -> None:
        sftp_download_bucket = gcsfs_sftp_download_bucket_path_for_region(
            "us_xx_yyyy", SystemLevel.COUNTY
        )
        self.assertEqual(
            sftp_download_bucket,
            GcsfsBucketPath("recidiviz-staging-direct-ingest-county-us-xx-yyyy-sftp"),
        )

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

    def test_gcsfs_ingest_view_export_args(self) -> None:
        dt_lower = datetime.datetime(2019, 1, 22, 11, 22, 33, 444444)
        dt_upper = datetime.datetime(2019, 11, 22, 11, 22, 33, 444444)

        args = GcsfsIngestViewExportArgs(
            ingest_view_name="my_file_tag",
            output_bucket_name="an_ingest_bucket",
            upper_bound_datetime_prev=None,
            upper_bound_datetime_to_export=dt_upper,
        )

        self.assertEqual(
            "ingest_view_export_my_file_tag-an_ingest_bucket-None-2019_11_22_11_22_33_444444",
            args.task_id_tag(),
        )

        args = GcsfsIngestViewExportArgs(
            ingest_view_name="my_file_tag",
            output_bucket_name="an_ingest_bucket",
            upper_bound_datetime_prev=dt_lower,
            upper_bound_datetime_to_export=dt_upper,
        )

        self.assertEqual(
            "ingest_view_export_my_file_tag-an_ingest_bucket-2019_01_22_11_22_33_444444-2019_11_22_11_22_33_444444",
            args.task_id_tag(),
        )
