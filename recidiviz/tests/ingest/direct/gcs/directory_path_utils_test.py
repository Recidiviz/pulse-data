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
"""Tests for directory_path_utils.py."""
from unittest import TestCase

from mock import Mock, patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_region,
    gcsfs_direct_ingest_storage_directory_path_for_region,
    gcsfs_sftp_download_bucket_path_for_region,
)
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class TestDirectoryPathUtils(TestCase):
    """Tests for directory_path_utils.py."""

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
