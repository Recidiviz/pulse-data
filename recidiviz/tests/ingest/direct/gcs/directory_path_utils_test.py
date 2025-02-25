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
import datetime
from unittest import TestCase, mock

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
    gcsfs_direct_ingest_deprecated_storage_directory_path_for_state,
    gcsfs_direct_ingest_storage_directory_path_for_state,
    gcsfs_direct_ingest_temporary_output_directory_path,
    gcsfs_sftp_download_bucket_path_for_state,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class TestDirectoryPathUtils(TestCase):
    """Tests for directory_path_utils.py."""

    def setUp(self) -> None:
        self.mocck_project_id = "recidiviz-staging"
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.metadata_patcher.start().return_value = self.mocck_project_id

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_get_state_storage_directory_path(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_storage_directory_path_for_state(
                region_code="us_nd",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).abs_path(),
            "recidiviz-staging-direct-ingest-state-storage/us_nd/",
        )

    def test_get_state_storage_directory_path_secondary(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_storage_directory_path_for_state(
                region_code="us_nd",
                ingest_instance=DirectIngestInstance.SECONDARY,
            ).abs_path(),
            "recidiviz-staging-direct-ingest-state-storage-secondary/us_nd/",
        )

    def test_get_state_ingest_bucket_path_for_region(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_bucket_for_state(
                region_code="us_nd",
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).abs_path(),
            "recidiviz-staging-direct-ingest-state-us-nd",
        )

    def test_get_state_ingest_bucket_path_for_region_secondary(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_bucket_for_state(
                region_code="us_nd",
                ingest_instance=DirectIngestInstance.SECONDARY,
            ).abs_path(),
            "recidiviz-staging-direct-ingest-state-us-nd-secondary",
        )

    def test_gcsfs_sftp_download_directory_path_for_region(self) -> None:
        self.assertEqual(
            gcsfs_sftp_download_bucket_path_for_state("us_nd"),
            GcsfsBucketPath("recidiviz-staging-direct-ingest-state-us-nd-sftp"),
        )

    def test_gcsfs_direct_ingest_deprecated_storage_directory_path_for_region(
        self,
    ) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_deprecated_storage_directory_path_for_state(
                region_code="us_nd",
                ingest_instance=DirectIngestInstance.PRIMARY,
                deprecated_on_date=datetime.date(2020, 1, 1),
            ).abs_path(),
            "recidiviz-staging-direct-ingest-state-storage/us_nd/deprecated/deprecated_on_2020-01-01/",
        )

    def test_gcsfs_direct_ingest_temporary_output_directory_path_no_args(self) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_temporary_output_directory_path().abs_path(),
            "recidiviz-staging-direct-ingest-temporary-files",
        )

    def test_gcsfs_direct_ingest_temporary_output_directory_path_custom_project(
        self,
    ) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_temporary_output_directory_path(
                project_id="fake-recidiviz-project"
            ).abs_path(),
            "fake-recidiviz-project-direct-ingest-temporary-files",
        )

    def test_gcsfs_direct_ingest_temporary_output_directory_path_custom_subdir(
        self,
    ) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_temporary_output_directory_path(
                subdir="my-fancy-subdir"
            ).abs_path(),
            "recidiviz-staging-direct-ingest-temporary-files/my-fancy-subdir/",
        )

    def test_gcsfs_direct_ingest_temporary_output_directory_path_custom_subdir_and_project(
        self,
    ) -> None:
        self.assertEqual(
            gcsfs_direct_ingest_temporary_output_directory_path(
                project_id="fake-recidiviz-project", subdir="my-fancy-subdir"
            ).abs_path(),
            "fake-recidiviz-project-direct-ingest-temporary-files/my-fancy-subdir/",
        )
