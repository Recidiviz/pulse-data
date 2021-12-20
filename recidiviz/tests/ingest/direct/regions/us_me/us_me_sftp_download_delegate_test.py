# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Unit tests for us_me_sftp_download_delegate.py"""
import unittest
from unittest.mock import Mock

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.regions.us_me.us_me_sftp_download_delegate import (
    UsMeSftpDownloadDelegate,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class UsMESftpDownloadDelegateTest(unittest.TestCase):
    """Unit tests for us_me_sftp_download_delegate.py"""

    def setUp(self) -> None:
        self.mock_fs = Mock(return_value=FakeGCSFileSystem())
        self.delegate = UsMeSftpDownloadDelegate()

    def test_root_directory(self) -> None:
        root_dir = self.delegate.root_directory([])
        self.assertEqual(root_dir, "/Users/recidiviz")

    def test_filter_paths(self) -> None:
        test_directories = [
            "Recidiviz_2021-11-23T133308",
            "Recidiviz_2021-11-15T072233",
            "Recdvz2020",
            "Recidiviz2020-01-01",
            "Recidiviz202002",
            "some_other_file.txt",
        ]
        expected_results = [
            "Recidiviz_2021-11-23T133308",
            "Recidiviz_2021-11-15T072233",
        ]
        results = self.delegate.filter_paths(test_directories)
        self.assertEqual(results, expected_results)

    def test_post_process_downloads(self) -> None:
        absolute_path = GcsfsFilePath.from_absolute_path("test_bucket/TEST.CSV")
        renamed_path = GcsfsFilePath.from_absolute_path("test_bucket/TEST.csv")
        result = self.delegate.post_process_downloads(
            absolute_path,
            self.mock_fs,
        )
        self.mock_fs.rename_blob.assert_called_with(absolute_path, renamed_path)
        self.assertEqual(result, ["test_bucket/TEST.csv"])
