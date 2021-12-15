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
"""Unit tests for us_pa_sftp_download_delegate.py"""
import unittest

from mock import patch
from mock.mock import MagicMock, Mock

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.io.file_contents_handle import (
    FileContentsHandle,
    FileContentsRowType,
    IoType,
)
from recidiviz.ingest.direct.regions.us_pa.us_pa_sftp_download_delegate import (
    UsPaSftpDownloadDelegate,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


def mock_is_zipfile(_: bytes) -> bool:
    return True


class TestFakeGCSFileSystem(FakeGCSFileSystem):
    def download_as_bytes(self, path: GcsfsFilePath) -> bytes:
        return bytes()

    def upload_from_contents_handle_stream(
        self,
        path: GcsfsFilePath,
        contents_handle: FileContentsHandle[FileContentsRowType, IoType],
        content_type: str,
    ) -> None:
        return None


class UsPaSftpDownloadDelegateTest(unittest.TestCase):
    """Unit tests for us_pa_sftp_download_delegate.py"""

    def setUp(self) -> None:
        self.delegate = UsPaSftpDownloadDelegate()

    def test_root_directory(self) -> None:
        root_dir = self.delegate.root_directory([])
        self.assertEqual(root_dir, ".")

    def test_filter_paths(self) -> None:
        test_directories = [
            "Recidiviz",
            "Recidiviz20210304",
            "Recdvz2020",
            "Recidiviz2020-01-01",
            "Recidiviz202002",
            "Recidiviz-20210901_to_20210908_-_generated_20210908_142035.zip",
            "Recidiviz-20210920_to_20210928_-_generated_20210928_120303.zip",
            "some_other_file.txt",
        ]
        expected_results = [
            "Recidiviz-20210901_to_20210908_-_generated_20210908_142035.zip",
            "Recidiviz-20210920_to_20210928_-_generated_20210928_120303.zip",
        ]
        results = self.delegate.filter_paths(test_directories)
        self.assertEqual(results, expected_results)

    @patch(
        "recidiviz.ingest.direct.regions.us_pa.us_pa_sftp_download_delegate.is_zipfile"
    )
    def test_post_process_downloads(self, mock_zipfile: MagicMock) -> None:
        mock_fs = TestFakeGCSFileSystem()
        mock_fs.test_add_path(
            GcsfsFilePath.from_absolute_path("test_bucket/test.zip"), None
        )
        mock_zipfile.return_value = mock_is_zipfile

        with patch(
            "recidiviz.ingest.direct.regions.us_pa.us_pa_sftp_download_delegate.ZipFile"
        ) as mock_zip:
            mock_ziparchive = Mock()
            mock_ziparchive.return_value.namelist.return_value = [
                "file1.csv",
                "file2.csv",
                "file3.csv",
            ]
            mock_zip.return_value.__enter__ = mock_ziparchive

            result = self.delegate.post_process_downloads(
                GcsfsFilePath.from_absolute_path("test_bucket/test.zip"),
                mock_fs,
            )
            self.assertEqual(
                result,
                [
                    "test_bucket/file1.csv",
                    "test_bucket/file2.csv",
                    "test_bucket/file3.csv",
                ],
            )
