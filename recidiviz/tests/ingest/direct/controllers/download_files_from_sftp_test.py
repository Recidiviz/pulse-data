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
"""Tests for download_files_from_sftp.py"""
import datetime
import itertools
import os
import stat
import unittest
from base64 import decodebytes
from typing import List, Callable

from mock import patch, Mock, MagicMock

import pysftp
from pysftp import CnOpts
from paramiko import SFTPAttributes, HostKeys, RSAKey

from recidiviz.cloud_storage.content_types import (
    FileContentsHandle,
    FileContentsRowType,
    IoType,
)
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath, GcsfsDirectoryPath
from recidiviz.ingest.direct.base_sftp_download_delegate import BaseSftpDownloadDelegate
from recidiviz.ingest.direct.controllers.download_files_from_sftp import (
    SftpAuth,
    DownloadFilesFromSftpController,
    RAW_INGEST_DIRECTORY,
)
from recidiviz.ingest.direct.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem

TODAY = datetime.datetime.today()
YESTERDAY = datetime.datetime.today() - datetime.timedelta(1)
TWO_DAYS_AGO = datetime.datetime.today() - datetime.timedelta(2)
TEST_SSH_RSA_KEY = (
    "AAAAB3NzaC1yc2EAAAADAQABAAABAQCaqqwHqIxyLJwk5ppScpxjGIr9YeGNtWL/Ci0cYKMtUBWrIcosPMnNkyR/"
    "SgtKXMmVDkL1FSFztu1qPY6bO4STWnhQgJCjLwimryOmey9u5V6Rx6E4R0rfT4851oknqRZANNRzMG4Eqh5OgFl4"
    "QtHS19OPq+PcBqG0naca+2SEOztbKSzrZ8tEmVMqHDbgmnqYXVZNNGoW4KEvHW1NgZDZBIPMTOXln1ELKJKNDSEa"
    "jQQZHK0OEZmWJjR4eW6xeepXb//F/xG7Vh809WturazOIKs4YEFdjM6DUIfPxmZhGuya0YSSl1GIkoxo5gPITd1R"
    "usN7l0VM2XgiNpz4tvhH"
)
INNER_FILE_TREE = [
    "file1.txt",
    "subdir1",
    "subdir2",
    "subdir3",
    "subdir1/file1.txt",
]


def create_files(
    download_dir: str, remotedir: str, gcsfs: FakeGCSFileSystem
) -> FakeGCSFileSystem:
    gcsfs.test_add_path(
        path=GcsfsDirectoryPath.from_absolute_path(f"{download_dir}/{remotedir}"),
        local_path=None,
    )
    for item in INNER_FILE_TREE:
        path_to_add = f"{download_dir}/{remotedir}/{item}"
        if os.path.isdir(item):
            gcsfs.test_add_path(
                path=GcsfsDirectoryPath.from_absolute_path(path_to_add), local_path=None
            )
        else:
            gcsfs.test_add_path(
                path=GcsfsFilePath.from_absolute_path(path_to_add), local_path=None
            )
    return gcsfs


def create_sftp_attrs() -> List[SFTPAttributes]:
    test_today_attr = SFTPAttributes()
    test_today_attr.st_mtime = TODAY.timestamp()
    test_today_attr.filename = "testToday"

    test_two_days_ago_attr = SFTPAttributes()
    test_two_days_ago_attr.st_mtime = TWO_DAYS_AGO.timestamp()
    test_two_days_ago_attr.filename = "testTwoDaysAgo"

    not_test_attr = SFTPAttributes()
    not_test_attr.filename = "nottest.txt"
    not_test_attr.st_mtime = YESTERDAY.timestamp()

    return [test_today_attr, test_two_days_ago_attr, not_test_attr]


def mock_stat(path: str) -> SFTPAttributes:
    sftp_attr = SFTPAttributes()
    sftp_attr.filename = path
    if ".txt" not in path:
        sftp_attr.st_mode = stat.S_IFDIR
    else:
        sftp_attr.st_mode = stat.S_IFREG
    return sftp_attr


def mock_listdir(remotepath: str = ".") -> List[str]:
    if remotepath == ".":
        return [sftp_attr.filename for sftp_attr in create_sftp_attrs()]
    return [f"{remotepath}/{item}" for item in INNER_FILE_TREE]


def mock_listdir_attr(_: str = ".") -> List[SFTPAttributes]:
    return create_sftp_attrs()


def mock_isdir(remotepath: str) -> bool:
    return ".txt" not in remotepath


def mock_init_connection_options(self: CnOpts) -> None:
    self.hostkeys = HostKeys()
    self.hostkeys.add(
        "testhost.ftp",
        "ssh-rsa",
        RSAKey(data=decodebytes(bytes(TEST_SSH_RSA_KEY, "utf-8"))),
    )


def mock_walktree(
    remotepath: str,
    fcallback: Callable[[str], None],
    dcallback: Callable[[str], None],
    ucallback: Callable[[str], None],
    recurse: bool,
) -> None:
    # pylint: disable=unused-argument
    for item in mock_listdir(remotepath):
        if mock_isdir(item):
            dcallback(item)
        else:
            fcallback(item)


class TestSftpDownloadDelegate(BaseSftpDownloadDelegate):
    def root_directory(self, _: List[str]) -> str:
        return "."

    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        return [path for path in candidate_paths if path.startswith("test")]

    def post_process_downloads(
        self, downloaded_path: GcsfsFilePath, _: GCSFileSystem
    ) -> str:
        return downloaded_path.abs_path()


class BrokenGCSFSFakeSystem(FakeGCSFileSystem):
    def upload_from_contents_handle_stream(
        self,
        path: GcsfsFilePath,
        contents_handle: FileContentsHandle[FileContentsRowType, IoType],
        content_type: str,
    ) -> None:
        if "subdir1" in path.abs_path():
            raise IOError
        super().upload_from_contents_handle_stream(path, contents_handle, content_type)

    def delete(self, path: GcsfsFilePath) -> None:
        raise ValueError


class TestSftpAuth(unittest.TestCase):
    """Tests for SftpAuth."""

    @patch("recidiviz.utils.secrets.get_secret")
    def test_initialization(self, mock_secret: MagicMock) -> None:
        test_secrets = {
            "us_xx_sftp_host": "testhost.ftp",
            "us_xx_sftp_username": "username",
            "us_xx_sftp_password": "password",
            "us_xx_sftp_hostkey": "testhost.ftp ssh-rsa " + TEST_SSH_RSA_KEY,
        }
        mock_secret.side_effect = test_secrets.get
        result = SftpAuth.for_region("us_xx")
        self.assertEqual(result.hostname, "testhost.ftp")
        self.assertEqual(result.username, "username")
        self.assertEqual(result.password, "password")
        self.assertTrue(
            result.connection_options.hostkeys.lookup("testhost.ftp") is not None
        )

    @patch("recidiviz.utils.secrets.get_secret")
    def test_initialization_empty_hostname_error(self, mock_secret: MagicMock) -> None:
        test_secrets = {"us_non_existent_host": "somehost"}
        mock_secret.side_effect = test_secrets.get
        with self.assertRaises(ValueError):
            _ = SftpAuth.for_region("us_yy")

    @patch("recidiviz.utils.secrets.get_secret")
    def test_initialization_connection_options_error(
        self, mock_secret: MagicMock
    ) -> None:
        test_secrets = {
            "us_xx_sftp_host": "testhost.ftp",
            "us_xx_sftp_username": "username",
            "us_xx_sftp_password": "password",
        }
        mock_secret.side_effect = test_secrets.get
        with self.assertRaises(ValueError):
            _ = SftpAuth.for_region("us_xx")

    @patch("recidiviz.utils.secrets.get_secret")
    def test_set_up_connection_options_succeeds(self, mock_secret: MagicMock) -> None:
        test_secrets = {
            "us_xx_sftp_hostkey": "testhost.ftp ssh-rsa " + TEST_SSH_RSA_KEY
        }
        mock_secret.side_effect = test_secrets.get
        result = SftpAuth.set_up_connection_options("us_xx_sftp", "testhost.ftp")
        self.assertTrue(result.hostkeys.lookup("testhost.ftp") is not None)
        mock_secret.assert_called_with("us_xx_sftp_hostkey")

    @patch("recidiviz.utils.secrets.get_secret")
    def test_set_up_connection_options_empty_hostkey_error(
        self, mock_secret: MagicMock
    ) -> None:
        test_secrets = {
            "us_xx_sftp_host": "testhost.ftp",
            "us_non_existent_hostkey": "somehost.ftp ssh-rsa someNonKey",
        }
        mock_secret.side_effect = test_secrets.get
        with self.assertRaises(ValueError):
            _ = SftpAuth.set_up_connection_options("us_xx_sftp", "testhost.ftp")

    @patch("recidiviz.utils.secrets.get_secret")
    @patch.object(CnOpts, "__init__", mock_init_connection_options)
    def test_set_up_connection_options_uses_known_hosts_first(
        self, mock_secret: MagicMock
    ) -> None:
        test_secrets = {
            "us_xx_sftp_hostkey": "testhost.ftp ssh-rsa " + TEST_SSH_RSA_KEY
        }
        mock_secret.side_effect = test_secrets.get
        result = SftpAuth.set_up_connection_options("us_xx_sftp", "testhost.ftp")
        self.assertTrue(result.hostkeys.lookup("testhost.ftp") is not None)
        mock_secret.assert_not_called()


@patch.object(
    SftpDownloadDelegateFactory, "build", return_value=TestSftpDownloadDelegate()
)
@patch.object(
    SftpAuth,
    "for_region",
    return_value=SftpAuth("testhost.ftp", "username", "password", CnOpts()),
)
@patch("recidiviz.ingest.direct.direct_ingest_control.GcsfsFactory.build")
class TestDownloadFilesFromSftpController(unittest.TestCase):
    """Tests for DownloadFilesFromSftpController."""

    def setUp(self) -> None:
        self.lower_bound_date = YESTERDAY
        self.project_id = "recidiviz-456"
        self.region = "us_xx"

    @patch.object(
        target=pysftp,
        attribute="Connection",
        autospec=True,
        return_value=Mock(
            spec=pysftp.Connection,
            __enter__=lambda self: self,
            __exit__=lambda *args: None,
            listdir=mock_listdir,
            listdir_attr=mock_listdir_attr,
            isdir=mock_isdir,
            walktree=mock_walktree,
        ),
    )
    def test_get_paths_to_download(
        self,
        _mock_connection: Mock,
        mock_fs_factory: Mock,
        _mock_auth: Mock,
        _mock_download: Mock,
    ) -> None:
        mock_fs = FakeGCSFileSystem()
        mock_fs_factory.return_value = mock_fs

        controller = DownloadFilesFromSftpController(
            self.project_id, self.region, self.lower_bound_date
        )

        files_with_timestamps = controller.get_paths_to_download()
        expected = [
            ("testToday/file1.txt", TODAY),
            ("testToday/subdir1/file1.txt", TODAY),
        ]
        self.assertListEqual(files_with_timestamps, expected)

    @patch.object(
        target=pysftp,
        attribute="Connection",
        autospec=True,
        return_value=Mock(
            spec=pysftp.Connection,
            __enter__=lambda self: self,
            __exit__=lambda *args: None,
            listdir=mock_listdir,
            listdir_attr=mock_listdir_attr,
            isdir=mock_isdir,
            walktree=mock_walktree,
        ),
    )
    def test_do_fetch_succeeds(
        self,
        _mock_connection: Mock,
        mock_fs_factory: Mock,
        _mock_auth: Mock,
        _mock_download: Mock,
    ) -> None:
        mock_fs = FakeGCSFileSystem()
        mock_fs_factory.return_value = mock_fs

        controller = DownloadFilesFromSftpController(
            self.project_id, self.region, self.lower_bound_date
        )

        items, _ = controller.do_fetch()
        self.assertListEqual(
            items,
            [
                (
                    os.path.join(
                        "recidiviz-456-direct-ingest-state-us-xx-sftp",
                        RAW_INGEST_DIRECTORY,
                        "testToday",
                        item,
                    ),
                    TODAY,
                )
                for item in INNER_FILE_TREE
                if ".txt" in item
            ],
        )
        self.assertEqual(len(mock_fs.files), len(items))

    @patch.object(
        target=pysftp,
        attribute="Connection",
        autospec=True,
        return_value=Mock(
            spec=pysftp.Connection,
            __enter__=lambda _: Exception("Connection failed"),
            __exit__=lambda *args: None,
        ),
    )
    def test_do_fetch_fails(
        self,
        _mock_connection: Mock,
        mock_fs_factory: Mock,
        _mock_auth: Mock,
        _mock_download: Mock,
    ) -> None:
        mock_fs = FakeGCSFileSystem()
        mock_fs_factory.return_value = mock_fs

        controller = DownloadFilesFromSftpController(
            self.project_id, self.region, self.lower_bound_date
        )
        with self.assertRaises(Exception):
            _ = controller.do_fetch()

    @patch.object(
        target=pysftp,
        attribute="Connection",
        autospec=True,
        return_value=Mock(
            spec=pysftp.Connection,
            __enter__=lambda self: self,
            __exit__=lambda *args: None,
            listdir=mock_listdir,
            listdir_attr=mock_listdir_attr,
            isdir=mock_isdir,
            walktree=mock_walktree,
        ),
    )
    def test_do_fetch_gets_all_files_if_no_lower_bound_date(
        self,
        _mock_connection: Mock,
        mock_fs_factory: Mock,
        _mock_auth: Mock,
        _mock_download: Mock,
    ) -> None:
        mock_fs = FakeGCSFileSystem()
        mock_fs_factory.return_value = mock_fs

        controller = DownloadFilesFromSftpController(self.project_id, self.region, None)
        items, _ = controller.do_fetch()
        self.assertSetEqual(
            set(items),
            set(
                itertools.chain(
                    *[
                        [
                            (
                                os.path.join(
                                    "recidiviz-456-direct-ingest-state-us-xx-sftp",
                                    RAW_INGEST_DIRECTORY,
                                    "testToday",
                                    item,
                                ),
                                TODAY,
                            ),
                            (
                                os.path.join(
                                    "recidiviz-456-direct-ingest-state-us-xx-sftp",
                                    RAW_INGEST_DIRECTORY,
                                    "testTwoDaysAgo",
                                    item,
                                ),
                                TWO_DAYS_AGO,
                            ),
                        ]
                        for item in INNER_FILE_TREE
                        if ".txt" in item
                    ]
                )
            ),
        )
        self.assertEqual(len(mock_fs.files), len(items))

    @patch.object(
        target=pysftp,
        attribute="Connection",
        autospec=True,
        return_value=Mock(
            spec=pysftp.Connection,
            __enter__=lambda self: self,
            __exit__=lambda *args: None,
            listdir=mock_listdir,
            listdir_attr=mock_listdir_attr,
            isdir=mock_isdir,
            walktree=mock_walktree,
        ),
    )
    def test_do_fetch_graceful_file_failures(
        self,
        _mock_connection: Mock,
        mock_fs_factory: Mock,
        _mock_auth: Mock,
        _mock_download: Mock,
    ) -> None:
        mock_fs = BrokenGCSFSFakeSystem()
        mock_fs_factory.return_value = mock_fs
        controller = DownloadFilesFromSftpController(
            self.project_id, self.region, self.lower_bound_date
        )
        items, _ = controller.do_fetch()
        self.assertEqual(
            items,
            [
                (
                    os.path.join(
                        "recidiviz-456-direct-ingest-state-us-xx-sftp",
                        RAW_INGEST_DIRECTORY,
                        "testToday",
                        "file1.txt",
                    ),
                    TODAY,
                )
            ],
        )
        self.assertEqual(len(mock_fs.files), len(items))

    def test_clean_up_succeeds(
        self, mock_fs_factory: Mock, _mock_auth: Mock, _mock_download: Mock
    ) -> None:
        mock_fs = create_files(
            download_dir=f"recidiviz-456-direct-ingest-state-us-xx-sftp/{RAW_INGEST_DIRECTORY}",
            remotedir="testToday",
            gcsfs=FakeGCSFileSystem(),
        )
        mock_fs_factory.return_value = mock_fs

        controller = DownloadFilesFromSftpController(
            self.project_id, self.region, self.lower_bound_date
        )
        controller.clean_up()
        items = mock_fs.ls_with_blob_prefix(
            bucket_name="recidiviz-456-direct-ingest-state-us-xx",
            blob_prefix=RAW_INGEST_DIRECTORY,
        )
        self.assertEqual(0, len(items))

    def test_clean_up_fails_gracefully(
        self, mock_fs_factory: Mock, _mock_auth: Mock, _mock_download: Mock
    ) -> None:
        mock_fs = BrokenGCSFSFakeSystem()
        mock_fs_factory.return_value = mock_fs

        controller = DownloadFilesFromSftpController(
            self.project_id, self.region, self.lower_bound_date
        )
        try:
            controller.clean_up()
        except BaseException:
            self.fail("clean_up should not raise exceptions.")
