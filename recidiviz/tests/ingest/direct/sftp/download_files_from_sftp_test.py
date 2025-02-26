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
from typing import List

import pytz
from mock import MagicMock, Mock, patch
from paramiko import RSAKey, SFTPAttributes
from paramiko.hostkeys import HostKeyEntry

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.io.file_contents_handle import FileContentsHandle
from recidiviz.common.sftp_connection import RecidivizSftpConnection
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.sftp.base_sftp_download_delegate import (
    BaseSftpDownloadDelegate,
)
from recidiviz.ingest.direct.sftp.download_files_from_sftp import (
    RAW_INGEST_DIRECTORY,
    DownloadFilesFromSftpController,
    SftpAuth,
)
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem

TODAY = datetime.datetime.fromtimestamp(int(datetime.datetime.today().timestamp()))
YESTERDAY = TODAY - datetime.timedelta(1)
TWO_DAYS_AGO = TODAY - datetime.timedelta(2)
TEST_SSH_RSA_KEY = (
    "AAAAB3NzaC1yc2EAAAADAQABAAABAQCaqqwHqIxyLJwk5ppScpxjGIr9YeGNtWL/Ci0cYKMtUBWrIcosPMnNkyR/"
    "SgtKXMmVDkL1FSFztu1qPY6bO4STWnhQgJCjLwimryOmey9u5V6Rx6E4R0rfT4851oknqRZANNRzMG4Eqh5OgFl4"
    "QtHS19OPq+PcBqG0naca+2SEOztbKSzrZ8tEmVMqHDbgmnqYXVZNNGoW4KEvHW1NgZDZBIPMTOXln1ELKJKNDSEa"
    "jQQZHK0OEZmWJjR4eW6xeepXb//F/xG7Vh809WturazOIKs4YEFdjM6DUIfPxmZhGuya0YSSl1GIkoxo5gPITd1R"
    "usN7l0VM2XgiNpz4tvhH"
)
FIXTURE_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "../controllers/fixtures", "sftp_test"
)


def create_files(
    download_dir: str, remotedir: str, gcsfs: FakeGCSFileSystem
) -> FakeGCSFileSystem:
    gcsfs.test_add_path(
        path=GcsfsDirectoryPath.from_absolute_path(f"{download_dir}/{remotedir}"),
        local_path=None,
    )
    for item in os.listdir(os.path.join(FIXTURE_PATH, remotedir)):
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
    test_today_attr.st_mtime = int(TODAY.timestamp())
    test_today_attr.filename = "testToday"
    test_today_attr.st_mode = stat.S_IFDIR

    test_two_days_ago_attr = SFTPAttributes()
    test_two_days_ago_attr.st_mtime = int(TWO_DAYS_AGO.timestamp())
    test_two_days_ago_attr.filename = "testTwoDaysAgo"
    test_two_days_ago_attr.st_mode = stat.S_IFDIR

    not_test_attr = SFTPAttributes()
    not_test_attr.filename = "nottest.txt"
    not_test_attr.st_mtime = int(YESTERDAY.timestamp())
    not_test_attr.st_mode = stat.S_IFREG

    return [test_today_attr, test_two_days_ago_attr, not_test_attr]


def mock_stat(path: str) -> SFTPAttributes:
    sftp_attr = SFTPAttributes()
    sftp_attr.filename = os.path.relpath(path)
    sftp_attr.st_mtime = int(TODAY.timestamp())
    if ".txt" not in path and ".csv" not in path:
        sftp_attr.st_mode = stat.S_IFDIR
    else:
        sftp_attr.st_mode = stat.S_IFREG
    return sftp_attr


def mock_listdir(remotepath: str = ".") -> List[str]:
    if remotepath == ".":
        return os.listdir(FIXTURE_PATH)
    return os.listdir(os.path.join(FIXTURE_PATH, remotepath))


def mock_listdir_attr(remotepath: str = ".") -> List[SFTPAttributes]:
    if remotepath == ".":
        return create_sftp_attrs()
    return [
        mock_stat(path) for path in os.listdir(os.path.join(FIXTURE_PATH, remotepath))
    ]


class _TestSftpDownloadDelegate(BaseSftpDownloadDelegate):
    def root_directory(self, _: List[str]) -> str:
        return "."

    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        return [path for path in candidate_paths if path.startswith("test")]

    def post_process_downloads(
        self, downloaded_path: GcsfsFilePath, _: GCSFileSystem
    ) -> List[str]:
        return [downloaded_path.abs_path()]


class BrokenGCSFSFakeSystem(FakeGCSFileSystem):
    def upload_from_contents_handle_stream(
        self,
        path: GcsfsFilePath,
        contents_handle: FileContentsHandle,
        content_type: str,
        timeout: int = 60,
    ) -> None:
        if "file1" in path.abs_path():
            raise IOError
        super().upload_from_contents_handle_stream(
            path, contents_handle, content_type, timeout
        )

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
            "us_xx_sftp_port": "2223",
        }
        mock_secret.side_effect = test_secrets.get
        result = SftpAuth.for_region("us_xx")
        self.assertEqual(result.hostname, "testhost.ftp")
        self.assertEqual(result.username, "username")
        self.assertEqual(result.password, "password")
        self.assertEqual(
            result.cnopts.get_hostkey("testhost.ftp"),
            RSAKey(data=decodebytes(bytes(TEST_SSH_RSA_KEY, "utf-8"))),
        )
        self.assertEqual(result.port, 2223)

    @patch("recidiviz.utils.secrets.get_secret")
    def test_initialization_empty_hostname_error(self, mock_secret: MagicMock) -> None:
        test_secrets = {"us_non_existent_host": "somehost"}
        mock_secret.side_effect = test_secrets.get
        with self.assertRaises(ValueError):
            _ = SftpAuth.for_region("us_yy")

    @patch("recidiviz.utils.secrets.get_secret")
    def test_initialization_no_hostkeys_error(self, mock_secret: MagicMock) -> None:
        test_secrets = {
            "us_xx_sftp_host": "testhost.ftp",
            "us_xx_sftp_username": "username",
            "us_xx_sftp_password": "password",
        }
        mock_secret.side_effect = test_secrets.get
        with self.assertRaises(ValueError):
            _ = SftpAuth.for_region("us_xx")

    @patch("recidiviz.utils.secrets.get_secret")
    def test_initialization_no_password_error(self, mock_secret: MagicMock) -> None:
        test_secrets = {
            "us_xx_sftp_host": "testhost.ftp",
            "us_xx_sftp_username": "username",
            "us_xx_sftp_hostkey": "testhost.ftp ssh-rsa " + TEST_SSH_RSA_KEY,
        }
        mock_secret.side_effect = test_secrets.get
        with self.assertRaises(ValueError):
            _ = SftpAuth.for_region("us_xx")

    @patch("recidiviz.utils.secrets.get_secret")
    def test_initialization_unknown_keytype_error(self, mock_secret: MagicMock) -> None:
        test_secrets = {
            "us_xx_sftp_host": "testhost.ftp",
            "us_xx_sftp_username": "username",
            "us_xx_sftp_password": "password",
            "us_xx_sftp_hostkey": "testhost.ftp ssh-nonsense " + TEST_SSH_RSA_KEY,
        }
        mock_secret.side_effect = test_secrets.get
        with self.assertRaises(ValueError):
            _ = SftpAuth.for_region("us_xx")

    @patch("recidiviz.utils.secrets.get_secret")
    def test_initialization_default_port_22(self, mock_secret: MagicMock) -> None:
        test_secrets = {
            "us_xx_sftp_host": "testhost.ftp",
            "us_xx_sftp_username": "username",
            "us_xx_sftp_password": "password",
            "us_xx_sftp_hostkey": "testhost.ftp ssh-rsa " + TEST_SSH_RSA_KEY,
        }
        mock_secret.side_effect = test_secrets.get
        result = SftpAuth.for_region("us_xx")
        self.assertEqual(result.port, 22)


@patch.object(
    SftpDownloadDelegateFactory, "build", return_value=_TestSftpDownloadDelegate()
)
@patch.object(
    SftpAuth,
    "for_region",
    return_value=SftpAuth(
        "testhost.ftp",
        f"testhost.ftp ssh-rsa {TEST_SSH_RSA_KEY}",
        HostKeyEntry(
            ["testhost.ftp"], RSAKey(data=decodebytes(bytes(TEST_SSH_RSA_KEY, "utf-8")))
        ),
        "username",
        "password",
        None,
    ),
)
@patch("recidiviz.ingest.direct.direct_ingest_control.GcsfsFactory.build")
@patch.object(
    PostgresDirectIngestRawFileMetadataManager,
    "has_raw_file_been_processed",
    lambda _, path: "already_processed" in path.abs_path(),
)
@patch.object(
    PostgresDirectIngestRawFileMetadataManager,
    "has_raw_file_been_discovered",
    lambda _, path: "discovered" in path.abs_path(),
)
@patch.object(RecidivizSftpConnection, "_start_transport", lambda _, _host, _port: None)
@patch.object(
    RecidivizSftpConnection,
    "_auth_transport",
    lambda _, _username, _password, _private_key: None,
)
class TestDownloadFilesFromSftpController(unittest.TestCase):
    """Tests for DownloadFilesFromSftpController."""

    def setUp(self) -> None:
        self.lower_bound_date = YESTERDAY
        self.project_id = "recidiviz-456"
        self.region = "us_xx"

        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    @patch.object(
        RecidivizSftpConnection,
        "__enter__",
        return_value=Mock(spec=RecidivizSftpConnection),
    )
    def test_get_paths_to_download(
        self,
        mock_connection: Mock,
        mock_fs_factory: Mock,
        _mock_auth: Mock,
        _mock_download: Mock,
    ) -> None:
        mock_fs = FakeGCSFileSystem()
        mock_fs_factory.return_value = mock_fs

        mock_connection.listdir.side_effect = mock_listdir
        mock_connection.listdir_attr.side_effect = mock_listdir_attr
        mock_connection.stat.side_effect = mock_stat

        controller = DownloadFilesFromSftpController(
            self.project_id, self.region, self.lower_bound_date
        )

        files_with_timestamps = controller.get_paths_to_download(mock_connection)
        expected = [
            ("./testToday/file1.txt", TODAY.astimezone(pytz.UTC)),
            ("./testToday/already_processed.csv", TODAY.astimezone(pytz.UTC)),
            ("./testToday/discovered.csv", TODAY.astimezone(pytz.UTC)),
        ]
        self.assertCountEqual(files_with_timestamps, expected)

    @patch.object(
        RecidivizSftpConnection,
        "__enter__",
        return_value=Mock(
            spec=RecidivizSftpConnection,
            listdir=mock_listdir,
            listdir_attr=mock_listdir_attr,
            stat=mock_stat,
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

        result = controller.do_fetch()
        self.assertCountEqual(
            result.successes,
            [
                (
                    os.path.join(
                        "recidiviz-456-direct-ingest-state-us-xx-sftp",
                        RAW_INGEST_DIRECTORY,
                        TODAY.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S:%f"),
                        "testToday",
                        "file1.txt",
                    ),
                    TODAY.astimezone(pytz.UTC),
                )
            ],
        )
        self.assertEqual(len(mock_fs.files), len(result.successes))

    @patch.object(
        RecidivizSftpConnection,
        "__enter__",
        return_value=Mock(
            spec=RecidivizSftpConnection,
            listdir=mock_listdir,
            listdir_attr=mock_listdir_attr,
            stat=mock_stat,
        ),
    )
    def test_do_fetch_succeeds_with_timezone(
        self,
        _mock_connection: Mock,
        mock_fs_factory: Mock,
        _mock_auth: Mock,
        _mock_download: Mock,
    ) -> None:
        mock_fs = FakeGCSFileSystem()
        mock_fs_factory.return_value = mock_fs

        lower_bound_with_tz = self.lower_bound_date.astimezone(pytz.UTC)
        controller = DownloadFilesFromSftpController(
            self.project_id, self.region, lower_bound_with_tz
        )

        result = controller.do_fetch()
        self.assertCountEqual(
            result.successes,
            [
                (
                    os.path.join(
                        "recidiviz-456-direct-ingest-state-us-xx-sftp",
                        RAW_INGEST_DIRECTORY,
                        TODAY.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S:%f"),
                        "testToday",
                        "file1.txt",
                    ),
                    TODAY.astimezone(pytz.UTC),
                )
            ],
        )
        self.assertEqual(len(mock_fs.files), len(result.successes))

    @patch.object(
        RecidivizSftpConnection,
        "__enter__",
        return_value=Mock(
            spec=RecidivizSftpConnection,
            listdir=lambda _: Exception("SomeError"),
            listdir_attr=mock_listdir_attr,
            stat=mock_stat,
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
        RecidivizSftpConnection,
        "__enter__",
        return_value=Mock(
            spec=RecidivizSftpConnection,
            listdir=mock_listdir,
            listdir_attr=mock_listdir_attr,
            stat=mock_stat,
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
        result = controller.do_fetch()
        self.assertSetEqual(
            set(result.successes),
            set(
                itertools.chain(
                    *[
                        [
                            (
                                os.path.join(
                                    "recidiviz-456-direct-ingest-state-us-xx-sftp",
                                    RAW_INGEST_DIRECTORY,
                                    TODAY.astimezone(pytz.UTC).strftime(
                                        "%Y-%m-%dT%H:%M:%S:%f"
                                    ),
                                    "testToday",
                                    "file1.txt",
                                ),
                                TODAY.astimezone(pytz.UTC),
                            ),
                            (
                                os.path.join(
                                    "recidiviz-456-direct-ingest-state-us-xx-sftp",
                                    RAW_INGEST_DIRECTORY,
                                    TWO_DAYS_AGO.astimezone(pytz.UTC).strftime(
                                        "%Y-%m-%dT%H:%M:%S:%f"
                                    ),
                                    "testTwoDaysAgo",
                                    "file1.txt",
                                ),
                                TWO_DAYS_AGO.astimezone(pytz.UTC),
                            ),
                        ]
                    ]
                )
            ),
        )
        self.assertEqual(len(mock_fs.files), len(result.successes))

    @patch.object(
        RecidivizSftpConnection,
        "__enter__",
        return_value=Mock(
            spec=RecidivizSftpConnection,
            listdir=mock_listdir,
            listdir_attr=mock_listdir_attr,
            stat=mock_stat,
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
        result = controller.do_fetch()
        self.assertEqual(result.failures, ["./testToday/file1.txt"])
        self.assertEqual(len(mock_fs.files), len(result.successes))

    @patch.object(
        RecidivizSftpConnection,
        "__enter__",
        return_value=Mock(
            spec=RecidivizSftpConnection,
            listdir=mock_listdir,
            listdir_attr=mock_listdir_attr,
            stat=mock_stat,
        ),
    )
    def test_do_fetch_skips_already_discovered_or_processed_files(
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
        result = controller.do_fetch()
        self.assertCountEqual(
            result.skipped,
            ["./testToday/already_processed.csv", "./testToday/discovered.csv"],
        )
        self.assertEqual(len(mock_fs.files), len(result.successes))
