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
import os
import shutil
import unittest
from pathlib import Path
from typing import List

from mock import patch, Mock, MagicMock

import pysftp
from paramiko import SFTPAttributes

from recidiviz.ingest.direct.controllers.download_files_from_sftp import DownloadFilesFromSftpController, SftpAuth, \
    BaseSftpDownloadDelegate

TODAY = datetime.datetime.today()
YESTERDAY = datetime.datetime.today() - datetime.timedelta(1)
TWO_DAYS_AGO = datetime.datetime.today() - datetime.timedelta(2)


def create_files(remotedir: str, localdir: str) -> None:
    if not os.path.exists(localdir):
        os.mkdir(localdir)
    main_directory = os.path.join(localdir, remotedir)
    os.mkdir(main_directory)
    Path(os.path.join(main_directory, "file1.txt")).touch()
    os.mkdir(os.path.join(main_directory, "subdir1"))
    os.mkdir(os.path.join(main_directory, "subdir2"))
    os.mkdir(os.path.join(main_directory, "subdir3"))
    Path(os.path.join(main_directory, "subdir1", "file1.txt"))


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

    return [
        test_today_attr,
        test_two_days_ago_attr,
        not_test_attr
    ]


def mock_get_r(remotedir: str, localdir: str) -> None:
    create_files(remotedir, localdir)


def mock_listdir(_: str=".") -> List[str]:
    return [sftp_attr.filename for sftp_attr in create_sftp_attrs()]


def mock_listdir_attr(_: str=".") -> List[SFTPAttributes]:
    return create_sftp_attrs()


class TestSftpDownloadDelegate(BaseSftpDownloadDelegate):

    def root_directory(self, _: List[str]) -> str:
        return "."

    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        return [path for path in candidate_paths if path.startswith("test")]

    def post_process_downloads(self, _: str) -> None:
        return


class TestSftpAuth(unittest.TestCase):
    """Tests for SftpAuth."""

    @patch("recidiviz.utils.secrets.get_secret")
    def test_initialization(self, mock_secret: MagicMock) -> None:
        test_secrets = {
            "us_xx_sftp_host": "testhost.ftp",
            "us_xx_sftp_username": "username",
            "us_xx_sftp_password": "password"
        }
        mock_secret.side_effect = test_secrets.get
        result = SftpAuth.for_region("xx")
        self.assertEqual(result.hostname, "testhost.ftp")
        self.assertEqual(result.username, "username")
        self.assertEqual(result.password, "password")

    @patch("recidiviz.utils.secrets.get_secret")
    def test_initialization_empty_hostname_error(self, mock_secret: MagicMock) -> None:
        test_secrets = {
            "us_non_existent_host": "somehost"
        }
        mock_secret.side_effect = test_secrets.get
        with self.assertRaises(ValueError):
            _ = SftpAuth.for_region("yy")


class TestDownloadFilesFromSftpController(unittest.TestCase):
    """Tests for DownloadFilesFromSftpController."""

    def setUp(self) -> None:
        self.auth = SftpAuth("testhost.ftp", "username", "password")
        self.delegate = TestSftpDownloadDelegate()
        self.lower_bound_date = YESTERDAY
        self.tempdir = os.path.join(os.path.dirname(__file__), "fixtures", "sftp")

    def tearDown(self) -> None:
        if os.path.isdir(self.tempdir):
            shutil.rmtree(self.tempdir)

    def test_initialization(self) -> None:
        controller_with_local_dir_override = DownloadFilesFromSftpController(self.auth, self.delegate,
                                                                             self.lower_bound_date, "my/local/path")
        self.assertEqual(controller_with_local_dir_override.localdir, "my/local/path")

    @patch.object(
        target=pysftp,
        attribute='Connection',
        autospec=True,
        return_value=Mock(
            spec=pysftp.Connection,
            __enter__=lambda self: self,
            __exit__=lambda *args: None,
            get_r=mock_get_r,
            listdir=mock_listdir,
            listdir_attr=mock_listdir_attr
        )
    )
    def test_do_fetch_succeeds(self, _mock_connection: Mock) -> None:
        controller = DownloadFilesFromSftpController(self.auth, self.delegate, self.lower_bound_date, self.tempdir)
        items = controller.do_fetch()
        self.assertEqual(items, 1)
        self.assertEqual(os.path.exists(os.path.join(self.tempdir, "testToday")), True)
        self.assertEqual(os.path.exists(os.path.join(self.tempdir, "testTwoDaysAgo")), False)

    @patch.object(
        target=pysftp,
        attribute='Connection',
        autospec=True,
        return_value=Mock(
            spec=pysftp.Connection,
            __enter__=lambda self: self,
            __exit__=lambda *args: None,
            get_r=mock_get_r,
            listdir=mock_listdir,
            listdir_attr=mock_listdir_attr
        )
    )
    @patch.object(target=os, attribute='listdir', return_value=FileNotFoundError)
    def test_do_fetch_fails(self, _mock_connection: Mock, _mock_listdir: Mock) -> None:
        controller = DownloadFilesFromSftpController(self.auth, self.delegate, self.lower_bound_date, self.tempdir)
        items = controller.do_fetch()
        self.assertEqual(items, None)

    @patch.object(
        target=pysftp,
        attribute='Connection',
        autospec=True,
        return_value=Mock(
            spec=pysftp.Connection,
            __enter__=lambda self: self,
            __exit__=lambda *args: None,
            get_r=mock_get_r,
            listdir=mock_listdir,
            listdir_attr=mock_listdir_attr
        )
    )
    def test_do_fetch_gets_all_files_if_no_lower_bound_date(self, _mock_connection: Mock) -> None:
        controller = DownloadFilesFromSftpController(self.auth, self.delegate, None, self.tempdir)
        items = controller.do_fetch()
        self.assertEqual(items, 2)
        self.assertEqual(os.path.exists(os.path.join(self.tempdir, "testToday")), True)
        self.assertEqual(os.path.exists(os.path.join(self.tempdir, "testTwoDaysAgo")), True)

    @patch.object(
        target=pysftp,
        attribute='Connection',
        autospec=True,
        return_value=Mock(
            spec=pysftp.Connection,
            __enter__=lambda self: self,
            __exit__=lambda *args: None,
            get_r=lambda remotedir, localdir: IOError if remotedir == "testToday" else mock_get_r(remotedir, localdir),
            listdir=mock_listdir,
            listdir_attr=mock_listdir_attr
        )
    )
    def test_do_fetch_graceful_get_r_failures(self, _mock_connection: Mock) -> None:
        controller = DownloadFilesFromSftpController(self.auth, self.delegate, None, self.tempdir)
        items = controller.do_fetch()
        self.assertEqual(items, 1)
        self.assertEqual(os.path.exists(os.path.join(self.tempdir, "testToday")), False)
        self.assertEqual(os.path.exists(os.path.join(self.tempdir, "testTwoDaysAgo")), True)

    def test_clean_up_succeeds(self) -> None:
        controller = DownloadFilesFromSftpController(self.auth, self.delegate, self.lower_bound_date, self.tempdir)
        create_files("testToday", self.tempdir)
        create_files("testTwoDaysAgo", self.tempdir)
        removed = controller.clean_up()
        self.assertEqual(removed, True)
        self.assertEqual(os.path.exists(os.path.join(self.tempdir, "testToday")), False)
        self.assertEqual(os.path.exists(os.path.join(self.tempdir, "testTwoDaysAgo")), False)
        self.assertEqual(os.path.exists(self.tempdir), False)

    def test_clean_up_fails(self) -> None:
        controller = DownloadFilesFromSftpController(self.auth, self.delegate, self.lower_bound_date, self.tempdir)
        removed = controller.clean_up()
        self.assertEqual(removed, False)
