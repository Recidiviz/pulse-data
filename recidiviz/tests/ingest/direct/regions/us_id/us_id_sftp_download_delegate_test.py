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
"""Unit tests for us_id_sftp_download_delegate.py"""
import unittest

from recidiviz.ingest.direct.regions.us_id.us_id_sftp_download_delegate import UsIdSftpDownloadDelegate


class UsIdSftpDownloadDelegateTest(unittest.TestCase):
    """Unit tests for us_id_sftp_download_delegate.py"""

    def setUp(self) -> None:
        self.delegate = UsIdSftpDownloadDelegate()

    def test_root_directory(self) -> None:
        root_dir = self.delegate.root_directory([])
        self.assertEqual(root_dir, ".")

    def test_filter_paths(self) -> None:
        test_directories = [
            "Recidiviz20200101",
            "Recidiviz20210304",
            "Recdvz2020",
            "Recidiviz2020-01-01",
            "Recidiviz202002",
            "some_other_file.txt"
        ]
        expected_results = [
            "Recidiviz20200101",
            "Recidiviz20210304"
        ]
        results = self.delegate.filter_paths(test_directories)
        self.assertEqual(results, expected_results)
