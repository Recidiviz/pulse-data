# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Unit tests for the SFTP download delegates."""

import unittest
from unittest.mock import create_autospec

import paramiko

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.sftp.remote_file_cleanup_mixin import (
    RemoteFileCleanupMixin,
)
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)


class SftpDownloadDelegateTest(unittest.TestCase):
    """Tests for the SFTP download delegates."""

    def test_every_sftp_download_delegate_uses_absolute_paths(self) -> None:
        for state_code in StateCode:
            try:
                delegate = SftpDownloadDelegateFactory.build(
                    region_code=state_code.value
                )
            except ValueError:
                continue
            self.assertTrue(delegate.root_directory([]).startswith("/"))

    def test_all_delegates_with_remote_file_cleanup_mixin_only_enabled_for_one_project(
        self,
    ) -> None:
        """Tests that all sftp download delegates"""
        for state_code in StateCode:
            try:
                delegate = SftpDownloadDelegateFactory.build(
                    region_code=state_code.value
                )
            except ValueError:
                continue

            if not issubclass(delegate.__class__, RemoteFileCleanupMixin):
                continue

            mock_sftp_client = create_autospec(paramiko.SFTPClient)
            delegate.post_download_actions(
                sftp_client=mock_sftp_client, remote_path="123"
            )
