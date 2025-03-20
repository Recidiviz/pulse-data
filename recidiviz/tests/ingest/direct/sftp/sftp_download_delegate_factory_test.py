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

            if len(delegate.supported_environments()) > 1:
                raise ValueError(
                    f"The RemoteFileCleanupMixin cannot be used for a region that runs in "
                    f"more than one project, as it can cause race conditions for file reading. "
                    f"Please configure [{state_code.value}] to only be supported in a single "
                    f"environment"
                )

            if delegate.allow_empty_sftp_directory is not True:
                raise ValueError(
                    f"The RemoteFileCleanupMixin will delete all files after they are "
                    f"downloaded; this will almost certainly mean that the SFTP directory "
                    f"will be empty for [{state_code.value}]. Pleas set [allow_empty_sftp_directory] "
                    f"to true on the delegate or create an exemption in this test if you "
                    f"are very confident you should!"
                )
