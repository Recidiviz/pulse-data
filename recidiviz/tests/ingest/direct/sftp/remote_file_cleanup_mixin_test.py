# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for the remote file cleanup mixin."""
from unittest import TestCase
from unittest.mock import create_autospec

import paramiko

from recidiviz.ingest.direct.sftp.remote_file_cleanup_mixin import (
    RemoteFileCleanupMixin,
)


class ClassThatIsMixedIn(RemoteFileCleanupMixin):
    pass


class TestRemoteFileCleanupMixin(TestCase):
    """Unit tests for RemoteFileCleanupMixin"""

    def test_supported_envs(self) -> None:
        mock_sftp_client = create_autospec(paramiko.SFTPClient)

        with self.assertRaisesRegex(
            ValueError,
            r"The RemoteFileCleanupMixin cannot be used for a region that runs in "
            r"more than one project, as it can cause race conditions for file reading. "
            r"Please configure \[RemoteFileCleanupMixin\] to only be supported in a single "
            r"environment",
        ):
            RemoteFileCleanupMixin.remove_remote_file(
                sftp_client=mock_sftp_client,
                remote_path="path/123.csv",
                supported_environments=["recidiviz-testing-1", "recidiviz-testing-2"],
            )

        mock_sftp_client.remove.assert_not_called()

        with self.assertRaisesRegex(
            ValueError,
            r"The RemoteFileCleanupMixin cannot be used for a region that runs in "
            r"more than one project, as it can cause race conditions for file reading. "
            r"Please configure \[ClassThatIsMixedIn\] to only be supported in a single "
            r"environment",
        ):
            ClassThatIsMixedIn.remove_remote_file(
                sftp_client=mock_sftp_client,
                remote_path="path/123.csv",
                supported_environments=["recidiviz-testing-1", "recidiviz-testing-2"],
            )

        mock_sftp_client.remove.assert_not_called()

        RemoteFileCleanupMixin.remove_remote_file(
            sftp_client=mock_sftp_client,
            remote_path="path/123.csv",
            supported_environments=["recidiviz-testing-2"],
        )

        mock_sftp_client.remove.assert_called_with("path/123.csv")
