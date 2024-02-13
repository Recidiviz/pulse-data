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
"""Tests for the backup_manager that handles cloud SQL backups."""
from unittest import TestCase
from unittest.mock import Mock, patch

from recidiviz.backup.backup_manager import await_operation


class TestBackupManager(TestCase):
    """Tests for the backup_manager that handles cloud SQL backups."""

    @patch("recidiviz.backup.backup_manager.get_operation_status")
    def test_await_operation_handles_status(self, mock_status: Mock) -> None:
        """Tests that await operation handles the underlying GCP status correctly."""
        for value in ["DONE", "ERROR", "UNRECOGNIZED"]:
            mock_status.return_value = value
            assert await_operation("project", "operation") == value

        mock_status.return_value = "not gonna see this"
        with self.assertRaisesRegex(
            RuntimeError,
            "Returned an operation status 'not gonna see this' for 'operation'",
        ):
            await_operation("project", "operation")
