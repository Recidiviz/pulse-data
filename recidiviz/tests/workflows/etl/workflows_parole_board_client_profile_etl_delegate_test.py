# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for WorkflowsParoleBoardClientProfileETLDelegate."""

import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.workflows.etl.workflows_parole_board_client_profile_etl_delegate import (
    WorkflowsParoleBoardClientProfileETLDelegate,
)


class WorkflowsParoleBoardClientProfileETLDelegateTest(unittest.TestCase):
    """Tests for WorkflowsParoleBoardClientProfileETLDelegate."""

    def test_supports_filename(self) -> None:
        """Test that the parole_board_client_profile_record file is supported"""
        self.assertTrue(
            WorkflowsParoleBoardClientProfileETLDelegate.supports_file(
                StateCode.US_XX, "parole_board_client_profile_record.json"
            )
        )
        self.assertFalse(
            WorkflowsParoleBoardClientProfileETLDelegate.supports_file(
                StateCode.US_XX, "resident_record.json"
            )
        )

    def test_collection_name(self) -> None:
        """Test that the correct collection_name is set after initialization"""
        delegate = WorkflowsParoleBoardClientProfileETLDelegate(
            StateCode.US_TN, "parole_board_client_profile_record.json"
        )
        self.assertEqual(delegate.collection_name, "paroleBoardClientProfiles")
