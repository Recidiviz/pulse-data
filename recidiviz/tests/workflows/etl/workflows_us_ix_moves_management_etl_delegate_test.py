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
"""Tests for WorkflowsUsIxMovesManagementETLDelegate."""

import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.workflows.etl.workflows_us_ix_moves_management_etl_delegate import (
    WorkflowsUsIxMovesManagementETLDelegate,
)


class WorkflowsUsIxMovesManagementETLDelegateTest(unittest.TestCase):
    """Tests for WorkflowsUsIxMovesManagementETLDelegate."""

    def test_supports_only_the_moves_management_record_file(self) -> None:
        delegate = WorkflowsUsIxMovesManagementETLDelegate(StateCode.US_XX)
        self.assertTrue(delegate.supports_file("us_ix_moves_management_record.json"))
        self.assertFalse(delegate.supports_file("resident_record.json"))

    def test_writes_to_the_moves_management_records_collection(self) -> None:
        delegate = WorkflowsUsIxMovesManagementETLDelegate(StateCode.US_XX)
        self.assertEqual(
            {"us_ix_moves_management_record.json": "usIxMovesManagementRecords"},
            delegate.COLLECTION_BY_FILENAME,
        )
