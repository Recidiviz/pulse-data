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
"""Tests for writeback route helpers."""
from typing import Generator
from unittest import TestCase

import pytest
from flask import Flask

from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.writeback.base import (
    WritebackExecutorInterface,
    WritebackRejectedError,
    WritebackRequestData,
    WritebackStatusTracker,
)
from recidiviz.case_triage.workflows.writeback.route_helpers import handle_writeback


@pytest.fixture(autouse=True)
def app_context() -> Generator[None, None, None]:
    test_app = Flask("test_route_helpers")
    with test_app.app_context():
        yield


class _RecordingStatusTracker(WritebackStatusTracker):
    def __init__(self) -> None:
        self.statuses: list[ExternalSystemRequestStatus] = []

    def set_status(self, status: ExternalSystemRequestStatus) -> None:
        self.statuses.append(status)


class _RaisingExecutor(WritebackExecutorInterface[WritebackRequestData]):
    def __init__(self, error: Exception) -> None:
        super().__init__(WritebackRequestData())
        self.error = error
        self.tracker = _RecordingStatusTracker()

    def execute(self) -> None:
        raise self.error

    def create_status_tracker(self) -> _RecordingStatusTracker:
        return self.tracker

    @property
    def operation_action_description(self) -> str:
        return "Inserting contact note into TOMIS"


class TestHandleWriteback(TestCase):
    """Tests for handle_writeback"""

    def test_rejected_error_marks_failure_without_logging(self) -> None:
        executor = _RaisingExecutor(
            WritebackRejectedError("TOMIS rejected contact note page [1] with A001")
        )

        with self.assertNoLogs(level="ERROR"):
            response = handle_writeback(executor)

        self.assertEqual(500, response.status_code)
        self.assertEqual(
            [
                ExternalSystemRequestStatus.IN_PROGRESS,
                ExternalSystemRequestStatus.FAILURE,
            ],
            executor.tracker.statuses,
        )

    def test_unexpected_error_marks_failure_and_logs(self) -> None:
        executor = _RaisingExecutor(ConnectionError("connection reset"))

        with self.assertLogs(level="ERROR") as logs:
            response = handle_writeback(executor)

        self.assertEqual(500, response.status_code)
        self.assertEqual(
            [
                "ERROR:root:'Inserting contact note into TOMIS' failed: "
                "connection reset"
            ],
            logs.output,
        )
        self.assertEqual(
            [
                ExternalSystemRequestStatus.IN_PROGRESS,
                ExternalSystemRequestStatus.FAILURE,
            ],
            executor.tracker.statuses,
        )
