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
"""Tests for the CO Edovo earned-time sentence-credit writeback flow.

The classification tests pin down when a person becomes an actionable write; the
run tests pin down the delta/idempotency and read-back control flow around the
(still-stubbed) eOMIS screen mechanics.
"""
import unittest
from typing import Any
from unittest.mock import Mock

from recidiviz.common.constants.states import StateCode
from recidiviz.eomis.client import EomisClient
from recidiviz.eomis.flow import SKIP_ACTION, ResultStatus
from recidiviz.eomis.us_co.sentence_credit_flow import (
    WRITE_ACTION,
    CoSentenceCreditCandidate,
    CoSentenceCreditFlow,
    classify_view_row,
)


def _view_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "OFFENDERID": "0000001",
        "proposed_credit_days": 3,
        "course_ids": ["course-a", "course-b"],
    }
    row.update(overrides)
    return row


def _candidate(**overrides: Any) -> CoSentenceCreditCandidate:
    kwargs: dict[str, Any] = {
        "offender_id": "0000001",
        "action": WRITE_ACTION,
        "reason": "test",
        "target_credit_days": 3,
        "course_ids": ("course-a",),
    }
    kwargs.update(overrides)
    return CoSentenceCreditCandidate(**kwargs)


class _StubbedCoFlow(CoSentenceCreditFlow):
    """A flow whose two eOMIS-touching helpers are replaced with in-memory
    stand-ins, so the delta/verify control flow can be exercised without the
    (unimplemented) CO eOMIS screens."""

    def __init__(self, *, reads: list[int]) -> None:
        super().__init__(bq_view="v", project_id="p", limit=None)
        self._reads = reads
        self.posted_days: list[int] = []

    def _recidiviz_pending_credit_days(
        self, client: EomisClient, offender_id: str
    ) -> int:
        return self._reads.pop(0)

    def _post_pending_credit(
        self, client: EomisClient, candidate: CoSentenceCreditCandidate, days: int
    ) -> None:
        self.posted_days.append(days)


class TestClassifyViewRow(unittest.TestCase):
    """Tests turning a source-view row into a candidate."""

    def test_positive_proposed_credit_is_a_write(self) -> None:
        candidate = classify_view_row(_view_row(proposed_credit_days=3))
        self.assertEqual(candidate.action, WRITE_ACTION)
        self.assertTrue(candidate.is_actionable)
        self.assertEqual(candidate.target_credit_days, 3)
        self.assertEqual(candidate.course_ids, ("course-a", "course-b"))

    def test_zero_proposed_credit_is_skipped(self) -> None:
        candidate = classify_view_row(_view_row(proposed_credit_days=0))
        self.assertEqual(candidate.action, SKIP_ACTION)
        self.assertFalse(candidate.is_actionable)

    def test_null_proposed_credit_is_skipped(self) -> None:
        candidate = classify_view_row(_view_row(proposed_credit_days=None))
        self.assertEqual(candidate.action, SKIP_ACTION)
        self.assertEqual(candidate.target_credit_days, 0)

    def test_null_course_ids_becomes_empty_tuple(self) -> None:
        candidate = classify_view_row(_view_row(course_ids=None))
        self.assertEqual(candidate.course_ids, ())

    def test_offender_id_is_stringified_and_stripped(self) -> None:
        candidate = classify_view_row(_view_row(OFFENDERID="  0000042 "))
        self.assertEqual(candidate.offender_id, "0000042")


class TestFlowProperties(unittest.TestCase):
    """Tests the flow's static contract."""

    def test_properties(self) -> None:
        flow = CoSentenceCreditFlow(bq_view="v", project_id="p", limit=None)
        self.assertEqual(flow.state_code, StateCode.US_CO)
        self.assertIn("CO", flow.flow_name)
        self.assertGreater(flow.max_writes_per_run, 0)


class TestRunCandidate(unittest.TestCase):
    """Tests the delta/idempotency and read-back control flow."""

    def test_non_write_action_raises(self) -> None:
        flow = CoSentenceCreditFlow(bq_view="v", project_id="p", limit=None)
        with self.assertRaisesRegex(ValueError, r"^unexpected action \[skip\]$"):
            flow.run_candidate(
                Mock(spec=EomisClient),
                _candidate(action=SKIP_ACTION),
                commit=False,
            )

    def test_already_synced_is_skipped_without_posting(self) -> None:
        flow = _StubbedCoFlow(reads=[3])
        result = flow.run_candidate(
            Mock(spec=EomisClient), _candidate(target_credit_days=3), commit=True
        )
        self.assertIs(result.status, ResultStatus.SKIPPED)
        self.assertEqual(flow.posted_days, [])

    def test_over_credit_is_surfaced_as_error_without_posting(self) -> None:
        flow = _StubbedCoFlow(reads=[5])
        result = flow.run_candidate(
            Mock(spec=EomisClient), _candidate(target_credit_days=3), commit=True
        )
        self.assertIs(result.status, ResultStatus.ERROR)
        self.assertIn("needs manual review", result.detail)
        self.assertEqual(flow.posted_days, [])

    def test_dry_run_reports_delta_without_posting(self) -> None:
        flow = _StubbedCoFlow(reads=[2])
        result = flow.run_candidate(
            Mock(spec=EomisClient), _candidate(target_credit_days=5), commit=False
        )
        self.assertIs(result.status, ResultStatus.DRY_RUN)
        self.assertIn("[3]", result.detail)
        self.assertEqual(flow.posted_days, [])

    def test_commit_posts_delta_and_verifies_by_readback(self) -> None:
        flow = _StubbedCoFlow(reads=[2, 5])
        result = flow.run_candidate(
            Mock(spec=EomisClient), _candidate(target_credit_days=5), commit=True
        )
        self.assertIs(result.status, ResultStatus.SUCCESS)
        self.assertEqual(flow.posted_days, [3])

    def test_readback_mismatch_is_an_error(self) -> None:
        flow = _StubbedCoFlow(reads=[2, 4])
        result = flow.run_candidate(
            Mock(spec=EomisClient), _candidate(target_credit_days=5), commit=True
        )
        self.assertIs(result.status, ResultStatus.ERROR)
        self.assertEqual(flow.posted_days, [3])

    def test_screen_helpers_are_not_yet_implemented(self) -> None:
        flow = CoSentenceCreditFlow(bq_view="v", project_id="p", limit=None)
        with self.assertRaises(NotImplementedError):
            flow.run_candidate(Mock(spec=EomisClient), _candidate(), commit=False)


if __name__ == "__main__":
    unittest.main()
