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
"""Tests for violation_utils.py"""
import datetime
import unittest

from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.persistence.entity.state.violation_utils import (
    collect_unique_violations,
    collect_violation_responses,
)


class TestViolationUtils(unittest.TestCase):
    """Tests for violation_utils.py"""

    def test_collect_violation_responses_empty(self) -> None:
        responses = collect_violation_responses([], StateSupervisionViolationResponse)
        self.assertEqual([], responses)

    def test_collect_violation_responses_single(self) -> None:
        violation_response = StateSupervisionViolationResponse(
            state_code=StateCode.US_XX.value,
            external_id="svr1",
            supervision_violation_response_id=1234,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=datetime.date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[],
        )
        violation = StateSupervisionViolation(
            state_code=StateCode.US_XX.value,
            external_id="sv1",
            supervision_violation_id=4567,
            violation_date=datetime.date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_responses=[violation_response],
        )

        responses = collect_violation_responses(
            [violation], StateSupervisionViolationResponse
        )
        self.assertEqual([violation_response], responses)

    def test_collect_violation_responses_multiple_responses_same_violation(
        self,
    ) -> None:
        violation_response_1 = StateSupervisionViolationResponse(
            state_code=StateCode.US_XX.value,
            external_id="svr1",
            supervision_violation_response_id=1234,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=datetime.date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[],
        )
        violation_response_2 = StateSupervisionViolationResponse(
            state_code=StateCode.US_XX.value,
            external_id="svr2",
            supervision_violation_response_id=1234,
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_date=datetime.date(2021, 1, 5),
            is_draft=False,
            supervision_violation_response_decisions=[],
        )
        violation = StateSupervisionViolation(
            state_code=StateCode.US_XX.value,
            external_id="sv1",
            supervision_violation_id=4567,
            violation_date=datetime.date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_responses=[
                violation_response_1,
                violation_response_2,
            ],
        )

        responses = collect_violation_responses(
            [violation], StateSupervisionViolationResponse
        )
        self.assertEqual([violation_response_1, violation_response_2], responses)

    def test_collect_violation_responses_multiple_violations(self) -> None:
        violation_response_1 = StateSupervisionViolationResponse(
            state_code=StateCode.US_XX.value,
            external_id="svr1",
            supervision_violation_response_id=1234,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=datetime.date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[],
        )
        violation_1 = StateSupervisionViolation(
            state_code=StateCode.US_XX.value,
            external_id="sv1",
            supervision_violation_id=4567,
            violation_date=datetime.date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_responses=[
                violation_response_1,
            ],
        )
        violation_response_2 = StateSupervisionViolationResponse(
            state_code=StateCode.US_XX.value,
            external_id="svr2",
            supervision_violation_response_id=1234,
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_date=datetime.date(2021, 1, 5),
            is_draft=False,
            supervision_violation_response_decisions=[],
        )
        violation_2 = StateSupervisionViolation(
            state_code=StateCode.US_XX.value,
            external_id="sv2",
            supervision_violation_id=45678,
            violation_date=datetime.date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_responses=[
                violation_response_2,
            ],
        )

        responses = collect_violation_responses(
            [violation_1, violation_2], StateSupervisionViolationResponse
        )
        self.assertEqual([violation_response_1, violation_response_2], responses)

    def test_collect_unique_violations_empty(self) -> None:
        violations = collect_unique_violations([])
        self.assertEqual([], violations)

    def test_collect_unique_violations_multiple_responses_same_violation(self) -> None:
        violation_response_1 = NormalizedStateSupervisionViolationResponse(
            state_code=StateCode.US_XX.value,
            external_id="svr1",
            supervision_violation_response_id=1234,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=datetime.date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[],
        )
        violation_response_2 = NormalizedStateSupervisionViolationResponse(
            state_code=StateCode.US_XX.value,
            external_id="svr2",
            supervision_violation_response_id=1234,
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_date=datetime.date(2021, 1, 5),
            is_draft=False,
            supervision_violation_response_decisions=[],
        )
        violation = NormalizedStateSupervisionViolation(
            state_code=StateCode.US_XX.value,
            external_id="sv1",
            supervision_violation_id=4567,
            violation_date=datetime.date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_responses=[
                violation_response_1,
                violation_response_2,
            ],
        )
        violation_response_1.supervision_violation = violation
        violation_response_2.supervision_violation = violation

        violations = collect_unique_violations(
            [violation_response_1, violation_response_2]
        )
        self.assertEqual([violation], violations)

    def test_collect_unique_violations_multiple_responses_different_violations(
        self,
    ) -> None:
        violation_response_1 = NormalizedStateSupervisionViolationResponse(
            state_code=StateCode.US_XX.value,
            external_id="svr1",
            supervision_violation_response_id=1234,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=datetime.date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[],
        )
        violation_1 = NormalizedStateSupervisionViolation(
            state_code=StateCode.US_XX.value,
            external_id="sv1",
            supervision_violation_id=4567,
            violation_date=datetime.date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_responses=[
                violation_response_1,
            ],
        )
        violation_response_2 = NormalizedStateSupervisionViolationResponse(
            state_code=StateCode.US_XX.value,
            external_id="svr2",
            supervision_violation_response_id=1234,
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_date=datetime.date(2021, 1, 5),
            is_draft=False,
            supervision_violation_response_decisions=[],
        )
        violation_2 = NormalizedStateSupervisionViolation(
            state_code=StateCode.US_XX.value,
            external_id="sv2",
            supervision_violation_id=45678,
            violation_date=datetime.date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_responses=[
                violation_response_2,
            ],
        )

        violation_response_1.supervision_violation = violation_1
        violation_response_2.supervision_violation = violation_2

        violations = collect_unique_violations(
            [violation_response_1, violation_response_2]
        )
        self.assertEqual([violation_1, violation_2], violations)
