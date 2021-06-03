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
"""Tests for violation/identifier.py"""
import unittest
from datetime import date

from recidiviz.calculator.pipeline.violation import identifier
from recidiviz.calculator.pipeline.violation.violation_event import (
    ViolationWithResponseEvent,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)


class TestFindViolationEvents(unittest.TestCase):
    """Tests the find_violation_events function."""

    def test_find_violation_events(self) -> None:
        violation_type = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.FELONY,
        )
        violation_decision = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_XX",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision],
        )
        violation = StateSupervisionViolation.new_with_defaults(
            state_code="US_XX",
            supervision_violation_id=1,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[violation_type],
            supervision_violation_responses=[violation_response],
        )
        violation_decision.supervision_violation_response = violation_response
        violation_response.supervision_violation = violation
        violation_type.supervision_violation = violation

        violation_events = identifier.find_violation_events([violation])

        expected = [
            ViolationWithResponseEvent(
                state_code="US_XX",
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.FELONY,
                violation_type_subtype="FELONY",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        ]
        self.assertEqual(expected, violation_events)

    def test_find_violation_events_no_violations(self) -> None:
        violation_events = identifier.find_violation_events([])

        self.assertEqual([], violation_events)


class TestFindViolationWithResponseEvents(unittest.TestCase):
    """Tests the find_violation_with_response_events function."""

    def setUp(self) -> None:
        self.violation_type = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        self.violation_decision = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            )
        )
        self.violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_XX",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[self.violation_decision],
        )
        self.violation = StateSupervisionViolation.new_with_defaults(
            state_code="US_XX",
            supervision_violation_id=1,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[self.violation_type],
            supervision_violation_responses=[self.violation_response],
        )
        self.violation_decision.supervision_violation_response = self.violation_response
        self.violation_response.supervision_violation = self.violation
        self.violation_type.supervision_violation = self.violation

    def test_find_violation_with_response_events(self) -> None:
        violation_with_response_events = identifier.find_violation_with_response_events(
            self.violation
        )

        expected = [
            ViolationWithResponseEvent(
                state_code="US_XX",
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="TECHNICAL",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            )
        ]

        self.assertEqual(expected, violation_with_response_events)

    def test_find_violation_with_response_events_needs_violation_id(
        self,
    ) -> None:
        self.violation.supervision_violation_id = None

        with self.assertRaises(ValueError):
            _ = identifier.find_violation_with_response_events(self.violation)

    def test_find_violation_with_response_events_needs_response_date(
        self,
    ) -> None:
        self.violation_response.response_date = None

        violation_with_response_events = identifier.find_violation_with_response_events(
            self.violation
        )
        self.assertEqual([], violation_with_response_events)

    def test_find_violation_with_response_events_includes_all_violation_types(
        self,
    ) -> None:
        violation_type_1 = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_type_2 = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.ABSCONDED,
        )
        violation_decision = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            )
        )
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_XX",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision],
        )
        violation = StateSupervisionViolation.new_with_defaults(
            state_code="US_XX",
            supervision_violation_id=1,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[violation_type_1, violation_type_2],
            supervision_violation_responses=[violation_response],
        )
        violation_decision.supervision_violation_response = violation_response
        violation_response.supervision_violation = violation
        violation_type_1.supervision_violation = violation
        violation_type_2.supervision_violation = violation

        expected = [
            ViolationWithResponseEvent(
                state_code="US_XX",
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.ABSCONDED,
                violation_type_subtype="ABSCONDED",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
            ViolationWithResponseEvent(
                state_code="US_XX",
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="TECHNICAL",
                is_most_severe_violation_type=False,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
        ]

        violation_with_response_events = identifier.find_violation_with_response_events(
            violation
        )

        self.assertEqual(expected, violation_with_response_events)

    def test_find_violation_with_response_events_filters_permanent_decisions(
        self,
    ) -> None:
        violation_type = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_decision_non_perm = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            )
        )
        violation_response_non_perm = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_XX",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2021, 1, 4),
                is_draft=False,
                supervision_violation_response_decisions=[violation_decision_non_perm],
            )
        )
        violation_decision_perm = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
            )
        )
        violation_response_perm = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_XX",
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_date=date(2021, 1, 5),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision_perm],
        )
        violation = StateSupervisionViolation.new_with_defaults(
            state_code="US_XX",
            supervision_violation_id=1,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[
                violation_type,
            ],
            supervision_violation_responses=[
                violation_response_non_perm,
                violation_response_perm,
            ],
        )
        violation_decision_perm.supervision_violation_response = violation_response_perm
        violation_decision_non_perm.supervision_violation_response = (
            violation_response_non_perm
        )
        violation_response_non_perm.supervision_violation = violation
        violation_response_perm.supervision_violation = violation
        violation_type.supervision_violation = violation

        expected = [
            ViolationWithResponseEvent(
                state_code="US_XX",
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="TECHNICAL",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
        ]

        violation_with_response_events = identifier.find_violation_with_response_events(
            violation
        )

        self.assertEqual(expected, violation_with_response_events)

    def test_find_violation_with_response_events_filters_draft_responses(
        self,
    ) -> None:
        violation_type = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_decision_non_draft = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            )
        )
        violation_response_non_draft = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_XX",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2021, 1, 4),
                is_draft=False,
                supervision_violation_response_decisions=[violation_decision_non_draft],
            )
        )
        violation_decision_draft = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
            )
        )
        violation_response_draft = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_XX",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 3),
            is_draft=True,
            supervision_violation_response_decisions=[violation_decision_draft],
        )
        violation = StateSupervisionViolation.new_with_defaults(
            state_code="US_XX",
            supervision_violation_id=1,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[
                violation_type,
            ],
            supervision_violation_responses=[
                violation_response_non_draft,
                violation_response_draft,
            ],
        )
        violation_decision_draft.supervision_violation_response = (
            violation_response_draft
        )
        violation_decision_non_draft.supervision_violation_response = (
            violation_response_non_draft
        )
        violation_response_non_draft.supervision_violation = violation
        violation_response_draft.supervision_violation = violation
        violation_type.supervision_violation = violation

        expected = [
            ViolationWithResponseEvent(
                state_code="US_XX",
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="TECHNICAL",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
        ]

        violation_with_response_events = identifier.find_violation_with_response_events(
            violation
        )

        self.assertEqual(expected, violation_with_response_events)

    def test_find_violation_with_response_events_takes_first_response(
        self,
    ) -> None:
        violation_type = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_decision_1 = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            )
        )
        violation_response_1 = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_XX",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision_1],
        )
        violation_decision_2 = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
            )
        )
        violation_response_2 = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_XX",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 5),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision_2],
        )
        violation = StateSupervisionViolation.new_with_defaults(
            state_code="US_XX",
            supervision_violation_id=1,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[
                violation_type,
            ],
            supervision_violation_responses=[
                violation_response_1,
                violation_response_2,
            ],
        )
        violation_decision_1.supervision_violation_response = violation_response_1
        violation_decision_2.supervision_violation_response = violation_response_2
        violation_response_1.supervision_violation = violation
        violation_response_2.supervision_violation = violation
        violation_type.supervision_violation = violation

        expected = [
            ViolationWithResponseEvent(
                state_code="US_XX",
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="TECHNICAL",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
        ]
        violation_with_response_events = identifier.find_violation_with_response_events(
            violation
        )

        self.assertEqual(expected, violation_with_response_events)

    def test_find_violation_with_response_events_takes_most_severe_decision(
        self,
    ) -> None:
        violation_type = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.FELONY,
        )
        violation_decision_1 = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                decision=StateSupervisionViolationResponseDecision.REVOCATION,
            )
        )
        violation_decision_2 = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_XX",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[
                violation_decision_1,
                violation_decision_2,
            ],
        )
        violation = StateSupervisionViolation.new_with_defaults(
            state_code="US_XX",
            supervision_violation_id=1,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[
                violation_type,
            ],
            supervision_violation_responses=[
                violation_response,
            ],
        )
        violation_decision_1.supervision_violation_response = violation_response
        violation_decision_2.supervision_violation_response = violation_response
        violation_response.supervision_violation = violation
        violation_type.supervision_violation = violation

        expected = [
            ViolationWithResponseEvent(
                state_code="US_XX",
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.FELONY,
                violation_type_subtype="FELONY",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            ),
        ]

        violation_with_response_events = identifier.find_violation_with_response_events(
            violation
        )

        self.assertEqual(expected, violation_with_response_events)


class TestUsMoFindViolationWithResponseEvents(unittest.TestCase):
    """Tests the find_violation_with_response_events function for US_MO."""

    def setUp(self) -> None:
        self.state_code = "US_MO"

    def test_find_violation_with_response_events_populates_violation_subtypes_correctly_for_conditions(
        self,
    ) -> None:
        violation_type_technical = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=self.state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_type_absconded = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=self.state_code,
            violation_type=StateSupervisionViolationType.ABSCONDED,
        )
        violation_decision = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=self.state_code,
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            )
        )
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=self.state_code,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="INI",
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision],
        )
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code,
            supervision_violation_id=1,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[
                violation_type_technical,
                violation_type_absconded,
            ],
            supervision_violation_responses=[violation_response],
        )
        violation_type_absconded.supervision_violation = violation
        violation_type_technical.supervision_violation = violation
        violation_decision.supervision_violation_response = violation_response
        violation_response.supervision_violation = violation

        violation_with_response_events = identifier.find_violation_with_response_events(
            violation
        )

        expected = [
            ViolationWithResponseEvent(
                state_code=self.state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.ABSCONDED,
                violation_type_subtype="ABSCONDED",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
            ViolationWithResponseEvent(
                state_code=self.state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="TECHNICAL",
                is_most_severe_violation_type=False,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
        ]

        self.assertEqual(expected, violation_with_response_events)

    def test_find_violation_with_response_events_populates_violation_subtypes_correctly_for_special_conditions(
        self,
    ) -> None:
        violation_type_technical = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=self.state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_type_absconded = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=self.state_code,
            violation_type=StateSupervisionViolationType.ABSCONDED,
        )
        violation_condition_law = (
            StateSupervisionViolatedConditionEntry.new_with_defaults(
                state_code=self.state_code,
                condition="law_citation",
            )
        )
        violation_condition_sub = (
            StateSupervisionViolatedConditionEntry.new_with_defaults(
                state_code=self.state_code, condition="substance_abuse"
            )
        )
        violation_decision = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=self.state_code,
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            )
        )
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=self.state_code,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="INI",
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision],
        )
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code,
            supervision_violation_id=1,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[
                violation_type_technical,
                violation_type_absconded,
            ],
            supervision_violation_responses=[violation_response],
            supervision_violated_conditions=[
                violation_condition_law,
                violation_condition_sub,
            ],
        )
        violation_type_absconded.supervision_violation = violation
        violation_type_technical.supervision_violation = violation
        violation_decision.supervision_violation_response = violation_response
        violation_response.supervision_violation = violation

        violation_with_response_events = identifier.find_violation_with_response_events(
            violation
        )

        expected = [
            ViolationWithResponseEvent(
                state_code=self.state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="LAW_CITATION",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
            ViolationWithResponseEvent(
                state_code=self.state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.ABSCONDED,
                violation_type_subtype="ABSCONDED",
                is_most_severe_violation_type=False,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
            ViolationWithResponseEvent(
                state_code=self.state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="SUBSTANCE_ABUSE",
                is_most_severe_violation_type=False,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
        ]

        self.assertEqual(expected, violation_with_response_events)

    def test_find_violation_with_response_events_removes_supplemental_violations_and_others(
        self,
    ) -> None:
        violation_type = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=self.state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_decision = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=self.state_code,
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            )
        )
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=self.state_code,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision],
        )
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code,
            supervision_violation_id=1,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[violation_type],
            supervision_violation_responses=[violation_response],
        )
        violation_type.supervision_violation = violation
        violation_decision.supervision_violation_response = violation_response
        violation_response.supervision_violation = violation

        for subtype in ["SUP", "HOF", "MOS", "ORI"]:
            violation_response.response_subtype = subtype
            violation_with_response_events = (
                identifier.find_violation_with_response_events(violation)
            )
            self.assertEqual([], violation_with_response_events)

    def test_find_violation_with_response_events_handles_citations_correctly(
        self,
    ) -> None:
        violated_condition = StateSupervisionViolatedConditionEntry.new_with_defaults(
            state_code=self.state_code, condition="LAW"
        )
        violation_response_citation = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                response_type=StateSupervisionViolationResponseType.CITATION,
                response_date=date(2021, 1, 4),
                is_draft=False,
            )
        )
        violation_with_no_types = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code,
            supervision_violation_id=1,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_responses=[violation_response_citation],
            supervision_violated_conditions=[violated_condition],
        )
        violation_response_citation.supervision_violation = violation_with_no_types
        violated_condition.supervision_violation = violation_with_no_types

        expected = [
            ViolationWithResponseEvent(
                state_code=self.state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="LAW_CITATION",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=None,
            )
        ]

        violation_with_response_events = identifier.find_violation_with_response_events(
            violation_with_no_types
        )

        self.assertEqual(expected, violation_with_response_events)


class TestUsPaFindViolationWithResponseEvents(unittest.TestCase):
    """Tests the find_violation_with_response_events function for US_PA."""

    def setUp(self) -> None:
        self.state_code = "US_PA"

    def test_find_violation_with_response_events_populates_violation_subtypes_correctly_for_technical(
        self,
    ) -> None:
        violation_type_low = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=self.state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="L01",
        )
        violation_type_med = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=self.state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="M01",
        )
        violation_type_high = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=self.state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="H01",
        )
        violation_type_sub = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=self.state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="H03",
        )
        violation_type_elec = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=self.state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="M16",
        )
        violation_decision = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=self.state_code,
                decision=StateSupervisionViolationResponseDecision.REVOCATION,
            )
        )
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=self.state_code,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision],
        )
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code,
            supervision_violation_id=1,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[
                violation_type_low,
                violation_type_med,
                violation_type_high,
                violation_type_sub,
                violation_type_elec,
            ],
            supervision_violation_responses=[violation_response],
        )
        violation_type_low.supervision_violation = violation
        violation_type_med.supervision_violation = violation
        violation_type_high.supervision_violation = violation
        violation_type_sub.supervision_violation = violation
        violation_type_elec.supervision_violation = violation
        violation_decision.supervision_violation_response = violation_response
        violation_response.supervision_violation = violation

        expected = [
            ViolationWithResponseEvent(
                state_code=self.state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="HIGH_TECH",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            ),
            ViolationWithResponseEvent(
                state_code=self.state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="SUBSTANCE_ABUSE",
                is_most_severe_violation_type=False,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            ),
            ViolationWithResponseEvent(
                state_code=self.state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="ELEC_MONITORING",
                is_most_severe_violation_type=False,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            ),
            ViolationWithResponseEvent(
                state_code=self.state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="MED_TECH",
                is_most_severe_violation_type=False,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            ),
            ViolationWithResponseEvent(
                state_code=self.state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                response_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="LOW_TECH",
                is_most_severe_violation_type=False,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            ),
        ]

        violation_with_response_events = identifier.find_violation_with_response_events(
            violation
        )
        self.assertEqual(expected, violation_with_response_events)


class TestUsNdFindViolationWithResponseEvents(unittest.TestCase):
    """Tests the find_violation_with_response_events function for US_ND."""

    def setUp(self) -> None:
        self.state_code = "US_ND"

    def test_find_violation_with_response_events_uses_permanent_decision_only(
        self,
    ) -> None:
        violation_type = StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=self.state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_decision_non_perm = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=self.state_code,
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            )
        )
        violation_response_non_perm = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2021, 1, 4),
                is_draft=False,
                supervision_violation_response_decisions=[violation_decision_non_perm],
            )
        )
        violation_decision_perm = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=self.state_code,
                decision=StateSupervisionViolationResponseDecision.SUSPENSION,
            )
        )
        violation_response_perm = StateSupervisionViolationResponse.new_with_defaults(
            state_code=self.state_code,
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_date=date(2021, 1, 5),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision_perm],
        )
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code,
            supervision_violation_id=1,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[violation_type],
            supervision_violation_responses=[
                violation_response_non_perm,
                violation_response_perm,
            ],
        )
        violation_type.supervision_violation = violation
        violation_decision_non_perm.supervision_violation_response = (
            violation_response_non_perm
        )
        violation_response_non_perm.supervision_violation = violation
        violation_decision_perm.supervision_violation_response = violation_response_perm
        violation_response_perm.supervision_violation = violation

        expected = [
            ViolationWithResponseEvent(
                state_code=self.state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 5),
                response_date=date(2021, 1, 5),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="TECHNICAL",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.SUSPENSION,
            )
        ]

        violation_with_response_events = identifier.find_violation_with_response_events(
            violation
        )
        self.assertEqual(expected, violation_with_response_events)
