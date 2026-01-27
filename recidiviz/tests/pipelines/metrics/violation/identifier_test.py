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
# pylint: disable=protected-access
"""Tests for violation/identifier.py"""
import unittest
from datetime import date
from typing import Dict, List, Sequence, Union

import attr

from recidiviz.common.constants.state.state_person import StateEthnicity
from recidiviz.common.constants.state.state_supervision_violated_condition import (
    StateSupervisionViolatedConditionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStateSupervisionViolatedConditionEntry,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
    NormalizedStateSupervisionViolationResponseDecisionEntry,
    NormalizedStateSupervisionViolationTypeEntry,
)
from recidiviz.pipelines.metrics.violation import identifier as violation_identifier
from recidiviz.pipelines.metrics.violation.events import (
    ViolationEvent,
    ViolationWithResponseEvent,
)
from recidiviz.pipelines.metrics.violation.identifier import ViolationIdentifier
from recidiviz.pipelines.utils.execution_utils import TableRow
from recidiviz.tests.pipelines.fake_state_calculation_config_manager import (
    start_pipeline_delegate_getter_patchers,
)

_STATE_CODE = "US_XX"


class TestFindViolationEvents(unittest.TestCase):
    """Tests the find_violation_events function."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            violation_identifier
        )
        self.identifier = ViolationIdentifier(StateCode.US_XX)
        self.person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=99000123,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def _run_find_violation_events(
        self, violations: List[NormalizedStateSupervisionViolation]
    ) -> List[ViolationEvent]:
        """Helper function for testing the find_events function."""

        all_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            NormalizedStateSupervisionViolation.__name__: violations,
        }

        return self.identifier.identify(
            self.person,
            all_kwargs,
            included_result_classes={ViolationWithResponseEvent},
        )

    def test_find_violation_events(self) -> None:
        violation_type = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.FELONY,
        )
        violation_decision = NormalizedStateSupervisionViolationResponseDecisionEntry(
            supervision_violation_response_decision_entry_id=1,
            state_code="US_XX",
            decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
        )
        violation_response = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            state_code="US_XX",
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision],
            sequence_num=0,
        )
        violation = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            supervision_violation_id=1,
            external_id="sv1",
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[violation_type],
            supervision_violation_responses=[violation_response],
        )
        violation_decision.supervision_violation_response = violation_response
        violation_response.supervision_violation = violation
        violation_type.supervision_violation = violation

        violation_events = self._run_find_violation_events([violation])

        expected = [
            ViolationWithResponseEvent(
                state_code="US_XX",
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.FELONY,
                violation_type_subtype="FELONY",
                violation_type_subtype_raw_text=None,
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
                is_most_severe_violation_type_of_all_violations=True,
                is_most_severe_response_decision_of_all_violations=True,
            )
        ]
        self.assertEqual(expected, violation_events)

    def test_find_violation_events_no_violations(self) -> None:
        violation_events = self._run_find_violation_events([])

        self.assertEqual([], violation_events)

    def test_find_violation_events_wrong_result_classes(self) -> None:
        with self.assertRaisesRegex(NotImplementedError, "Filtering of events"):
            self.identifier.identify(
                self.person, identifier_context={}, included_result_classes=set()
            )


class TestFindViolationWithResponseEvents(unittest.TestCase):
    """Tests the find_violation_with_response_events function."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            violation_identifier
        )
        state_code = StateCode.US_XX
        self.violation_type = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code=state_code.value,
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        self.violation_decision = (
            NormalizedStateSupervisionViolationResponseDecisionEntry(
                supervision_violation_response_decision_entry_id=1,
                state_code=state_code.value,
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            )
        )
        self.violation_response = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            state_code=state_code.value,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[self.violation_decision],
            sequence_num=0,
        )
        self.violation = NormalizedStateSupervisionViolation(
            state_code=state_code.value,
            supervision_violation_id=1,
            external_id="sv1",
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[self.violation_type],
            supervision_violation_responses=[self.violation_response],
        )
        self.violation_decision.supervision_violation_response = self.violation_response
        self.violation_response.supervision_violation = self.violation
        self.violation_type.supervision_violation = self.violation
        self.identifier = ViolationIdentifier(state_code)

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def test_find_violation_with_response_events(self) -> None:
        violation_with_response_events = (
            self.identifier._find_violation_with_response_events(
                violation=self.violation
            )
        )

        expected = [
            ViolationWithResponseEvent(
                state_code="US_XX",
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="TECHNICAL",
                violation_type_subtype_raw_text=None,
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            )
        ]

        self.assertEqual(expected, violation_with_response_events)

    def test_find_violation_with_response_events_needs_response_date(
        self,
    ) -> None:
        self.violation_response.response_date = None

        with self.assertRaises(ValueError):
            _ = self.identifier._find_violation_with_response_events(
                violation=self.violation
            )

    def test_find_violation_with_response_events_includes_all_violation_types(
        self,
    ) -> None:
        violation_type_1 = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_type_2 = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.ABSCONDED,
        )
        violation_decision = NormalizedStateSupervisionViolationResponseDecisionEntry(
            supervision_violation_response_decision_entry_id=1,
            state_code="US_XX",
            decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
        )
        violation_response = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            state_code="US_XX",
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision],
            sequence_num=0,
        )
        violation = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            supervision_violation_id=1,
            external_id="sv1",
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
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="TECHNICAL",
                is_most_severe_violation_type=False,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
        ]

        violation_with_response_events = (
            self.identifier._find_violation_with_response_events(violation=violation)
        )

        self.assertEqual(expected, violation_with_response_events)

    def test_find_violation_with_response_events_filters_permanent_decisions(
        self,
    ) -> None:
        violation_type = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_decision_non_perm = (
            NormalizedStateSupervisionViolationResponseDecisionEntry(
                supervision_violation_response_decision_entry_id=1,
                state_code="US_XX",
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            )
        )
        violation_response_non_perm = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            state_code="US_XX",
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision_non_perm],
            sequence_num=0,
        )
        violation_decision_perm = (
            NormalizedStateSupervisionViolationResponseDecisionEntry(
                supervision_violation_response_decision_entry_id=1,
                state_code="US_XX",
                decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
            )
        )
        violation_response_perm = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            state_code="US_XX",
            external_id="svr2",
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_date=date(2021, 1, 5),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision_perm],
            sequence_num=0,
        )
        violation = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            supervision_violation_id=1,
            external_id="sv1",
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
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="TECHNICAL",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
        ]

        violation_with_response_events = (
            self.identifier._find_violation_with_response_events(
                violation=self.violation
            )
        )

        self.assertEqual(expected, violation_with_response_events)

    def test_find_violation_with_response_events_filters_draft_responses(
        self,
    ) -> None:
        violation_type = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_decision_non_draft = (
            NormalizedStateSupervisionViolationResponseDecisionEntry(
                supervision_violation_response_decision_entry_id=1,
                state_code="US_XX",
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            )
        )
        violation_response_non_draft = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            state_code="US_XX",
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision_non_draft],
            sequence_num=0,
        )
        violation_decision_draft = (
            NormalizedStateSupervisionViolationResponseDecisionEntry(
                supervision_violation_response_decision_entry_id=1,
                state_code="US_XX",
                decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
            )
        )
        violation_response_draft = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            state_code="US_XX",
            external_id="svr2",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 3),
            is_draft=True,
            supervision_violation_response_decisions=[violation_decision_draft],
            sequence_num=0,
        )
        violation = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            supervision_violation_id=1,
            external_id="sv1",
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
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="TECHNICAL",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
        ]

        violation_with_response_events = (
            self.identifier._find_violation_with_response_events(
                violation=self.violation
            )
        )

        self.assertEqual(expected, violation_with_response_events)

    def test_find_violation_with_response_events_takes_first_response(
        self,
    ) -> None:
        violation_type = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_decision_1 = NormalizedStateSupervisionViolationResponseDecisionEntry(
            supervision_violation_response_decision_entry_id=1,
            state_code="US_XX",
            decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
        )
        violation_response_1 = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            state_code="US_XX",
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision_1],
            sequence_num=0,
        )
        violation_decision_2 = NormalizedStateSupervisionViolationResponseDecisionEntry(
            supervision_violation_response_decision_entry_id=1,
            state_code="US_XX",
            decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
        )
        violation_response_2 = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            state_code="US_XX",
            external_id="svr2",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 5),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision_2],
            sequence_num=0,
        )
        violation = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            supervision_violation_id=1,
            external_id="sv1",
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
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="TECHNICAL",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
        ]
        violation_with_response_events = (
            self.identifier._find_violation_with_response_events(
                violation=self.violation
            )
        )

        self.assertEqual(expected, violation_with_response_events)

    def test_find_violation_with_response_events_takes_most_severe_decision(
        self,
    ) -> None:
        violation_type = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.FELONY,
        )
        violation_decision_1 = NormalizedStateSupervisionViolationResponseDecisionEntry(
            supervision_violation_response_decision_entry_id=1,
            state_code="US_XX",
            decision=StateSupervisionViolationResponseDecision.REVOCATION,
        )
        violation_decision_2 = NormalizedStateSupervisionViolationResponseDecisionEntry(
            supervision_violation_response_decision_entry_id=1,
            state_code="US_XX",
            decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
        )
        violation_response = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            state_code="US_XX",
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            sequence_num=0,
            supervision_violation_response_decisions=[
                violation_decision_1,
                violation_decision_2,
            ],
        )
        violation = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            supervision_violation_id=1,
            external_id="sv1",
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
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.FELONY,
                violation_type_subtype="FELONY",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            ),
        ]

        violation_with_response_events = (
            self.identifier._find_violation_with_response_events(violation=violation)
        )

        self.assertEqual(expected, violation_with_response_events)

    def test_find_violation_with_response_events_populates_violation_subtypes_correctly_for_conditions(
        self,
    ) -> None:
        self._stop_state_specific_delegate_patchers()
        identifier = ViolationIdentifier(StateCode.US_MO)
        state_code = "US_MO"

        violation_type_technical = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code=state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_type_absconded = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code=state_code,
            violation_type=StateSupervisionViolationType.ABSCONDED,
        )
        violation_decision = NormalizedStateSupervisionViolationResponseDecisionEntry(
            supervision_violation_response_decision_entry_id=1,
            state_code=state_code,
            decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
        )
        violation_response = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            state_code=state_code,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="INI",
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision],
            sequence_num=0,
        )
        violation = NormalizedStateSupervisionViolation(
            state_code=state_code,
            supervision_violation_id=1,
            external_id="sv1",
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

        violation_with_response_events = (
            identifier._find_violation_with_response_events(violation=violation)
        )

        expected = [
            ViolationWithResponseEvent(
                state_code=state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.ABSCONDED,
                violation_type_subtype="ABSCONDED",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
            ViolationWithResponseEvent(
                state_code=state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
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
        self._stop_state_specific_delegate_patchers()
        identifier = ViolationIdentifier(StateCode.US_MO)
        state_code = "US_MO"

        violation_type_technical = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code=state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_type_absconded = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code=state_code,
            violation_type=StateSupervisionViolationType.ABSCONDED,
        )
        violation_condition_law = NormalizedStateSupervisionViolatedConditionEntry(
            supervision_violated_condition_entry_id=1,
            state_code=state_code,
            condition=StateSupervisionViolatedConditionType.LAW,
            condition_raw_text="law_citation",
        )
        violation_condition_sub = NormalizedStateSupervisionViolatedConditionEntry(
            supervision_violated_condition_entry_id=1,
            state_code=state_code,
            condition=StateSupervisionViolatedConditionType.SUBSTANCE,
            condition_raw_text="substance_abuse",
        )
        violation_decision = NormalizedStateSupervisionViolationResponseDecisionEntry(
            supervision_violation_response_decision_entry_id=1,
            state_code=state_code,
            decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
        )
        violation_response = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            state_code=state_code,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="INI",
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision],
            sequence_num=0,
        )
        violation = NormalizedStateSupervisionViolation(
            state_code=state_code,
            supervision_violation_id=1,
            external_id="sv1",
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

        violation_with_response_events = (
            identifier._find_violation_with_response_events(violation=violation)
        )

        expected = [
            ViolationWithResponseEvent(
                state_code=state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="LAW_CITATION",
                violation_type_subtype_raw_text="LAW_CITATION",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
            ViolationWithResponseEvent(
                state_code=state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.ABSCONDED,
                violation_type_subtype="ABSCONDED",
                is_most_severe_violation_type=False,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
            ViolationWithResponseEvent(
                state_code=state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="SUBSTANCE_ABUSE",
                violation_type_subtype_raw_text="SUBSTANCE_ABUSE",
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
        self._stop_state_specific_delegate_patchers()
        identifier = ViolationIdentifier(StateCode.US_MO)
        state_code = "US_MO"

        violation_type = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code=state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_decision = NormalizedStateSupervisionViolationResponseDecisionEntry(
            supervision_violation_response_decision_entry_id=1,
            state_code=state_code,
            decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
        )
        violation_response = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            state_code=state_code,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision],
            sequence_num=0,
        )
        violation = NormalizedStateSupervisionViolation(
            state_code=state_code,
            supervision_violation_id=1,
            external_id="sv1",
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
                identifier._find_violation_with_response_events(violation=violation)
            )
            self.assertEqual([], violation_with_response_events)

    def test_find_violation_with_response_events_skips_unexpected_subtypes(
        self,
    ) -> None:
        self._stop_state_specific_delegate_patchers()
        identifier = ViolationIdentifier(StateCode.US_MO)
        state_code = "US_MO"

        violation_type_technical = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code=state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_type_absconded = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code=state_code,
            violation_type=StateSupervisionViolationType.ABSCONDED,
        )
        violation_condition_unk = NormalizedStateSupervisionViolatedConditionEntry(
            supervision_violated_condition_entry_id=1,
            state_code=state_code,
            condition=StateSupervisionViolatedConditionType.EXTERNAL_UNKNOWN,
            condition_raw_text="UNK",
        )
        violation_condition_sub = NormalizedStateSupervisionViolatedConditionEntry(
            supervision_violated_condition_entry_id=1,
            state_code=state_code,
            condition=StateSupervisionViolatedConditionType.SUBSTANCE,
            condition_raw_text="substance_abuse",
        )
        violation_decision = NormalizedStateSupervisionViolationResponseDecisionEntry(
            supervision_violation_response_decision_entry_id=1,
            state_code=state_code,
            decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
        )
        violation_response = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            state_code=state_code,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="INI",
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision],
            sequence_num=0,
        )
        violation = NormalizedStateSupervisionViolation(
            state_code=state_code,
            supervision_violation_id=1,
            external_id="sv1",
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[
                violation_type_technical,
                violation_type_absconded,
            ],
            supervision_violation_responses=[violation_response],
            supervision_violated_conditions=[
                violation_condition_unk,
                violation_condition_sub,
            ],
        )
        violation_type_absconded.supervision_violation = violation
        violation_type_technical.supervision_violation = violation
        violation_decision.supervision_violation_response = violation_response
        violation_response.supervision_violation = violation

        violation_with_response_events = (
            identifier._find_violation_with_response_events(violation=violation)
        )

        expected = [
            ViolationWithResponseEvent(
                state_code=state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.ABSCONDED,
                violation_type_subtype="ABSCONDED",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
            ViolationWithResponseEvent(
                state_code=state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="SUBSTANCE_ABUSE",
                violation_type_subtype_raw_text="SUBSTANCE_ABUSE",
                is_most_severe_violation_type=False,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
            ViolationWithResponseEvent(
                state_code=state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
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

    def test_find_violation_with_response_events_populates_violation_subtypes_correctly_for_technical(
        self,
    ) -> None:
        self._stop_state_specific_delegate_patchers()
        identifier = ViolationIdentifier(StateCode.US_PA)
        state_code = "US_PA"
        violation_type_low = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code=state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="L01",
        )
        violation_type_med = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code=state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="M01",
        )
        violation_type_high = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code=state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="H01",
        )
        violation_type_sub = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code=state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="H03",
        )
        violation_type_elec = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code=state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="M16",
        )
        violation_decision = NormalizedStateSupervisionViolationResponseDecisionEntry(
            supervision_violation_response_decision_entry_id=1,
            state_code=state_code,
            decision=StateSupervisionViolationResponseDecision.REVOCATION,
        )
        violation_response = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            state_code=state_code,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision],
            sequence_num=0,
        )
        violation = NormalizedStateSupervisionViolation(
            state_code=state_code,
            supervision_violation_id=1,
            external_id="sv1",
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
                state_code=state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="HIGH_TECH",
                violation_type_subtype_raw_text="H01",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            ),
            ViolationWithResponseEvent(
                state_code=state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="SUBSTANCE_ABUSE",
                violation_type_subtype_raw_text="H03",
                is_most_severe_violation_type=False,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            ),
            ViolationWithResponseEvent(
                state_code=state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="ELEC_MONITORING",
                violation_type_subtype_raw_text="M16",
                is_most_severe_violation_type=False,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            ),
            ViolationWithResponseEvent(
                state_code=state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="MED_TECH",
                violation_type_subtype_raw_text="M01",
                is_most_severe_violation_type=False,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            ),
            ViolationWithResponseEvent(
                state_code=state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="LOW_TECH",
                violation_type_subtype_raw_text="L01",
                is_most_severe_violation_type=False,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            ),
        ]

        violation_with_response_events = (
            identifier._find_violation_with_response_events(violation=violation)
        )
        self.assertEqual(expected, violation_with_response_events)

    def test_find_violation_with_response_events_uses_permanent_decision_only_us_nd(
        self,
    ) -> None:
        self._stop_state_specific_delegate_patchers()
        identifier = ViolationIdentifier(StateCode.US_ND)
        state_code = "US_ND"
        violation_type = NormalizedStateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=1,
            state_code=state_code,
            violation_type=StateSupervisionViolationType.TECHNICAL,
        )
        violation_decision_non_perm = (
            NormalizedStateSupervisionViolationResponseDecisionEntry(
                supervision_violation_response_decision_entry_id=1,
                state_code=state_code,
                decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            )
        )
        violation_response_non_perm = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=1,
            external_id="svr1",
            state_code=state_code,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision_non_perm],
            sequence_num=0,
        )
        violation_decision_perm = (
            NormalizedStateSupervisionViolationResponseDecisionEntry(
                supervision_violation_response_decision_entry_id=1,
                state_code=state_code,
                decision=StateSupervisionViolationResponseDecision.SUSPENSION,
            )
        )
        violation_response_perm = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=2,
            external_id="svr2",
            state_code=state_code,
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_date=date(2021, 1, 5),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision_perm],
            sequence_num=1,
        )
        violation = NormalizedStateSupervisionViolation(
            state_code=state_code,
            supervision_violation_id=1,
            external_id="sv1",
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
                state_code=state_code,
                supervision_violation_id=1,
                event_date=date(2021, 1, 5),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype="TECHNICAL",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.SUSPENSION,
            )
        ]

        violation_with_response_events = (
            identifier._find_violation_with_response_events(violation=violation)
        )
        self.assertEqual(expected, violation_with_response_events)


class TestAddAggregateEventDateFields(unittest.TestCase):
    """Tests the _add_aggregate_event_date_fields function."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            violation_identifier
        )
        self.identifier = ViolationIdentifier(StateCode.US_XX)

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def test_add_aggregate_event_date_fields(self) -> None:
        first_violation_on_first_day = ViolationWithResponseEvent(
            state_code="US_XX",
            supervision_violation_id=1,
            event_date=date(2021, 1, 1),
            violation_date=None,
            violation_type=StateSupervisionViolationType.MISDEMEANOR,
            violation_type_subtype="MISDEMEANOR",
            is_most_severe_violation_type=True,
            is_violent=False,
            is_sex_offense=False,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
        )
        second_violation_on_first_day = ViolationWithResponseEvent(
            state_code="US_XX",
            supervision_violation_id=2,
            event_date=date(2021, 1, 1),
            violation_date=None,
            violation_type=StateSupervisionViolationType.FELONY,
            violation_type_subtype="FELONY",
            is_most_severe_violation_type=True,
            is_violent=False,
            is_sex_offense=False,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
        )
        violation_with_response_events = [
            first_violation_on_first_day,
            second_violation_on_first_day,
        ]

        expected = [
            attr.evolve(
                first_violation_on_first_day,
                is_most_severe_response_decision_of_all_violations=False,
                is_most_severe_violation_type_of_all_violations=False,
            ),
            attr.evolve(
                second_violation_on_first_day,
                is_most_severe_response_decision_of_all_violations=True,
                is_most_severe_violation_type_of_all_violations=True,
            ),
        ]

        self.assertEqual(
            self.identifier._add_aggregate_event_date_fields(
                violation_with_response_events
            ),
            expected,
        )

    def test_add_aggregate_event_date_fields_aggregates_per_day(self) -> None:
        first_violation_on_first_day = ViolationWithResponseEvent(
            state_code="US_XX",
            supervision_violation_id=1,
            event_date=date(2021, 1, 1),
            violation_date=None,
            violation_type=StateSupervisionViolationType.MISDEMEANOR,
            violation_type_subtype="MISDEMEANOR",
            is_most_severe_violation_type=True,
            is_violent=False,
            is_sex_offense=False,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
        )
        second_violation_on_first_day = ViolationWithResponseEvent(
            state_code="US_XX",
            supervision_violation_id=2,
            event_date=date(2021, 1, 1),
            violation_date=None,
            violation_type=StateSupervisionViolationType.FELONY,
            violation_type_subtype="FELONY",
            is_most_severe_violation_type=True,
            is_violent=False,
            is_sex_offense=False,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
        )
        first_violation_on_second_day = ViolationWithResponseEvent(
            state_code="US_XX",
            supervision_violation_id=3,
            event_date=date(2021, 1, 2),
            violation_date=None,
            violation_type=StateSupervisionViolationType.FELONY,
            violation_type_subtype="FELONY",
            is_most_severe_violation_type=True,
            is_violent=False,
            is_sex_offense=False,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.TREATMENT_IN_PRISON,
        )
        second_violation_on_second_day = ViolationWithResponseEvent(
            state_code="US_XX",
            supervision_violation_id=3,
            event_date=date(2021, 1, 2),
            violation_date=None,
            violation_type=StateSupervisionViolationType.ABSCONDED,
            violation_type_subtype="ABSCONDED",
            is_most_severe_violation_type=True,
            is_violent=False,
            is_sex_offense=False,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
        )
        violation_with_response_events = [
            first_violation_on_first_day,
            second_violation_on_first_day,
            first_violation_on_second_day,
            second_violation_on_second_day,
        ]

        expected = [
            attr.evolve(
                first_violation_on_first_day,
                is_most_severe_response_decision_of_all_violations=False,
                is_most_severe_violation_type_of_all_violations=False,
            ),
            attr.evolve(
                second_violation_on_first_day,
                is_most_severe_response_decision_of_all_violations=True,
                is_most_severe_violation_type_of_all_violations=True,
            ),
            attr.evolve(
                first_violation_on_second_day,
                is_most_severe_response_decision_of_all_violations=False,
                is_most_severe_violation_type_of_all_violations=True,
            ),
            attr.evolve(
                second_violation_on_second_day,
                is_most_severe_response_decision_of_all_violations=True,
                is_most_severe_violation_type_of_all_violations=False,
            ),
        ]

        self.assertEqual(
            self.identifier._add_aggregate_event_date_fields(
                violation_with_response_events
            ),
            expected,
        )

    def test_add_aggregate_event_date_fields_none_if_no_decisions(self) -> None:
        first_violation_on_first_day = ViolationWithResponseEvent(
            state_code="US_XX",
            supervision_violation_id=1,
            event_date=date(2021, 1, 1),
            violation_date=None,
            violation_type=StateSupervisionViolationType.MISDEMEANOR,
            violation_type_subtype="MISDEMEANOR",
            is_most_severe_violation_type=True,
            is_violent=False,
            is_sex_offense=False,
            most_severe_response_decision=None,
        )
        second_violation_on_first_day = ViolationWithResponseEvent(
            state_code="US_XX",
            supervision_violation_id=2,
            event_date=date(2021, 1, 1),
            violation_date=None,
            violation_type=StateSupervisionViolationType.FELONY,
            violation_type_subtype="FELONY",
            is_most_severe_violation_type=True,
            is_violent=False,
            is_sex_offense=False,
            most_severe_response_decision=None,
        )
        violation_with_response_events = [
            first_violation_on_first_day,
            second_violation_on_first_day,
        ]

        expected = [
            attr.evolve(
                first_violation_on_first_day,
                is_most_severe_response_decision_of_all_violations=None,
                is_most_severe_violation_type_of_all_violations=False,
            ),
            attr.evolve(
                second_violation_on_first_day,
                is_most_severe_response_decision_of_all_violations=None,
                is_most_severe_violation_type_of_all_violations=True,
            ),
        ]

        self.assertEqual(
            self.identifier._add_aggregate_event_date_fields(
                violation_with_response_events
            ),
            expected,
        )
