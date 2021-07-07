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
"""Identifies violations and their responses with appropriate decisions."""
from typing import List, Optional

from recidiviz.calculator.pipeline.base_identifier import (
    BaseIdentifier,
    IdentifierContextT,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_violation_type_subtype_strings_for_violation,
    sorted_violation_subtypes_by_severity,
    state_specific_violation_response_pre_processing_function,
    state_specific_violation_responses_for_violation_history,
    state_specific_violation_type_subtypes_with_violation_type_mappings,
    violation_type_from_subtype,
)
from recidiviz.calculator.pipeline.utils.violation_response_utils import (
    get_most_severe_response_decision,
    prepare_violation_responses_for_calculations,
)
from recidiviz.calculator.pipeline.utils.violation_utils import (
    DEFAULT_VIOLATION_SUBTYPE_SEVERITY_ORDER,
)
from recidiviz.calculator.pipeline.violation.events import (
    ViolationEvent,
    ViolationWithResponseEvent,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)


class ViolationIdentifier(BaseIdentifier[List[ViolationEvent]]):
    """Identifier class for violations and their responses with appropriate decisions."""

    def __init__(self) -> None:
        self.identifier_event_class = ViolationEvent

    def find_events(
        self, _person: StatePerson, identifier_context: IdentifierContextT
    ) -> List[ViolationEvent]:
        return self._find_violation_events(**identifier_context)

    def _find_violation_events(
        self,
        violations: List[StateSupervisionViolation],
    ) -> List[ViolationEvent]:
        """Finds instances of a violation.

        Identifies instance of a violation occurring.

        Args:
            - violations: All of the person's StateSupervisionViolations
            - violation_responses: All of the person's StateSupervisionViolationResponses

        Returns:
            A list of ViolationEvents for the person.
        """

        violation_events: List[ViolationEvent] = []

        for violation in violations:
            if is_placeholder(violation):
                continue
            violation_with_response_events = self._find_violation_with_response_events(
                violation
            )
            violation_events.extend(violation_with_response_events)
        return violation_events

    def _find_violation_with_response_events(
        self,
        violation: StateSupervisionViolation,
    ) -> List[ViolationWithResponseEvent]:
        """Finds instances of a violation with its earliest response."""
        violation_with_response_events: List[ViolationWithResponseEvent] = []

        supervision_violation_id = violation.supervision_violation_id

        if not supervision_violation_id:
            raise ValueError(
                "Invalid event: a violation should always have a supervision_violation_id."
            )

        state_code = violation.state_code
        violation_date = violation.violation_date
        is_violent = violation.is_violent
        is_sex_offense = violation.is_sex_offense

        sorted_violation_responses = prepare_violation_responses_for_calculations(
            violation_responses=violation.supervision_violation_responses,
            pre_processing_function=state_specific_violation_response_pre_processing_function(
                state_code=state_code
            ),
        )
        appropriate_violation_responses_with_dates: List[
            StateSupervisionViolationResponse
        ] = state_specific_violation_responses_for_violation_history(
            state_code, sorted_violation_responses
        )

        if not appropriate_violation_responses_with_dates:
            return violation_with_response_events

        first_violation_response = appropriate_violation_responses_with_dates[0]
        response_date = first_violation_response.response_date

        most_severe_response_decision: Optional[
            StateSupervisionViolationResponseDecision
        ] = get_most_severe_response_decision(
            violation_responses=[first_violation_response]
        )

        if not response_date:
            raise ValueError(
                "Invalid control: All responses should have response_dates."
            )

        violation_subtypes = get_violation_type_subtype_strings_for_violation(
            first_violation_response.supervision_violation  # type: ignore
        )
        sorted_violation_subtypes = sorted_violation_subtypes_by_severity(
            state_code, violation_subtypes, DEFAULT_VIOLATION_SUBTYPE_SEVERITY_ORDER
        )
        supported_violation_subtypes = (
            state_specific_violation_type_subtypes_with_violation_type_mappings(
                state_code
            )
        )
        for index, violation_subtype in enumerate(sorted_violation_subtypes):
            if violation_subtype not in supported_violation_subtypes:
                # It's possible for violations to have subtypes that don't explicitly map to a StateSupervisionViolationType value.
                # We only want to record violation events for the defined StateSupervisionViolationType values on a violation,
                # so we avoid creating events for subtypes without supported mappings to these values.
                continue
            violation_type = violation_type_from_subtype(state_code, violation_subtype)
            is_most_severe_violation_type = index == 0
            violation_with_response_events.append(
                ViolationWithResponseEvent(
                    state_code=state_code,
                    supervision_violation_id=supervision_violation_id,
                    event_date=response_date,
                    response_date=response_date,
                    violation_date=violation_date,
                    violation_type=violation_type,
                    violation_type_subtype=violation_subtype,
                    is_most_severe_violation_type=is_most_severe_violation_type,
                    is_violent=is_violent,
                    is_sex_offense=is_sex_offense,
                    most_severe_response_decision=most_severe_response_decision,
                )
            )

        return violation_with_response_events
