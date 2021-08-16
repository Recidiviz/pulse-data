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
import datetime
from collections import defaultdict
from typing import Dict, List, Optional

import attr

from recidiviz.calculator.pipeline.base_identifier import (
    BaseIdentifier,
    IdentifierContextT,
)
from recidiviz.calculator.pipeline.utils.entity_pre_processing_utils import (
    pre_processed_violation_responses_for_calculations,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_violation_delegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.violation_response_utils import (
    get_most_severe_response_decision,
    identify_most_severe_response_decision,
)
from recidiviz.calculator.pipeline.utils.violation_utils import (
    filter_violation_responses_for_violation_history,
    most_severe_violation_subtype,
    sorted_violation_subtypes_by_severity,
    violation_type_from_subtype,
    violation_type_subtypes_with_violation_type_mappings,
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

        Returns:
            A list of ViolationEvents for the person.
        """

        violation_events: List[ViolationEvent] = []

        violation_with_response_events: List[ViolationWithResponseEvent] = []
        for violation in violations:
            if is_placeholder(violation):
                continue
            violation_with_response_events.extend(
                self._find_violation_with_response_events(violation)
            )

        if violation_with_response_events:
            violation_with_response_events = self._add_aggregate_event_date_fields(
                violation_with_response_events,
                get_state_specific_violation_delegate(
                    violation_with_response_events[0].state_code
                ),
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

        sorted_violation_responses = pre_processed_violation_responses_for_calculations(
            violation_responses=violation.supervision_violation_responses,
            state_code=state_code,
        )

        violation_delegate = get_state_specific_violation_delegate(state_code)
        appropriate_violation_responses_with_dates: List[
            StateSupervisionViolationResponse
        ] = filter_violation_responses_for_violation_history(
            violation_delegate,
            violation_responses=sorted_violation_responses,
            include_follow_up_responses=False,
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

        violation_subtypes = violation_delegate.get_violation_type_subtype_strings_for_violation(
            first_violation_response.supervision_violation  # type: ignore
        )
        sorted_violation_subtypes = sorted_violation_subtypes_by_severity(
            violation_subtypes, violation_delegate
        )
        supported_violation_subtypes = (
            violation_type_subtypes_with_violation_type_mappings(violation_delegate)
        )

        for index, violation_subtype in enumerate(sorted_violation_subtypes):
            if violation_subtype not in supported_violation_subtypes:
                # It's possible for violations to have subtypes that don't explicitly map to a
                # StateSupervisionViolationType value. We only want to record violation events for the defined
                # StateSupervisionViolationType values on a violation, so we avoid creating events for subtypes
                # without supported mappings to these values.
                continue
            violation_type = violation_type_from_subtype(
                violation_delegate, violation_subtype
            )
            is_most_severe_violation_type = index == 0
            violation_with_response_events.append(
                ViolationWithResponseEvent(
                    state_code=state_code,
                    supervision_violation_id=supervision_violation_id,
                    event_date=response_date,
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

    def _add_aggregate_event_date_fields(
        self,
        violation_with_response_events: List[ViolationWithResponseEvent],
        state_violation_delegate: StateSpecificViolationDelegate,
    ) -> List[ViolationWithResponseEvent]:
        """Augments existing ViolationWithResponseEvents with aggregate statistics
        over all events of a particular day."""

        processed_events: List[ViolationWithResponseEvent] = []
        events_by_event_date: Dict[
            datetime.date, List[ViolationWithResponseEvent]
        ] = defaultdict(list)
        for violation_with_response_event in violation_with_response_events:
            events_by_event_date[violation_with_response_event.event_date].append(
                violation_with_response_event
            )

        for events in events_by_event_date.values():
            most_severe_response_decision_of_all_violations_in_day = (
                identify_most_severe_response_decision(
                    [
                        event.most_severe_response_decision
                        for event in events
                        if event.most_severe_response_decision
                    ]
                )
            )
            most_severe_violation_subtype_of_all_violations_in_day = (
                most_severe_violation_subtype(
                    [
                        event.violation_type_subtype
                        for event in events
                        if event.violation_type_subtype
                        and event.is_most_severe_violation_type
                    ],
                    state_violation_delegate,
                )
            )
            most_severe_violation_type_of_all_violations_in_day = (
                violation_type_from_subtype(
                    state_violation_delegate,
                    most_severe_violation_subtype_of_all_violations_in_day,
                )
                if most_severe_violation_subtype_of_all_violations_in_day
                else None
            )
            for event in events:
                if most_severe_response_decision_of_all_violations_in_day:
                    event = attr.evolve(
                        event,
                        is_most_severe_response_decision_of_all_violations=(
                            event.most_severe_response_decision
                            == most_severe_response_decision_of_all_violations_in_day
                        ),
                    )
                if most_severe_violation_type_of_all_violations_in_day:
                    event = attr.evolve(
                        event,
                        is_most_severe_violation_type_of_all_violations=(
                            event.violation_type
                            == most_severe_violation_type_of_all_violations_in_day
                        ),
                    )
                processed_events.append(event)
        return processed_events
