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
from typing import Dict, List, Optional, Set, Type

import attr

from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.normalized_entities_utils import (
    sort_normalized_entities_by_sequence_num,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.metrics.base_identifier import (
    BaseIdentifier,
    IdentifierContext,
)
from recidiviz.pipelines.metrics.utils.violation_utils import (
    filter_violation_responses_for_violation_history,
    most_severe_violation_subtype,
    sorted_violation_subtypes_by_severity,
    violation_type_from_subtype,
    violation_type_subtypes_with_violation_type_mappings,
)
from recidiviz.pipelines.metrics.violation.events import (
    ViolationEvent,
    ViolationWithResponseEvent,
)
from recidiviz.pipelines.utils.identifier_models import IdentifierResult
from recidiviz.pipelines.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_violation_delegate,
)
from recidiviz.pipelines.utils.violation_response_utils import (
    get_most_severe_response_decision,
    identify_most_severe_response_decision,
)
from recidiviz.utils.types import assert_type


class ViolationIdentifier(BaseIdentifier[List[ViolationEvent]]):
    """Identifier class for violations and their responses with appropriate decisions."""

    def __init__(self, state_code: StateCode) -> None:
        self.identifier_result_class = ViolationEvent
        self.violation_delegate = get_state_specific_violation_delegate(
            state_code.value
        )

    def identify(
        self,
        _person: NormalizedStatePerson,
        identifier_context: IdentifierContext,
        included_result_classes: Set[Type[IdentifierResult]],
    ) -> List[ViolationEvent]:
        if included_result_classes != {ViolationWithResponseEvent}:
            raise NotImplementedError(
                "Filtering of events is not yet implemented for the violation pipeline."
            )

        return self._find_violation_events(
            violations=identifier_context[NormalizedStateSupervisionViolation.__name__],
        )

    def _find_violation_events(
        self,
        violations: List[NormalizedStateSupervisionViolation],
    ) -> List[ViolationEvent]:
        """Finds instances of a violation.

        Identifies instance of a violation occurring.

        Args:
            - violations: All of the person's StateSupervisionViolations

        Returns:
            A list of ViolationEvents for the person.
        """
        violation_events: List[ViolationEvent] = []

        if not violations:
            return violation_events

        violation_with_response_events: List[ViolationWithResponseEvent] = []
        for violation in violations:
            violation_with_response_events.extend(
                self._find_violation_with_response_events(
                    violation=violation,
                )
            )

        if violation_with_response_events:
            violation_with_response_events = self._add_aggregate_event_date_fields(
                violation_with_response_events,
            )
            violation_events.extend(violation_with_response_events)

        return violation_events

    def _find_violation_with_response_events(
        self, violation: NormalizedStateSupervisionViolation
    ) -> List[ViolationWithResponseEvent]:
        """Finds instances of a violation with its earliest response."""

        violation_with_response_events: List[ViolationWithResponseEvent] = []

        supervision_violation_id = assert_type(violation.supervision_violation_id, int)

        state_code = violation.state_code
        violation_date = violation.violation_date
        is_violent = violation.is_violent
        is_sex_offense = violation.is_sex_offense

        normalized_violation_responses: List[
            NormalizedStateSupervisionViolationResponse
        ] = []

        for response in violation.supervision_violation_responses:
            if not isinstance(response, NormalizedStateSupervisionViolationResponse):
                raise ValueError(
                    "Expected all supervision_violation_responses on "
                    "NormalizedStateSupervisionViolation to be of type "
                    "NormalizedStateSupervisionViolationResponse. Found: "
                    f"{type(response)}."
                )

            normalized_violation_responses.append(response)

        sorted_violation_responses = sort_normalized_entities_by_sequence_num(
            normalized_violation_responses
        )

        appropriate_violation_responses_with_dates: List[
            NormalizedStateSupervisionViolationResponse
        ] = filter_violation_responses_for_violation_history(
            self.violation_delegate,
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

        violation_subtypes_to_raw_text_map = self.violation_delegate.get_violation_type_subtype_strings_for_violation(
            first_violation_response.supervision_violation  # type: ignore
        )
        sorted_violation_subtypes = sorted_violation_subtypes_by_severity(
            violation_subtypes_to_raw_text_map.keys(), self.violation_delegate
        )
        supported_violation_subtypes = (
            violation_type_subtypes_with_violation_type_mappings(
                self.violation_delegate
            )
        )

        for index, violation_subtype in enumerate(sorted_violation_subtypes):
            for subtype_raw_text in violation_subtypes_to_raw_text_map[
                violation_subtype
            ]:
                if violation_subtype not in supported_violation_subtypes:
                    # It's possible for violations to have subtypes that don't explicitly map to a
                    # StateSupervisionViolationType value. We only want to record violation events for the defined
                    # StateSupervisionViolationType values on a violation, so we avoid creating events for subtypes
                    # without supported mappings to these values.
                    continue
                violation_type = violation_type_from_subtype(
                    self.violation_delegate, violation_subtype
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
                        violation_type_subtype_raw_text=subtype_raw_text,
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
                    self.violation_delegate,
                )
            )
            most_severe_violation_type_of_all_violations_in_day = (
                violation_type_from_subtype(
                    self.violation_delegate,
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
