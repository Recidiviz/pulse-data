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
"""Various utils functions for working with StateSupervisionViolationResponses in calculations."""
import datetime
from collections import defaultdict
from datetime import date
from typing import Callable, Dict, List, Optional

from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolationResponse,
)

DECISION_SEVERITY_ORDER = [
    StateSupervisionViolationResponseDecision.REVOCATION,
    StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
    StateSupervisionViolationResponseDecision.TREATMENT_IN_PRISON,
    StateSupervisionViolationResponseDecision.WARRANT_ISSUED,
    StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
    StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
    StateSupervisionViolationResponseDecision.EXTENSION,
    StateSupervisionViolationResponseDecision.SPECIALIZED_COURT,
    StateSupervisionViolationResponseDecision.SUSPENSION,
    StateSupervisionViolationResponseDecision.SERVICE_TERMINATION,
    StateSupervisionViolationResponseDecision.TREATMENT_IN_FIELD,
    StateSupervisionViolationResponseDecision.COMMUNITY_SERVICE,
    StateSupervisionViolationResponseDecision.DELAYED_ACTION,
    StateSupervisionViolationResponseDecision.OTHER,
    StateSupervisionViolationResponseDecision.INTERNAL_UNKNOWN,
    StateSupervisionViolationResponseDecision.WARNING,
    StateSupervisionViolationResponseDecision.CONTINUANCE,
]


def prepare_violation_responses_for_calculations(
    violation_responses: List[StateSupervisionViolationResponse],
    pre_processing_function: Optional[
        Callable[
            [List[StateSupervisionViolationResponse]],
            List[StateSupervisionViolationResponse],
        ]
    ],
) -> List[StateSupervisionViolationResponse]:
    """Performs the provided pre-processing step on the list of StateSupervisionViolationResponses, if applicable.
    Else, returns the original |violation_responses| list."""
    prepared_violation_responses = violation_responses
    if pre_processing_function:
        prepared_violation_responses = pre_processing_function(violation_responses)

    return sorted_violation_responses_with_set_dates(prepared_violation_responses)


def responses_on_most_recent_response_date(
    violation_responses: List[StateSupervisionViolationResponse],
) -> List[StateSupervisionViolationResponse]:
    """Finds the most recent response_date out of all of the violation_responses, and returns all responses with that
    response_date."""
    if not violation_responses:
        return []

    responses_by_date: Dict[
        datetime.date, List[StateSupervisionViolationResponse]
    ] = defaultdict(list)

    for response in violation_responses:
        if not response.response_date:
            raise ValueError(
                f"Found null response date on response {response} - all null dates should be filtered at this point"
            )
        responses_by_date[response.response_date].append(response)

    most_recent_response_date = max(responses_by_date.keys())

    return responses_by_date[most_recent_response_date]


def get_most_severe_response_decision(
    violation_responses: List[StateSupervisionViolationResponse],
) -> Optional[StateSupervisionViolationResponseDecision]:
    """Returns the most severe response decision on the given |violation_responses|."""
    if not violation_responses:
        return None

    response_decisions: List[StateSupervisionViolationResponseDecision] = []
    for response in violation_responses:

        if response.supervision_violation_response_decisions:
            decision_entries = response.supervision_violation_response_decisions

            for decision_entry in decision_entries:
                if decision_entry.decision:
                    response_decisions.append(decision_entry.decision)

    # Find the most severe decision responses
    return identify_most_severe_response_decision(response_decisions)


def sorted_violation_responses_with_set_dates(
    violation_responses: List[StateSupervisionViolationResponse],
) -> List[StateSupervisionViolationResponse]:
    """Sorts the list of |violation_responses|. Filters out any responses that don't
    have a set response_date."""
    filtered_responses = [
        response
        for response in violation_responses
        if response.response_date is not None
    ]

    # All responses will have a response_date at this point, but date.min helps to satisfy mypy
    filtered_responses.sort(key=lambda b: b.response_date or date.min)

    return filtered_responses


def violation_responses_in_window(
    violation_responses: List[StateSupervisionViolationResponse],
    upper_bound_exclusive: date,
    lower_bound_inclusive: Optional[date],
) -> List[StateSupervisionViolationResponse]:
    """Filters the violation responses to the ones that have a response_date before the
    |upper_bound_exclusive| date and after the |lower_bound_inclusive|, if set.
    """
    responses_in_window = [
        response
        for response in violation_responses
        if response.response_date is not None
        and response.response_date < upper_bound_exclusive
        # Only limit with a lower bound if one is set
        and (
            lower_bound_inclusive is None
            or lower_bound_inclusive <= response.response_date
        )
    ]

    return responses_in_window


def identify_most_severe_response_decision(
    decisions: List[StateSupervisionViolationResponseDecision],
) -> Optional[StateSupervisionViolationResponseDecision]:
    """Identifies the most severe decision on the responses according
    to the static decision type ranking."""
    return next(
        (decision for decision in DECISION_SEVERITY_ORDER if decision in decisions),
        None,
    )
