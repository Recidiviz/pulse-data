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
from typing import List, Optional, Callable, Dict

from recidiviz.calculator.pipeline.utils.calculator_utils import (
    identify_most_severe_response_decision,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)

from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolationResponse,
)


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
    if pre_processing_function:
        return pre_processing_function(violation_responses)
    return violation_responses


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
