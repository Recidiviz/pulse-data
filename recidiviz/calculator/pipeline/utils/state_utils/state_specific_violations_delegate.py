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
"""Contains the StateSpecificViolationDelegate, the interface
for state-specific decisions involved in categorizing various attributes of
violations."""
import abc
from typing import List, Tuple

from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationType,
)

DEFAULT_VIOLATION_TYPE_SEVERITY_ORDER: List[StateSupervisionViolationType] = [
    StateSupervisionViolationType.FELONY,
    StateSupervisionViolationType.MISDEMEANOR,
    StateSupervisionViolationType.LAW,
    StateSupervisionViolationType.ABSCONDED,
    StateSupervisionViolationType.MUNICIPAL,
    StateSupervisionViolationType.ESCAPED,
    StateSupervisionViolationType.TECHNICAL,
    StateSupervisionViolationType.INTERNAL_UNKNOWN,
    StateSupervisionViolationType.EXTERNAL_UNKNOWN,
]


class StateSpecificViolationDelegate(abc.ABC, StateSpecificDelegate):
    """Interface for state-specific decisions involved in categorizing various
    attributes of violations."""

    # Default values for the severity order of violation subtypes, with defined
    # shorthand values. Variable should be overridden by state-specific implementations
    # if the state has unique subtypes.
    violation_type_and_subtype_shorthand_ordered_map: List[
        Tuple[StateSupervisionViolationType, str, str]
    ] = [
        (violation_type, violation_type.value, violation_type.value.lower())
        for violation_type in DEFAULT_VIOLATION_TYPE_SEVERITY_ORDER
    ]

    def should_include_response_in_violation_history(
        self,
        response: StateSupervisionViolationResponse,
        include_follow_up_responses: bool = False,  # pylint: disable=unused-argument
    ) -> bool:
        """Determines whether the given |response| should be included in violation history analyses.

        Default behavior is to include the response if it is of type VIOLATION_REPORT or CITATION.
        Should be overridden by state-specific implementations if necessary."""
        return response.response_type in (
            StateSupervisionViolationResponseType.VIOLATION_REPORT,
            StateSupervisionViolationResponseType.CITATION,
        )

    def get_violation_type_subtype_strings_for_violation(
        self,
        violation: StateSupervisionViolation,
    ) -> List[str]:
        """Returns a list of strings that represent the violation subtypes present on
        the given |violation|.

        Default behavior is to return a list of the violation_type raw values in the
        violation's supervision_violation_types.

        Should be overridden by state-specific implementations if necessary."""

        supervision_violation_types = violation.supervision_violation_types

        if not supervision_violation_types:
            return []

        return [
            violation_type_entry.violation_type.value
            for violation_type_entry in supervision_violation_types
            if violation_type_entry.violation_type
        ]

    def include_decisions_on_follow_up_responses_for_most_severe_response(self) -> bool:
        """Some StateSupervisionViolationResponses are a 'follow-up' type of response, which is a state-defined response
        that is related to a previously submitted response. This returns whether or not the decision entries on
        follow-up responses should be considered in the calculation of the most severe response decision. By default,
        follow-up responses should not be considered"""
        return False
