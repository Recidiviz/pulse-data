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
from typing import List, Set

from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationType,
)

DEFAULT_VIOLATION_SUBTYPE_SEVERITY_ORDER: List[str] = [
    StateSupervisionViolationType.FELONY.value,
    StateSupervisionViolationType.MISDEMEANOR.value,
    StateSupervisionViolationType.LAW.value,
    StateSupervisionViolationType.ABSCONDED.value,
    StateSupervisionViolationType.MUNICIPAL.value,
    StateSupervisionViolationType.ESCAPED.value,
    StateSupervisionViolationType.TECHNICAL.value,
]


class StateSpecificViolationDelegate(abc.ABC):
    """Interface for state-specific decisions involved in categorizing various
    attributes of violations."""

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

    def violation_type_subtypes_with_violation_type_mappings(self) -> Set[str]:
        """Returns the set of violation_type_subtype values that have a defined mapping
        to a violation_type value.

        Default subtypes with mapping to violation_type values are just the raw values of the
        StateSupervisionViolationType enum.

        Should be overridden by state-specific implementations if necessary.
        """
        return {
            violation_type.value for violation_type in StateSupervisionViolationType
        }

    def get_violation_subtype_severity_order(self) -> List[str]:
        """Returns the sort order of violation subtypes by severity. Default behavior is to follow
        DEFAULT_VIOLATION_SUBTYPE_SEVERITY_ORDER"""
        return DEFAULT_VIOLATION_SUBTYPE_SEVERITY_ORDER

    def violation_type_from_subtype(
        self, violation_subtype: str
    ) -> StateSupervisionViolationType:
        """Determines which StateSupervisionViolationType corresponds to the |violation_subtype| value for the given
        |state_code|. Default behavior is to return the StateSupervisionViolationType corresponding to
        the |violation_subtype|."""

        for violation_type in StateSupervisionViolationType:
            if violation_subtype == violation_type.value:
                return violation_type

        raise ValueError(f"Unexpected violation_subtype {violation_subtype}.")

    def shorthand_for_violation_subtype(self, violation_subtype: str) -> str:
        """Returns the shorthand string representing the given |violation_subtype| in the given |state_code|. Default
        behavior is to return a lowercase version of the |violation_subtype| string."""

        return violation_subtype.lower()

    def include_decisions_on_follow_up_responses_for_most_severe_response(self) -> bool:
        """Some StateSupervisionViolationResponses are a 'follow-up' type of response, which is a state-defined response
        that is related to a previously submitted response. This returns whether or not the decision entries on
        follow-up responses should be considered in the calculation of the most severe response decision. By default,
        follow-up responses should not be considered"""
        return False
