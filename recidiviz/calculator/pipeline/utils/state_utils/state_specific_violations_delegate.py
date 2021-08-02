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
