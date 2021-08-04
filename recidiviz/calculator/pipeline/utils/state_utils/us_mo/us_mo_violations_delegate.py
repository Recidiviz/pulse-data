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
"""Utils for state-specific logic related to violations
in US_MO."""
from typing import List, Optional, Tuple

from recidiviz.calculator.pipeline.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)

FOLLOW_UP_RESPONSE_SUBTYPE = "SUP"

_SUBSTANCE_ABUSE_CONDITION_STR = "DRG"
_LAW_CONDITION_STR = "LAW"

_LAW_CITATION_SUBTYPE_STR: str = "LAW_CITATION"
_SUBSTANCE_ABUSE_SUBTYPE_STR: str = "SUBSTANCE_ABUSE"

_UNSUPPORTED_VIOLATION_SUBTYPE_VALUES = [
    # We don't expect to see these types in US_MO
    StateSupervisionViolationType.LAW.value,
]

_VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP: List[
    Tuple[StateSupervisionViolationType, str, str]
] = [
    (
        StateSupervisionViolationType.FELONY,
        StateSupervisionViolationType.FELONY.value,
        "fel",
    ),
    (
        StateSupervisionViolationType.MISDEMEANOR,
        StateSupervisionViolationType.MISDEMEANOR.value,
        "misd",
    ),
    (StateSupervisionViolationType.TECHNICAL, _LAW_CITATION_SUBTYPE_STR, "law_cit"),
    (
        StateSupervisionViolationType.ABSCONDED,
        StateSupervisionViolationType.ABSCONDED.value,
        "absc",
    ),
    (
        StateSupervisionViolationType.MUNICIPAL,
        StateSupervisionViolationType.MUNICIPAL.value,
        "muni",
    ),
    (
        StateSupervisionViolationType.ESCAPED,
        StateSupervisionViolationType.ESCAPED.value,
        "esc",
    ),
    (StateSupervisionViolationType.TECHNICAL, _SUBSTANCE_ABUSE_SUBTYPE_STR, "subs"),
    (
        StateSupervisionViolationType.TECHNICAL,
        StateSupervisionViolationType.TECHNICAL.value,
        "tech",
    ),
]


class UsMoViolationDelegate(StateSpecificViolationDelegate):
    """US_MO implementation of the StateSpecificViolationDelegate."""

    # US_MO has state-specific violation subtypes
    violation_type_and_subtype_shorthand_ordered_map = (
        _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP
    )

    def should_include_response_in_violation_history(
        self,
        response: StateSupervisionViolationResponse,
        include_follow_up_responses: bool = False,
    ) -> bool:
        """For US_MO, we include all responses of type CITATION, and certain VIOLATION_REPORT responses depending on
        the report subtype and whether follow-up responses should be included in the violation history."""
        return (
            response.response_type == StateSupervisionViolationResponseType.CITATION
            or (
                response.response_type
                == StateSupervisionViolationResponseType.VIOLATION_REPORT
                and _get_violation_report_subtype_should_be_included_in_calculations(
                    response.response_subtype, include_follow_up_responses
                )
            )
        )

    def get_violation_type_subtype_strings_for_violation(
        self,
        violation: StateSupervisionViolation,
    ) -> List[str]:
        """Returns a list of strings that represent the violation subtypes present on
        the given |violation|.

        For all non-TECHNICAL violation types on the |violation|, includes the raw
        violation_type value string. If the |violation| includes a TECHNICAL
        violation type, looks at the conditions violated to see if the violation
        contains substance abuse or law citation subtypes. If the violation contains a
        TECHNICAL violation_type without either a SUBSTANCE_ABUSE or LAW_CITATION
        condition, then the general 'TECHNICAL' value is included in the list. If the
        violation does contain one or more of these TECHNICAL subtypes,
        then the subtype values are included in the list and 'TECHNICAL' is not
        included.
        """
        violation_type_list: List[str] = []

        includes_technical_violation = False
        includes_special_case_violation_subtype = False

        supervision_violation_types = violation.supervision_violation_types

        if not supervision_violation_types:
            return violation_type_list

        for violation_type_entry in violation.supervision_violation_types:
            if (
                violation_type_entry.violation_type
                and violation_type_entry.violation_type
                != StateSupervisionViolationType.TECHNICAL
            ):
                violation_type_list.append(violation_type_entry.violation_type.value)
            else:
                includes_technical_violation = True

        for condition_entry in violation.supervision_violated_conditions:
            condition = condition_entry.condition
            if condition:
                if condition.upper() == _SUBSTANCE_ABUSE_CONDITION_STR:
                    violation_type_list.append(_SUBSTANCE_ABUSE_SUBTYPE_STR)
                    includes_special_case_violation_subtype = True
                else:
                    if condition.upper() == _LAW_CITATION_SUBTYPE_STR:
                        includes_special_case_violation_subtype = True

                    # Condition values are free text so we standardize all to be upper case
                    violation_type_list.append(condition.upper())

        # If this is a TECHNICAL violation without either a SUBSTANCE_ABUSE or LAW_CITATION condition, then add
        # 'TECHNICAL' to the type list so that this will get classified as a TECHNICAL violation
        if includes_technical_violation and not includes_special_case_violation_subtype:
            violation_type_list.append(StateSupervisionViolationType.TECHNICAL.value)

        return violation_type_list

    def include_decisions_on_follow_up_responses_for_most_severe_response(self) -> bool:
        """Some StateSupervisionViolationResponses are a 'follow-up' type of response, which is a state-defined response
        that is related to a previously submitted response. This returns whether or not the decision entries on
        follow-up responses should be considered in the calculation of the most severe response decision. For MO, follow-up
        responses should be considered."""
        return True


def _get_violation_report_subtype_should_be_included_in_calculations(
    response_subtype: Optional[str], include_follow_up_responses: bool
) -> bool:
    """Returns whether a VIOLATION_REPORT with the given response_subtype should be included in calculations."""
    if response_subtype in ["INI", "ITR"]:
        return True
    if response_subtype == FOLLOW_UP_RESPONSE_SUBTYPE:
        return include_follow_up_responses
    if response_subtype is None or response_subtype in ["HOF", "MOS", "ORI"]:
        return False

    raise ValueError(
        f"Unexpected violation response subtype {response_subtype} for a US_MO VIOLATION_REPORT."
    )
