# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utils for state-specific calculations related to violations for US_MO."""
import sys
from typing import List, Optional, Tuple

from recidiviz.calculator.pipeline.utils.calculator_utils import safe_list_index
from recidiviz.common.constants.state.state_supervision_violation import StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response import StateSupervisionViolationResponseType
from recidiviz.persistence.entity.state.entities import StateSupervisionViolation, StateSupervisionViolationResponse, \
    StateSupervisionViolationTypeEntry

_SUBSTANCE_ABUSE_CONDITION_STR = 'DRG'
_LAW_CONDITION_STR = 'LAW'

_LAW_CITATION_SUBTYPE_STR: str = 'LAW_CITATION'
_SUBSTANCE_ABUSE_SUBTYPE_STR: str = 'SUBSTANCE_ABUSE'

_UNSUPPORTED_VIOLATION_SUBTYPE_VALUES = [
    # We don't expect to see these types in US_MO
    StateSupervisionViolationType.LAW.value,
]

_VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP: List[Tuple[StateSupervisionViolationType, str, str]] = [
    (StateSupervisionViolationType.FELONY, StateSupervisionViolationType.FELONY.value, 'fel'),
    (StateSupervisionViolationType.MISDEMEANOR, StateSupervisionViolationType.MISDEMEANOR.value, 'misd'),
    (StateSupervisionViolationType.TECHNICAL, _LAW_CITATION_SUBTYPE_STR, 'law_cit'),
    (StateSupervisionViolationType.ABSCONDED, StateSupervisionViolationType.ABSCONDED.value, 'absc'),
    (StateSupervisionViolationType.MUNICIPAL, StateSupervisionViolationType.MUNICIPAL.value, 'muni'),
    (StateSupervisionViolationType.ESCAPED, StateSupervisionViolationType.ESCAPED.value, 'esc'),
    (StateSupervisionViolationType.TECHNICAL, _SUBSTANCE_ABUSE_SUBTYPE_STR, 'subs'),
    (StateSupervisionViolationType.TECHNICAL, StateSupervisionViolationType.TECHNICAL.value, 'tech')
]

FOLLOW_UP_RESPONSE_SUBTYPE = 'SUP'


def us_mo_filter_violation_responses(violation_responses: List[StateSupervisionViolationResponse],
                                     include_follow_up_responses: bool) -> \
        List[StateSupervisionViolationResponse]:
    """Filters out VIOLATION_REPORT responses that are not of type INI (Initial) or ITR (Inter district)."""
    filtered_responses = [
        response for response in violation_responses
        if response.response_type != StateSupervisionViolationResponseType.VIOLATION_REPORT
        or _get_violation_report_subtype_should_be_included_in_calculations(
            response.response_subtype, include_follow_up_responses)
    ]

    return filtered_responses


def _get_violation_report_subtype_should_be_included_in_calculations(response_subtype: Optional[str],
                                                                     include_follow_up_responses) -> bool:
    """Returns whether a VIOLATION_REPORT with the given response_subtype should be included in calculations."""
    if response_subtype in ['INI', 'ITR']:
        return True
    if response_subtype == FOLLOW_UP_RESPONSE_SUBTYPE:
        return include_follow_up_responses
    if response_subtype is None or response_subtype in ['HOF', 'MOS', 'ORI']:
        return False

    raise ValueError(f"Unexpected violation response subtype {response_subtype} for a US_MO VIOLATION_REPORT.")


def us_mo_prepare_violation_responses_for_calculations(
        violation_responses: List[StateSupervisionViolationResponse]) -> List[StateSupervisionViolationResponse]:
    """Performs required pre-processing on US_MO StateSupervisionViolationResponses."""
    return [
        _normalize_violations_on_responses(response)
        for response in violation_responses
    ]


def _normalize_violations_on_responses(response: StateSupervisionViolationResponse) -> \
        StateSupervisionViolationResponse:
    """For responses that are not of type CITATION or don't have an associated violation, does nothing. Responses of
    type CITATION do not have violation types on their violations, so the violation types and conditions violated on
    these violations are updated."""
    if not response.state_code or response.state_code.upper() != 'US_MO':
        raise ValueError(f"Calling state-specific US_MO helper function for a response from {response.state_code}")

    # Citations in US_MO don't have violation types on them. However, we want to classify them as TECHNICAL.
    # If this is a CITATION and there's a violation on it, add TECHNICAL to the list of violation types.
    if response.response_type == StateSupervisionViolationResponseType.CITATION and response.supervision_violation:
        supervision_violation = response.supervision_violation

        if not supervision_violation.supervision_violation_types:
            supervision_violation.supervision_violation_types.append(
                StateSupervisionViolationTypeEntry(
                    state_code=response.state_code,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text=None
                )
            )

        # Update citations that list the _LAW_CONDITION_STR as a condition to instead list _LAW_CITATION_SUBTYPE_STR
        # so we can track law citations independently from other violations with LAW conditions on them
        for condition_entry in supervision_violation.supervision_violated_conditions:
            if condition_entry.condition == _LAW_CONDITION_STR:
                condition_entry.condition = _LAW_CITATION_SUBTYPE_STR

    return response


def us_mo_get_violation_type_subtype_strings_for_violation(violation: StateSupervisionViolation) -> List[str]:
    """Returns a list of strings that represent the violation subtypes present on the given |violation|."""
    violation_type_list: List[str] = []

    includes_technical_violation = False
    includes_special_case_violation_subtype = False

    supervision_violation_types = violation.supervision_violation_types

    if not supervision_violation_types:
        return violation_type_list

    for violation_type_entry in violation.supervision_violation_types:
        if violation_type_entry.violation_type and \
                violation_type_entry.violation_type != StateSupervisionViolationType.TECHNICAL:
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


def us_mo_sorted_violation_subtypes_by_severity(violation_subtypes: List[str]) -> List[str]:
    """Returns the list of |violation_subtypes| sorted in order of the subtype values in the
    _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP."""
    subtype_sort_order = [
        subtype for _, subtype, _ in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP
    ]

    sorted_violation_subtypes = sorted(violation_subtypes,
                                       key=lambda subtype: safe_list_index(subtype_sort_order, subtype, sys.maxsize))

    return sorted_violation_subtypes


def us_mo_violation_type_from_subtype(violation_subtype: str) -> StateSupervisionViolationType:
    """Determines which StateSupervisionViolationType corresponds to the |violation_subtype| value."""
    for violation_type, violation_subtype_value, _ in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP:
        if violation_subtype == violation_subtype_value:
            return violation_type

    raise ValueError(f"Unexpected violation_subtype {violation_subtype} for US_MO.")


def us_mo_shorthand_for_violation_subtype(violation_subtype: str) -> str:
    """Returns the shorthand string corresponding to the |violation_subtype| value."""
    for _, violation_subtype_value, subtype_shorthand in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP:
        if violation_subtype == violation_subtype_value:
            return subtype_shorthand

    raise ValueError(f"Unexpected violation_subtype {violation_subtype} for US_MO.")
