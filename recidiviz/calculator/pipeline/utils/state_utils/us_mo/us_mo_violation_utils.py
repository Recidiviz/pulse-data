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
from collections import defaultdict, OrderedDict
from typing import List, Optional, Dict, Any

from recidiviz.common.constants.state.state_supervision_violation import StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response import StateSupervisionViolationResponseType
from recidiviz.persistence.entity.state.entities import StateSupervisionViolation, StateSupervisionViolationResponse, \
    StateSupervisionViolationTypeEntry

_SUBSTANCE_ABUSE_CONDITION_STR = 'DRG'
_LAW_CONDITION_STR = 'LAW'

_SUBSTANCE_ABUSE_SUBTYPE_STR: str = 'SUBSTANCE_ABUSE'
_LAW_CITATION_SUBTYPE_STR: str = 'LAW_CITATION'

_VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP = OrderedDict([
    (StateSupervisionViolationType.FELONY.value, 'fel'),
    (StateSupervisionViolationType.MISDEMEANOR.value, 'misd'),
    (_LAW_CITATION_SUBTYPE_STR, 'law_cit'),
    (StateSupervisionViolationType.ABSCONDED.value, 'absc'),
    (StateSupervisionViolationType.MUNICIPAL.value, 'muni'),
    (StateSupervisionViolationType.ESCAPED.value, 'esc'),
    (_SUBSTANCE_ABUSE_SUBTYPE_STR, 'subs'),
    (StateSupervisionViolationType.TECHNICAL.value, 'tech')
])


def us_mo_filter_violation_responses(violation_responses: List[StateSupervisionViolationResponse]) -> \
        List[StateSupervisionViolationResponse]:
    """Filters out VIOLATION_REPORT responses that are not of type INI (Initial) or ITR (Inter district)."""
    filtered_responses = [
        response for response in violation_responses
        if response.response_type != StateSupervisionViolationResponseType.VIOLATION_REPORT
        or _get_violation_report_subtype_should_be_included_in_calculations(response.response_subtype)
    ]

    return filtered_responses


def _get_violation_report_subtype_should_be_included_in_calculations(response_subtype: Optional[str]) -> bool:
    """Returns whether a VIOLATION_REPORT with the given response_subtype should be included in calculations."""
    if response_subtype in ['INI', 'ITR']:
        return True
    if response_subtype is None or response_subtype in ['HOF', 'MOS', 'ORI', 'SUP']:
        return False

    raise ValueError(f"Unexpected violation response subtype {response_subtype} for a US_MO VIOLATION_REPORT.")


def us_mo_identify_most_severe_violation_type(violations: List[StateSupervisionViolation],
                                              violation_type_severity_order: List[StateSupervisionViolationType]) -> \
        Optional[StateSupervisionViolationType]:
    """Identifies the most severe violation type on the violation according to the violation type severity order
    ranking. Classifies LAW_CITATION violations as severe as a MISDEMEANOR."""
    has_law_citation = False

    violation_types = []
    for violation in violations:
        for violation_type_entry in violation.supervision_violation_types:
            violation_types.append(violation_type_entry.violation_type)

            if violation_type_entry.violation_type == StateSupervisionViolationType.TECHNICAL:
                technical_subtype = us_mo_identify_violation_subtype(StateSupervisionViolationType.TECHNICAL,
                                                                     [violation])

                if technical_subtype == _LAW_CITATION_SUBTYPE_STR:
                    has_law_citation = True

    most_severe_type = next((violation_type for violation_type in violation_type_severity_order
                             if violation_type in violation_types), None)

    if (most_severe_type not in (StateSupervisionViolationType.FELONY, StateSupervisionViolationType.MISDEMEANOR)
            and has_law_citation):
        # The most severe violation type was a LAW_CITATION. Return TECHNICAL so that TECHNICAL-LAW_CITATION will be
        # this person's most severe violation type and subtype.
        return StateSupervisionViolationType.TECHNICAL

    return most_severe_type


def us_mo_identify_violation_subtype(violation_type: StateSupervisionViolationType,
                                     violations: List[StateSupervisionViolation]) -> Optional[str]:
    """Looks through all provided |violations| of type |violation_type| and returns a string subtype, if necessary."""
    # We only care about subtypes for technical violations
    if violation_type != StateSupervisionViolationType.TECHNICAL:
        return None

    technical_violations = _get_violations_of_type(violations, StateSupervisionViolationType.TECHNICAL)

    all_conditions = [
        condition.condition for violation in technical_violations
        for condition in violation.supervision_violated_conditions
    ]

    if _LAW_CITATION_SUBTYPE_STR in all_conditions:
        return _LAW_CITATION_SUBTYPE_STR

    if _SUBSTANCE_ABUSE_CONDITION_STR in all_conditions:
        return _SUBSTANCE_ABUSE_SUBTYPE_STR

    return None


def _get_violations_of_type(violations: List[StateSupervisionViolation], violation_type: StateSupervisionViolationType):
    return [violation for violation in violations if _is_violation_of_type(violation, violation_type)]


def _is_violation_of_type(violation: StateSupervisionViolation, violation_type: StateSupervisionViolationType) -> bool:
    for violation_type_entry in violation.supervision_violation_types:
        if violation_type_entry.violation_type == violation_type:
            return True
    return False


def get_ranked_violation_type_and_subtype_counts(violations: List[StateSupervisionViolation],
                                                 violation_type_severity_order: List[StateSupervisionViolationType]) \
        -> Dict[str, int]:
    """Given a list of violations, returns a dictionary containing violation type/subtypes and their relative
    frequency as the most severe violation type listed on a report. All keys are shorthand versions of the violation
    type/subtype for ease of display in our front end. Entries in the dictionary are ordered by decreasing severity.
    """
    type_and_subtype_counts: Dict[Any, int] = defaultdict(int)

    # Count all violation types and subtypes
    for violation in violations:
        most_severe_violation_type = us_mo_identify_most_severe_violation_type([violation],
                                                                               violation_type_severity_order)

        if not most_severe_violation_type:
            continue

        is_technical = most_severe_violation_type == StateSupervisionViolationType.TECHNICAL

        subtype = us_mo_identify_violation_subtype(most_severe_violation_type, [violation])

        to_increment = subtype if subtype and is_technical else most_severe_violation_type.value
        type_and_subtype_counts[to_increment] += 1

    # Convert to string shorthand
    ranked_shorthand_counts: Dict[str, int] = OrderedDict()
    for violation_type_or_subtype in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP.keys():
        violation_count = type_and_subtype_counts[violation_type_or_subtype]
        shorthand = _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP[violation_type_or_subtype]
        if violation_count:
            ranked_shorthand_counts[shorthand] = violation_count

    return ranked_shorthand_counts


def normalize_violations_on_responses(response: StateSupervisionViolationResponse) -> \
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
