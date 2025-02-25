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
"""Utils for state-specific calculations for Missouri."""
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
    (StateSupervisionViolationType.ABSCONDED.value, 'absc'),
    (StateSupervisionViolationType.MUNICIPAL.value, 'muni'),
    (_LAW_CITATION_SUBTYPE_STR, 'law_cit'),
    (StateSupervisionViolationType.ESCAPED.value, 'esc'),
    (_SUBSTANCE_ABUSE_SUBTYPE_STR, 'subs'),
    (StateSupervisionViolationType.TECHNICAL.value, 'tech')
])


def identify_violation_subtype(violation_type: StateSupervisionViolationType,
                               violations: List[StateSupervisionViolation]) -> Optional[str]:
    """Looks through all provided |violations| of type |violation_type| and returns a string subtype, if necessary."""
    # We only care about subtypes for technical violations
    if violation_type != StateSupervisionViolationType.TECHNICAL:
        return None

    technical_violations = _get_violations_of_type(violations, StateSupervisionViolationType.TECHNICAL)

    for violation in technical_violations:
        for condition in violation.supervision_violated_conditions:
            if condition.condition == _LAW_CITATION_SUBTYPE_STR:
                return _LAW_CITATION_SUBTYPE_STR
            if condition.condition == _SUBSTANCE_ABUSE_CONDITION_STR:
                return _SUBSTANCE_ABUSE_SUBTYPE_STR
    return None


def _get_violations_of_type(violations: List[StateSupervisionViolation], violation_type: StateSupervisionViolationType):
    return [violation for violation in violations if _is_violation_of_type(violation, violation_type)]


def _is_violation_of_type(violation: StateSupervisionViolation, violation_type: StateSupervisionViolationType) -> bool:
    for violation_type_entry in violation.supervision_violation_types:
        if violation_type_entry.violation_type == violation_type:
            return True
    return False


def get_ranked_violation_type_and_subtype_counts(violations: List[StateSupervisionViolation]) -> Dict[str, int]:
    """Given a list of violations, returns a dictionary containing violation type/subtypes and their relative
    frequency. All keys are shorthand versions of the violation type/subtype for ease of display in our front end.
    Entries in the dictionary are ordered by decreasing severity.
    """
    type_and_subtype_counts: Dict[Any, int] = defaultdict(int)

    # Count all violation types and subtypes
    for violation in violations:
        # If all violation types are technical, then we want to replace it with the relevant subtype, when present.
        all_technicals = all([vte.violation_type == StateSupervisionViolationType.TECHNICAL
                              for vte in violation.supervision_violation_types])

        for violation_type_entry in violation.supervision_violation_types:
            violation_type = violation_type_entry.violation_type
            if not violation_type:
                continue
            subtype = identify_violation_subtype(violation_type, [violation])

            to_increment = subtype if subtype and all_technicals else violation_type.value
            type_and_subtype_counts[to_increment] += 1

    # Convert to string shorthand
    ranked_shorthand_counts: Dict[str, int] = OrderedDict()
    for violation_type_or_subtype in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP.keys():
        violation_count = type_and_subtype_counts[violation_type_or_subtype]
        shorthand = _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP[violation_type_or_subtype]
        if violation_count:
            ranked_shorthand_counts[shorthand] = violation_count

    return ranked_shorthand_counts


def _normalize_violations_on_responses_us_mo(response: StateSupervisionViolationResponse) -> \
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
