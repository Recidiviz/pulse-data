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
from recidiviz.persistence.entity.state.entities import StateSupervisionViolation

_SUBSTANCE_ABUSE_SUBTYPE_STR = 'SUBSTANCE_ABUSE'

_ORDERED_VIOLATION_TYPES_AND_SUBTYPES: List[Any] = [
    StateSupervisionViolationType.FELONY,
    StateSupervisionViolationType.MISDEMEANOR,
    StateSupervisionViolationType.ABSCONDED,
    StateSupervisionViolationType.MUNICIPAL,
    StateSupervisionViolationType.ESCAPED,
    _SUBSTANCE_ABUSE_SUBTYPE_STR,
    StateSupervisionViolationType.TECHNICAL
]

_VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_MAP: Dict[Any, str] = {
    StateSupervisionViolationType.FELONY: 'fel',
    StateSupervisionViolationType.MISDEMEANOR: 'misd',
    StateSupervisionViolationType.ABSCONDED: 'absc',
    StateSupervisionViolationType.MUNICIPAL: 'muni',
    StateSupervisionViolationType.ESCAPED: 'esc',
    StateSupervisionViolationType.TECHNICAL: 'tech',
    _SUBSTANCE_ABUSE_SUBTYPE_STR: 'subs',
}


def identify_violation_subtype(violation_type: StateSupervisionViolationType,
                               violations: List[StateSupervisionViolation]) -> Optional[str]:
    """Looks through all provided |violations| of type |violation_type| and returns a string subtype, if necessary."""
    # We only care about subtypes for technical violations
    if violation_type != StateSupervisionViolationType.TECHNICAL:
        return None

    technical_violations = _get_violations_of_type(violations, StateSupervisionViolationType.TECHNICAL)

    for violation in technical_violations:
        for condition in violation.supervision_violated_conditions:
            if condition.condition == 'DRG':
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

            to_increment = subtype if subtype and all_technicals else violation_type
            type_and_subtype_counts[to_increment] += 1

    # Convert to string shorthand
    ranked_shorthand_counts: Dict[str, int] = OrderedDict()
    for violation_type_or_subtype in _ORDERED_VIOLATION_TYPES_AND_SUBTYPES:
        violation_count = type_and_subtype_counts[violation_type_or_subtype]
        shorthand = _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_MAP[violation_type_or_subtype]
        if violation_count:
            ranked_shorthand_counts[shorthand] = violation_count

    return ranked_shorthand_counts
