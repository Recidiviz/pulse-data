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
"""Utils for working with StateSupervisionViolations and their related entities."""
from collections import OrderedDict
from typing import List, Optional, Dict, Tuple

from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import \
    shorthand_for_violation_subtype, sorted_violation_subtypes_by_severity,\
    get_violation_type_subtype_strings_for_violation, violation_type_from_subtype
from recidiviz.common.constants.state.state_supervision_violation import StateSupervisionViolationType
from recidiviz.persistence.entity.entity_utils import get_single_state_code
from recidiviz.persistence.entity.state.entities import StateSupervisionViolation


SUBSTANCE_ABUSE_SUBTYPE_STR: str = 'SUBSTANCE_ABUSE'

DEFAULT_VIOLATION_SUBTYPE_SEVERITY_ORDER = [
    StateSupervisionViolationType.FELONY.value,
    StateSupervisionViolationType.MISDEMEANOR.value,
    StateSupervisionViolationType.ABSCONDED.value,
    StateSupervisionViolationType.MUNICIPAL.value,
    StateSupervisionViolationType.ESCAPED.value,
    StateSupervisionViolationType.TECHNICAL.value
]


def get_violations_of_type(violations: List[StateSupervisionViolation], violation_type: StateSupervisionViolationType):
    """Returns the violations in |violations| that contain a StateSupervisionViolationTypeEntry with a violation_type
    matching |violation_type|."""
    return [violation for violation in violations if is_violation_of_type(violation, violation_type)]


def is_violation_of_type(violation: StateSupervisionViolation, violation_type: StateSupervisionViolationType) -> bool:
    """Determined whether the violation_type on any of the violation type entries on the |violation| match the given
    |violation_type|."""
    for violation_type_entry in violation.supervision_violation_types:
        if violation_type_entry.violation_type == violation_type:
            return True
    return False


def shorthand_description_for_ranked_violation_counts(state_code: str, subtype_counts: Dict[str, int]) -> \
        Optional[str]:
    """Converts the dictionary mapping types of violations to the number of that type into a string listing the types
    and counts, ordered by the violation type severity defined by state-specific logic. If there aren't any counts of
    any violations, returns None."""
    sorted_subtypes = sorted_violation_subtypes_by_severity(
        state_code, list(subtype_counts.keys()), DEFAULT_VIOLATION_SUBTYPE_SEVERITY_ORDER)

    if not sorted_subtypes:
        return None

    ranked_shorthand_counts: Dict[str, int] = OrderedDict()
    for subtype in sorted_subtypes:
        violation_count = subtype_counts[subtype]
        if violation_count:
            # Convert to string shorthand
            ranked_shorthand_counts[shorthand_for_violation_subtype(state_code, subtype)] = violation_count

    descriptions = [f"{count}{label}" for label, count in ranked_shorthand_counts.items() if count > 0]

    if descriptions:
        return ';'.join(descriptions)

    return None


def identify_most_severe_violation_type_and_subtype(violations: List[StateSupervisionViolation]) \
        -> Tuple[Optional[StateSupervisionViolationType], Optional[str]]:
    """Identifies the most severe violation type on the provided |violations|, and, if relevant, the subtype of that
    most severe violation type. Returns both as a tuple.
    """
    violation_subtypes: List[str] = []

    if not violations:
        return None, None

    state_code = get_single_state_code(violations)

    for violation in violations:
        violation_subtypes.extend(get_violation_type_subtype_strings_for_violation(violation))

    if not violation_subtypes:
        return None, None

    most_severe_subtype = most_severe_violation_subtype(
        state_code, violation_subtypes, DEFAULT_VIOLATION_SUBTYPE_SEVERITY_ORDER)

    most_severe_type = None

    if most_severe_subtype:
        most_severe_type = violation_type_from_subtype(state_code, most_severe_subtype)

    return most_severe_type, most_severe_subtype


def most_severe_violation_subtype(state_code: str,
                                  violation_subtypes: List[str],
                                  default_severity_order: List[str]) -> Optional[str]:
    """Given the |state_code| and list of |violation_subtypes|, determines the most severe subtype present. Defers to
    the severity in the |default_severity_order| if no state-specific logic is implemented."""
    if not violation_subtypes:
        return None

    sorted_subtypes = sorted_violation_subtypes_by_severity(state_code, violation_subtypes, default_severity_order)

    if sorted_subtypes:
        return sorted_subtypes[0]

    return None


def get_violation_type_frequency_counter(violations: List[StateSupervisionViolation]) -> Optional[List[List[str]]]:
    """For every violation in violations, builds a list of strings, where each string is a violation type or a
    condition violated that is recorded on the given violation. Returns a list of all lists of strings, where the length
    of the list is the number of violations."""

    violation_type_frequency_counter: List[List[str]] = []

    for violation in violations:
        violation_types = get_violation_type_subtype_strings_for_violation(violation)

        if violation_types:
            violation_type_frequency_counter.append(violation_types)

    return violation_type_frequency_counter if violation_type_frequency_counter else None
