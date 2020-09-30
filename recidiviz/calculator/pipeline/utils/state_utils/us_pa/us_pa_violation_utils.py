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
"""Utils for state-specific calculations related to violations for US_PA."""
import sys
from typing import List, Dict, Tuple

from recidiviz.calculator.pipeline.utils.calculator_utils import safe_list_index
from recidiviz.common.constants.state.state_supervision_violation import StateSupervisionViolationType
from recidiviz.persistence.entity.state.entities import StateSupervisionViolation, StateSupervisionViolationTypeEntry

_LOW_TECHNICAL_SUBTYPE_STR: str = 'LOW_TECH'
_MEDIUM_TECHNICAL_SUBTYPE_STR: str = 'MED_TECH'
_HIGH_TECHNICAL_SUBTYPE_STR: str = 'HIGH_TECH'
_ELECTRONIC_MONITORING_SUBTYPE_STR: str = 'ELEC_MONITORING'
_SUBSTANCE_ABUSE_SUBTYPE_STR: str = 'SUBSTANCE_ABUSE'

_UNSUPPORTED_VIOLATION_SUBTYPE_VALUES = [
    # We don't expect to see this type in US_PA
    StateSupervisionViolationType.ESCAPED.value,
    # We expect all violations of type TECHNICAL to have expected special subtypes
    StateSupervisionViolationType.TECHNICAL.value
]

_VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP: List[Tuple[StateSupervisionViolationType, str, str]] = [
    (StateSupervisionViolationType.FELONY, StateSupervisionViolationType.FELONY.value, 'fel'),
    (StateSupervisionViolationType.MISDEMEANOR, StateSupervisionViolationType.MISDEMEANOR.value, 'misd'),
    (StateSupervisionViolationType.MUNICIPAL, StateSupervisionViolationType.MUNICIPAL.value, 'summ'),
    (StateSupervisionViolationType.TECHNICAL, _HIGH_TECHNICAL_SUBTYPE_STR, 'high_tech'),
    (StateSupervisionViolationType.ABSCONDED, StateSupervisionViolationType.ABSCONDED.value, 'absc'),
    (StateSupervisionViolationType.TECHNICAL, _SUBSTANCE_ABUSE_SUBTYPE_STR, 'subs'),
    (StateSupervisionViolationType.TECHNICAL, _ELECTRONIC_MONITORING_SUBTYPE_STR, 'em'),
    (StateSupervisionViolationType.TECHNICAL, _MEDIUM_TECHNICAL_SUBTYPE_STR, 'med_tech'),
    (StateSupervisionViolationType.TECHNICAL, _LOW_TECHNICAL_SUBTYPE_STR, 'low_tech')
]

_VIOLATION_TYPE_SEVERITY_ORDER = [
    StateSupervisionViolationType.FELONY,
    StateSupervisionViolationType.MISDEMEANOR,
    StateSupervisionViolationType.MUNICIPAL,
    StateSupervisionViolationType.ABSCONDED,
    StateSupervisionViolationType.TECHNICAL
]


_SPECIAL_VIOLATION_TYPE_RAW_STRING_SUBTYPE_MAP: Dict[str, str] = {
    'H03': _SUBSTANCE_ABUSE_SUBTYPE_STR,
    'H12': _SUBSTANCE_ABUSE_SUBTYPE_STR,
    'L02': _SUBSTANCE_ABUSE_SUBTYPE_STR,
    'L08': _SUBSTANCE_ABUSE_SUBTYPE_STR,
    'M03': _SUBSTANCE_ABUSE_SUBTYPE_STR,
    'M14': _SUBSTANCE_ABUSE_SUBTYPE_STR,
    'M16': _ELECTRONIC_MONITORING_SUBTYPE_STR
}


def us_pa_get_violation_type_subtype_strings_for_violation(violation: StateSupervisionViolation) -> List[str]:
    """Returns a list of strings that represent the violation subtypes present on the given |violation|."""
    violation_type_list: List[str] = []

    supervision_violation_types = violation.supervision_violation_types

    if not supervision_violation_types:
        return violation_type_list

    for violation_type_entry in violation.supervision_violation_types:
        if violation_type_entry.violation_type and \
                violation_type_entry.violation_type != StateSupervisionViolationType.TECHNICAL:
            violation_type_list.append(violation_type_entry.violation_type.value)
        else:
            violation_type_list.append(_violation_subtype_from_violation_type_entry(violation_type_entry))
    return violation_type_list


def us_pa_sorted_violation_subtypes_by_severity(violation_subtypes: List[str]) -> List[str]:
    """Returns the list of |violation_subtypes| sorted in order of the subtype values in the
    _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP."""
    subtype_sort_order = [
        subtype for _, subtype, _ in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP
    ]

    sorted_violation_subtypes = sorted(violation_subtypes,
                                       key=lambda subtype: safe_list_index(subtype_sort_order, subtype, sys.maxsize))

    return sorted_violation_subtypes


def us_pa_violation_type_from_subtype(violation_subtype: str) -> StateSupervisionViolationType:
    """Determines which StateSupervisionViolationType corresponds to the |violation_subtype| value."""
    for violation_type, violation_subtype_value, _ in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP:
        if violation_subtype == violation_subtype_value:
            return violation_type

    raise ValueError(f"Unexpected violation_subtype {violation_subtype} for US_PA.")


def us_pa_shorthand_for_violation_subtype(violation_subtype: str) -> str:
    """Returns the shorthand string corresponding to the |violation_subtype| value."""
    for _, violation_subtype_value, subtype_shorthand in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP:
        if violation_subtype == violation_subtype_value:
            return subtype_shorthand

    raise ValueError(f"Unexpected violation_subtype {violation_subtype} for US_PA.")


def _violation_subtype_from_violation_type_entry(violation_type_entry: StateSupervisionViolationTypeEntry) -> str:
    """Returns the subtype on the |violation_type_entry|. Fails if this does not have a violation_type_raw_text value
    that we expect to see for TECHNICAL violations in US_PA."""
    violation_type_raw_text = violation_type_entry.violation_type_raw_text

    if not violation_type_raw_text:
        # This should never happen
        raise ValueError("Unexpected null violation_type_raw_text.")

    special_subtype = _SPECIAL_VIOLATION_TYPE_RAW_STRING_SUBTYPE_MAP.get(violation_type_raw_text)

    if special_subtype:
        return special_subtype
    if violation_type_raw_text.startswith('L'):
        return _LOW_TECHNICAL_SUBTYPE_STR
    if violation_type_raw_text.startswith('M'):
        return _MEDIUM_TECHNICAL_SUBTYPE_STR
    if violation_type_raw_text.startswith('H'):
        return _HIGH_TECHNICAL_SUBTYPE_STR
    raise ValueError(f"Unexpected violation_type_raw_text: {violation_type_raw_text}")
