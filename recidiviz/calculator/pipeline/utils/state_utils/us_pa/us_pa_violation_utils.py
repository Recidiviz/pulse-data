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
from typing import List

from recidiviz.calculator.pipeline.utils.calculator_utils import safe_list_index

# TODO(#8106): Delete these imports before closing this task
# pylint: disable=protected-access
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_violations_delegate import (
    _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)


def us_pa_sorted_violation_subtypes_by_severity(
    violation_subtypes: List[str],
) -> List[str]:
    """Returns the list of |violation_subtypes| sorted in order of the subtype values in the
    _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP."""
    subtype_sort_order = [
        subtype for _, subtype, _ in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP
    ]

    sorted_violation_subtypes = sorted(
        violation_subtypes,
        key=lambda subtype: safe_list_index(subtype_sort_order, subtype, sys.maxsize),
    )

    return sorted_violation_subtypes


def us_pa_violation_type_from_subtype(
    violation_subtype: str,
) -> StateSupervisionViolationType:
    """Determines which StateSupervisionViolationType corresponds to the |violation_subtype| value."""
    for (
        violation_type,
        violation_subtype_value,
        _,
    ) in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP:
        if violation_subtype == violation_subtype_value:
            return violation_type

    raise ValueError(f"Unexpected violation_subtype {violation_subtype} for US_PA.")


def us_pa_shorthand_for_violation_subtype(violation_subtype: str) -> str:
    """Returns the shorthand string corresponding to the |violation_subtype| value."""
    for (
        _,
        violation_subtype_value,
        subtype_shorthand,
    ) in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP:
        if violation_subtype == violation_subtype_value:
            return subtype_shorthand

    raise ValueError(f"Unexpected violation_subtype {violation_subtype} for US_PA.")
