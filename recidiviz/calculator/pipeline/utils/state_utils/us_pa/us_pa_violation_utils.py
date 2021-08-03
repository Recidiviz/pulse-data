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

# TODO(#8106): Delete these imports before closing this task
# pylint: disable=protected-access
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_violations_delegate import (
    _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP,
)


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
