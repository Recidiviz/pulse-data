# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Helper functions for constants defined in external_id_types"""
import re
from collections import defaultdict

from recidiviz.common.constants.state import external_id_types
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.types import assert_type

# An external ID type is a US_XX state code prefix (captured, so the same regex
# parses the owning state) followed by capital letters and underscores.
EXTERNAL_ID_TYPE_REGEX = re.compile(r"^(?P<state_code>US_[A-Z]{2})[A-Z_]*$")


def is_valid_external_id_type_shape(external_id_type: str) -> bool:
    """Returns True if |external_id_type| is shaped like an external ID type,
    e.g. `US_CO_OFFENDERID`. Every constant in external_id_types.py has this
    shape.

    This is a shape check only, not a membership check against
    external_id_types.py. Where the state an ID type belongs to is known, check
    membership in `external_id_types_by_state_code()[state_code]` instead — that
    catches a typo'd or wrong-state ID type, which this cannot.
    """
    return EXTERNAL_ID_TYPE_REGEX.match(external_id_type) is not None


def get_external_id_types() -> list[str]:
    return [
        assert_type(id_type, str)
        for id_type in dir(external_id_types)
        # Skip built-in variables and Final import
        if not id_type.startswith("__") and not id_type == "Final"
    ]


def external_id_types_by_state_code() -> dict[StateCode, set[str]]:
    result = defaultdict(set)
    for external_id_name in get_external_id_types():
        match = EXTERNAL_ID_TYPE_REGEX.match(external_id_name)
        if not match:
            raise ValueError(
                f"Expected external id name [{external_id_name}] to match "
                f"[{EXTERNAL_ID_TYPE_REGEX.pattern}]."
            )
        result[StateCode[match.group("state_code")]].add(external_id_name)
    return result
