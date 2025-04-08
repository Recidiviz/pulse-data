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


def get_external_id_types() -> list[str]:
    return [
        assert_type(id_type, str)
        for id_type in dir(external_id_types)
        # Skip built-in variables
        if not id_type.startswith("__")
    ]


def external_id_types_by_state_code() -> dict[StateCode, set[str]]:
    result = defaultdict(set)
    for external_id_name in get_external_id_types():
        match = re.match(r"^US_[A-Z]{2}", external_id_name)
        if not match:
            raise ValueError(
                f"Expected external id name to match regex: {external_id_name}"
            )
        result[StateCode[match.group(0)]].add(external_id_name)
    return result
