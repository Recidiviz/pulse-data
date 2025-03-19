# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for the constants defined in external_id_types"""

import re
import unittest
from collections import defaultdict
from typing import List

from recidiviz.common.constants.state import external_id_types
from recidiviz.common.constants.states import StateCode


def get_external_id_types() -> List[str]:
    return [
        var_name
        for var_name in dir(external_id_types)
        # Skip built-in variables
        if not var_name.startswith("__")
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


class ExternalIdTypeTest(unittest.TestCase):
    """Tests reasonableness of all the external ids"""

    def test_starts_with_us_xx(self) -> None:
        for var_name in get_external_id_types():
            if not re.match(r"^US_[A-Z]{2}", var_name):
                raise ValueError(
                    f"Found variable in external_id_types.py that does not start"
                    f" with a state code in US_XX format: {var_name}"
                )

    def test_only_letters_and_underscores_in_names(self) -> None:
        for var_name in get_external_id_types():
            var_value = getattr(external_id_types, var_name)
            if not re.match(r"^[A-Z_]+$", var_value):
                raise ValueError(
                    f"Found type [{var_value}] in external_id_types.py that does not "
                    f"match expected pattern - name should only have capital letters"
                    f"and underscores."
                )

    def test_variable_name_matches_value(self) -> None:
        for var_name in get_external_id_types():
            var_value = getattr(external_id_types, var_name)

            # Check that the variable value and name are equivalent
            self.assertEqual(var_name, var_value)
