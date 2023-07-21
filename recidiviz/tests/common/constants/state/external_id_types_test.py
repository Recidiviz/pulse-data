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

from recidiviz.common.constants.state import external_id_types
from recidiviz.common.constants.states import STATE_CODE_PATTERN


class ExternalIdTypeTest(unittest.TestCase):
    """Tests reasonableness of all the external ids"""

    def test_starts_with_us_xx(self) -> None:
        for var_name in dir(external_id_types):
            if var_name.startswith("__"):
                # Skip built-in variables
                continue

            if not re.match(STATE_CODE_PATTERN, var_name):
                raise ValueError(
                    f"Found variable in external_id_types.py that does not start"
                    f" with a state code in US_XX format: {var_name}"
                )

            var_value = getattr(external_id_types, var_name)

            # Handle legacy case where variable name and value don't match
            # TODO(#22448): Rename US_PA_CONTROL variable to US_PA_CONT and remove this check
            if var_value == "US_PA_CONT":
                self.assertEqual(var_name, "US_PA_CONTROL")
                continue

            # Check that the variable value and name are equivalent
            self.assertEqual(var_name, var_value)
