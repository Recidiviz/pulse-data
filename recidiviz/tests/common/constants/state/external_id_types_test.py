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
from recidiviz.ingest.direct.external_id_type_helpers import get_external_id_types


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
