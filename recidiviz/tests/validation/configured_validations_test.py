# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Unit test for all configured data validations"""
import unittest

import attr

from recidiviz.validation.configured_validations import get_all_validations


class TestConfiguredValidations(unittest.TestCase):
    """Unit test for all configured data validations"""

    def test_configured_validations_all_compile(self) -> None:
        validations = get_all_validations()
        for validation in validations:
            try:
                attr.validate(validation)
            except Exception as e:
                self.fail(
                    f"{validation.validation_name} threw an unexpected exception: {e}"
                )
