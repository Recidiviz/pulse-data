# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for attr_validator_checks.py."""
import unittest

from recidiviz.common import attr_validators
from recidiviz.common.attr_validator_checks import passes_validator


class TestPassesValidator(unittest.TestCase):
    """Tests for passes_validator."""

    def test_passes_validator(self) -> None:
        # A value the validator accepts returns True; one it rejects with a
        # ValueError returns False.
        self.assertTrue(passes_validator(attr_validators.is_non_empty_str, "foo"))
        self.assertFalse(passes_validator(attr_validators.is_non_empty_str, ""))
        # A validator that raises TypeError on the wrong type also returns False
        # rather than propagating.
        self.assertFalse(passes_validator(attr_validators.is_non_empty_str, 5))
