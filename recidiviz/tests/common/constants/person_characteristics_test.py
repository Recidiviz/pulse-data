# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for recidiviz/common/constants/person_characteristics.py"""

import datetime
from unittest import TestCase

from recidiviz.common.constants.entity_enum import EnumParsingError
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.person_characteristics import Gender

_NOW = datetime.datetime(2000, 1, 1)


class TestCommonConstantsPersonCharacteristics(TestCase):
    """Test person constants."""

    def test_parseGenderEnum(self) -> None:
        assert Gender.parse("Male", EnumOverrides.empty()) == Gender.MALE

    def test_parseBadGenderEnum(self) -> None:
        with self.assertRaises(EnumParsingError):
            Gender.parse("ABD", EnumOverrides.empty())
