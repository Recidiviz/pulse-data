# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
"""Tests for conversion_utils.py"""

import datetime
from unittest import TestCase

import pytest
from mock import patch

from recidiviz.common.constants.mappable_enum import EnumParsingError
from recidiviz.common.constants.person import Gender
from recidiviz.persistence.converter import converter_utils

_NOW = datetime.datetime(2000, 1, 1)


class TestConverterUtils(TestCase):
    """Test conversion util methods."""

    def test_parseDateTime(self):
        assert converter_utils.parse_datetime('Jan 1, 2018 1:40') == \
               datetime.datetime(year=2018, month=1, day=1, hour=1, minute=40)

    def test_parseDate(self):
        assert converter_utils.parse_date('Jan 1, 2018') == \
               datetime.date(year=2018, month=1, day=1)

    def test_parseBadDate(self):
        with pytest.raises(ValueError):
            converter_utils.parse_datetime('ABC')

    @patch('recidiviz.persistence.converter.converter_utils.datetime.datetime')
    def test_parseAge(self, mock_datetime):
        mock_datetime.now.return_value = _NOW

        expected_birthdate = datetime.date(
            year=_NOW.date().year - 1000, month=1, day=1)
        assert converter_utils.calculate_birthdate_from_age('1000') == \
               expected_birthdate

    def test_parseBadAge(self):
        with pytest.raises(ValueError):
            converter_utils.calculate_birthdate_from_age('ABC')

    def test_parseTimeDuration(self):
        assert converter_utils.parse_days('10') == 10

    def test_parseBadTimeDuration(self):
        with pytest.raises(ValueError):
            converter_utils.parse_days('ABC')

    def test_parseDollarAmount(self):
        assert converter_utils.parse_dollars('$100.00') == 100
        assert converter_utils.parse_dollars('$') == 0

    def test_parseBadDollarAmount(self):
        with pytest.raises(ValueError):
            converter_utils.parse_dollars('ABC')

    def test_parseBool(self):
        assert converter_utils.parse_bool("True") is True

    def test_parseBadBoolField(self):
        with pytest.raises(ValueError):
            converter_utils.parse_bool('ABC')

    def test_parseEnum(self):
        assert Gender.parse('Male', {}) == Gender.MALE

    def test_parseBadEnum(self):
        with pytest.raises(EnumParsingError):
            Gender.parse('ABD', {})
