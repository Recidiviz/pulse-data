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

from recidiviz.common.constants.person import Gender
from recidiviz.persistence import converter_utils


class TestConverterUtils(TestCase):
    """Test conversion util methods."""

    def test_parseDate(self):
        assert converter_utils.parse_date_or_error('Jan 1, 2018') == \
               datetime.datetime(year=2018, month=1, day=1)

    def test_parseBadDate(self):
        with pytest.raises(ValueError):
            converter_utils.parse_date_or_error('ABC')

    def test_parseAge(self):
        expected_birthdate = datetime.date(
            year=datetime.date.today().year - 1000, month=1, day=1)
        assert converter_utils.calculate_birthdate_from_age('1000') == \
               expected_birthdate

    def test_parseBadAge(self):
        with pytest.raises(ValueError):
            converter_utils.calculate_birthdate_from_age('ABC')

    def test_parseTimeDuration(self):
        assert converter_utils.time_string_to_days('10') == 10

    def test_parseBadTimeDuration(self):
        with pytest.raises(ValueError):
            converter_utils.time_string_to_days('ABC')

    def test_parseName(self):
        assert converter_utils.split_full_name("LAST,FIRST") == \
               ('LAST', 'FIRST')

    def test_parseBadName(self):
        with pytest.raises(ValueError):
            converter_utils.split_full_name('ABC')

    def test_parseDollarAmount(self):
        assert converter_utils.parse_dollar_amount('$100.00') == 100

    def test_parseBadDollarAmount(self):
        with pytest.raises(ValueError):
            converter_utils.parse_dollar_amount('ABC')

    def test_parseBool(self):
        assert converter_utils.verify_is_bool(True) is True

    def test_parseBadBoolField(self):
        with pytest.raises(ValueError):
            converter_utils.verify_is_bool('ABC')

    def test_parseEnum(self):
        assert converter_utils.string_to_enum('gender', 'Male') == Gender.MALE

    def test_parseBadEnum(self):
        with pytest.raises(ValueError):
            converter_utils.string_to_enum('gender', 'ABC')
