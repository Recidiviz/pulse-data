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
"""Tests for str_field_utils.py"""
import datetime
from unittest import TestCase

import pytest

from recidiviz.common.str_field_utils import parse_days, parse_dollars, \
    parse_bool, parse_date, parse_datetime


class TestStrFieldUtils(TestCase):
    """Test conversion util methods."""

    def test_parseTimeDurationDays(self):
        assert parse_days('10') == 10

    def test_parseTimeDurationFormat(self):
        source_date = datetime.datetime(2001, 1, 1)
        # 2000 was a leap year
        assert parse_days(
            '1 YEAR 2 DAYS', from_dt=source_date) == 368

    def test_parseBadTimeDuration(self):
        with pytest.raises(ValueError):
            parse_days('ABC')

    def test_parseDollarAmount(self):
        assert parse_dollars('$100.00') == 100
        assert parse_dollars('$') == 0

    def test_parseBadDollarAmount(self):
        with pytest.raises(ValueError):
            parse_dollars('ABC')

    def test_parseBool(self):
        assert parse_bool("True") is True

    def test_parseBadBoolField(self):
        with pytest.raises(ValueError):
            parse_bool('ABC')

    def test_parseDateTime(self):
        assert parse_datetime('Jan 1, 2018 1:40') == \
               datetime.datetime(year=2018, month=1, day=1, hour=1, minute=40)

    def test_parseDateTime_yearMonth(self):
        assert parse_datetime(
            '2018-04', from_dt=datetime.datetime(2019, 3, 15)) == \
               datetime.datetime(year=2018, month=4, day=1)

    def test_parseDateTime_relative(self):
        assert parse_datetime(
            '1y 1m 1d', from_dt=datetime.datetime(2000, 1, 1)) == \
               datetime.datetime(year=1998, month=11, day=30)

    def test_parseDate(self):
        assert parse_date('Jan 1, 2018') == \
               datetime.date(year=2018, month=1, day=1)

    def test_parseNoDate(self):
        assert parse_date('None set') is None

    def test_parseBadDate(self):
        with pytest.raises(ValueError):
            parse_datetime('ABC')
