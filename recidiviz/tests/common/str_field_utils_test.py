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
    parse_bool, parse_date, parse_datetime, parse_days_from_duration_pieces, parse_int, parse_date_from_date_pieces, \
    safe_parse_date_from_date_pieces


class TestStrFieldUtils(TestCase):
    """Test conversion util methods."""

    def test_parseInt(self):
        assert parse_int('123') == 123

    def test_parseInt_floatProvided(self):
        assert parse_int('123.6') == 123

    def test_parseInt_invalidStr(self):
        with pytest.raises(ValueError):
            parse_int('hello')

    def test_parseTimeDurationDays(self):
        assert parse_days('10') == 10

    def test_parseTimeDurationFormat(self):
        source_date = datetime.datetime(2001, 1, 1)
        # 2000 was a leap year
        assert parse_days(
            '1 YEAR 2 DAYS', from_dt=source_date) == 368

    def test_parseDaysFromDurationPieces(self):
        two_years_in_days = parse_days_from_duration_pieces(years_str='2')
        assert two_years_in_days == (365 * 2)

        four_years_in_days = parse_days_from_duration_pieces(years_str='4')
        assert four_years_in_days == (365 * 4 + 1)  # Include leap day

        leap_year_source_date = datetime.datetime(2000, 1, 1)

        two_months = parse_days_from_duration_pieces(
            months_str='2', start_dt_str=str(leap_year_source_date))

        assert two_months == (31 + 29)

        leap_year_in_days = parse_days_from_duration_pieces(
            years_str='1', start_dt_str=str(leap_year_source_date))
        assert leap_year_in_days == 366

        combo_duration = parse_days_from_duration_pieces(years_str='1',
                                                         months_str='2',
                                                         days_str='3')

        assert combo_duration == (1*365 + 2*30 + 3)

        combo_duration_with_start_date = \
            parse_days_from_duration_pieces(
                years_str='0',
                months_str='2',
                days_str='3',
                start_dt_str=str(leap_year_source_date))

        assert combo_duration_with_start_date == (2 * 30 + 3)

    def test_parseDaysFromDurationPieces_negativePieces(self):
        duration = parse_days_from_duration_pieces(years_str='2',
                                                   months_str='-5')

        assert duration == (2*365 + -5*30)

        duration = parse_days_from_duration_pieces(years_str='2',
                                                   months_str='-5',
                                                   days_str='-15')

        assert duration == (2*365 + -5*30 - 15)

    def test_parseDateFromDatePieces(self):
        parsed = parse_date_from_date_pieces('2005', '10', '12')
        assert parsed == datetime.date(year=2005, month=10, day=12)

    def test_parseDateFromDatePieces_noneValues(self):
        parsed = parse_date_from_date_pieces(None, None, None)
        assert parsed is None

    def test_parseDateFromDatePieces_invalidValues(self):
        with pytest.raises(ValueError):
            parse_date_from_date_pieces('abc', 'def', 'LIFE')

    def test_safeParseDateFromDatePieces_invalidValues(self):
        parsed = safe_parse_date_from_date_pieces('abc', 'def', 'LIFE')
        assert parsed is None

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

    def test_parseDateTime_yyyymmdd_format(self):
        assert parse_datetime(
            '19990629', from_dt=None) == \
               datetime.datetime(year=1999, month=6, day=29)

    def test_parseDateTime_relative(self):
        assert parse_datetime(
            '1y 1m 1d', from_dt=datetime.datetime(2000, 1, 1)) == \
               datetime.datetime(year=1998, month=11, day=30)

    def test_parseDateTime_zero(self):
        assert parse_datetime('0') is None

    def test_parseDateTime_zeroes(self):
        assert parse_datetime('00000000') is None

    def test_parseDateTime_zeroes_weird(self):
        assert parse_datetime('0 0 0') is None
        assert parse_datetime('0000-00-00') is None

    def test_parseDate(self):
        assert parse_date('Jan 1, 2018') == \
               datetime.date(year=2018, month=1, day=1)

    def test_parseDate_zero(self):
        assert parse_date('0') is None

    def test_parseDate_zeroes(self):
        assert parse_date('00000000') is None

    def test_parseDate_zeroes_weird(self):
        assert parse_date('0 0 0') is None
        assert parse_date('0000-00-00') is None

    def test_parseNoDate(self):
        assert parse_date('None set') is None

    def test_parseBadDate(self):
        with pytest.raises(ValueError):
            parse_datetime('ABC')
