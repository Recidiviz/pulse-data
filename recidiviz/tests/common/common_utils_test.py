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

"""Tests for ingest/common_utils.py."""

from datetime import date, datetime
from recidiviz.common import common_utils


def test_parse_date_string_american_date():
    date_string = "11/20/1991 12:30"

    result = common_utils.parse_date_string(date_string)
    assert result is not None
    assert result == date(1991, 11, 20)


def test_parse_datetime_string_american_date():
    date_string = "11/20/1991 12:30"

    result = common_utils.parse_datetime_string(date_string)
    assert result is not None
    assert result == datetime(1991, 11, 20, 12, 30)


def test_parse_datetime_string_american_date_two_digit_year():
    date_string = "11/20/91"

    result = common_utils.parse_datetime_string(date_string)
    assert result is not None
    assert result == datetime(1991, 11, 20)


def test_parse_datetime_string_american_date_no_day():
    date_string = "11/1991"

    result = common_utils.parse_datetime_string(date_string)
    assert result is not None
    assert result == datetime(1991, 11, 1)


def test_parse_datetime_string_american_date_no_day_two_digit_year():
    date_string = "11/91"

    result = common_utils.parse_datetime_string(date_string)
    assert result is not None
    assert result == datetime(1991, 11, 1)


def test_parse_datetime_string_international_date():
    date_string = "2018-05-24"

    result = common_utils.parse_datetime_string(date_string)
    assert result is not None
    assert result == datetime(2018, 5, 24)


def test_parse_datetime_string_international_date_no_day():
    date_string = "2018-03"

    result = common_utils.parse_datetime_string(date_string)
    assert result is not None
    assert result == datetime(2018, 3, 1)


def test_parse_datetime_string_none():
    assert common_utils.parse_datetime_string(None) is None


def test_parse_datetime_string_garbage():
    assert common_utils.parse_datetime_string("whatever") is None


def test_parse_datetime_string_empty():
    assert common_utils.parse_datetime_string("") is None


def test_parse_datetime_string_space():
    assert common_utils.parse_datetime_string("    ") is None
