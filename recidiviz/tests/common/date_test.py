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
"""Tests for date.py"""
import datetime
import pytest

from recidiviz.common import date

def test_mungeDateString_munges():
    assert date.munge_date_string('1y 1m 1d') == '1year 1month 1day'

def test_mungeDateString_doesntMunge():
    assert date.munge_date_string('1year 1month 1day') == '1year 1month 1day'

def test_parseDateTime():
    assert date.parse_datetime('Jan 1, 2018 1:40') == \
            datetime.datetime(year=2018, month=1, day=1, hour=1, minute=40)


def test_parseDateTime_yearMonth():
    assert date.parse_datetime(
        '2018-04', from_dt=datetime.datetime(2019, 3, 15)) == \
            datetime.datetime(year=2018, month=4, day=1)


def test_parseDateTime_relative():
    assert date.parse_datetime(
        '1y 1m 1d', from_dt=datetime.datetime(2000, 1, 1)) == \
        datetime.datetime(year=1998, month=11, day=30)


def test_parseDate():
    assert date.parse_date('Jan 1, 2018') == \
            datetime.date(year=2018, month=1, day=1)


def test_parseNoDate():
    assert date.parse_date('None set') is None


def test_parseBadDate():
    with pytest.raises(ValueError):
        date.parse_datetime('ABC')
