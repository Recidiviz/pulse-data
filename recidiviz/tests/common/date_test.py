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

from recidiviz.common import date


def test_mungeDateString_munges():
    assert date.munge_date_string('1y 1m 1d') == '1year 1month 1day'


def test_mungeDateString_doesntMunge():
    assert date.munge_date_string('1year 1month 1day') == '1year 1month 1day'


def test_mungeDateString_caseInsensitive():
    assert date.munge_date_string('1Y 1M 1D') == '1year 1month 1day'


def test_mungeDateString_noYear():
    assert date.munge_date_string('10M 12D') == '10month 12day'


def test_mungeDateString_noMonth():
    assert date.munge_date_string('9Y 28D') == '9year 28day'


def test_mungeDateString_noDay():
    assert date.munge_date_string('4y 3m') == '4year 3month'


def test_mungeDateString_ZeroAm():
    assert date.munge_date_string('Jan 1, 2018 00:00 AM') == \
        'Jan 1, 2018 12:00 AM'
