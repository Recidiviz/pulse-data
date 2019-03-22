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
# ============================================================================
"""Utils for parsing dates."""
import datetime
import re
from typing import Any, Dict, Optional

import dateparser

from recidiviz.persistence.converter import converter_utils


def munge_date_string(date_string: str) -> str:
    """Tranforms the input date string so it can be parsed, if necessary"""
    return re.sub(
        r'^(?P<year>\d+)y\s*(?P<month>\d+)m\s*(?P<day>\d+)d$',
        lambda match: '{year}year {month}month {day}day'.format(
            year=match.group('year'), month=match.group('month'),
            day=match.group('day')),
        date_string)


def parse_datetime(
        date_string: str, from_dt: Optional[datetime.datetime] = None
    ) -> Optional[datetime.datetime]:
    """
    Parses a string into a datetime object.

    Args:
        date_string: The string to be parsed.
        from_dt: Datetime to use as base for any relative dates.

    Return:
        (datetime) Datetime representation of the provided string.
    """
    if date_string == '' or date_string.isspace():
        return None
    if converter_utils.is_none(date_string):
        return None

    settings: Dict[str, Any] = {'PREFER_DAY_OF_MONTH': 'first'}
    if from_dt:
        settings['RELATIVE_BASE'] = from_dt

    date_string = munge_date_string(date_string)
    parsed = dateparser.parse(date_string, languages=['en'], settings=settings)
    if parsed:
        return parsed

    raise ValueError('cannot parse date: %s' % date_string)


def parse_date(
        date_string: str, from_dt: Optional[datetime.datetime] = None
    ) -> Optional[datetime.date]:
    """
    Parses a string into a datetime object.

    Args:
        date_string: The string to be parsed.
        from_dt: Datetime to use as base for any relative dates.

    Return:
        (date) Date representation of the provided string.
    """
    parsed = parse_datetime(date_string, from_dt=from_dt)
    return parsed.date() if parsed else None
