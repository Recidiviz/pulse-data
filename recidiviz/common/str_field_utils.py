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
"""
Common utils for processing str fields (parsing into primitive types,
normalizing, etc).
"""

import datetime
import locale
import re
import string
from distutils.util import strtobool  # pylint: disable=no-name-in-module
from typing import Optional, Dict, Any

import dateparser

from recidiviz.common.date import munge_date_string


def parse_dollars(dollar_string: str) -> int:
    """Parses a string and returns an int dollar amount"""
    clean_string = dollar_string.strip(' ').strip('$')
    if not clean_string:
        return 0
    try:
        return int(locale.atof(clean_string))
    except Exception:
        raise ValueError("Cannot parse dollar value: %s" % dollar_string)


def parse_bool(bool_string: str) -> bool:
    """Parses a string and returns a bool."""
    try:
        return bool(strtobool(bool_string))
    except Exception:
        raise ValueError("Cannot parse bool value: %s" % bool_string)


def parse_int(int_string: str) -> int:
    """Parses a string and returns an int."""
    try:
        return int(normalize(int_string))
    except Exception:
        raise ValueError("Cannot parse int value: %s" % int_string)


def parse_days(
        time_string: str,
        from_dt: Optional[datetime.datetime] = None
) -> int:
    """
    Converts the given string into an int number of days, using |from_dt|
    as a base for any relative dates.
    """
    if time_string == '' or time_string.isspace():
        return 0
    try:
        return int(time_string)
    except ValueError:
        # Parses the string '1 YEAR 2 DAYS' to mean 1 year and 2 days prior.
        current_dt = from_dt or datetime.datetime.now()
        past_dt = parse_datetime(time_string, from_dt=current_dt)
        if past_dt:
            return (current_dt - past_dt).days

    raise ValueError("Cannot parse time duration: %s" % time_string)


def parse_datetime(
        date_string: str, from_dt: Optional[datetime.datetime] = None
    ) -> Optional[datetime.datetime]:
    """
    Parses a string into a datetime.datetime object, using |from_dt| as a base
    for any relative dates.
    """
    if date_string == '' or date_string.isspace():
        return None
    if is_str_field_none(date_string):
        return None

    settings: Dict[str, Any] = {'PREFER_DAY_OF_MONTH': 'first'}
    if from_dt:
        settings['RELATIVE_BASE'] = from_dt

    date_string = munge_date_string(date_string)
    parsed = dateparser.parse(date_string, languages=['en'], settings=settings)
    if parsed:
        return parsed

    raise ValueError("cannot parse date: %s" % date_string)


def parse_date(
        date_string: str, from_dt: Optional[datetime.datetime] = None
    ) -> Optional[datetime.date]:
    """
    Parses a string into a datetime.date object, using |from_dt| as a base for
    any relative dates.
    """
    parsed = parse_datetime(date_string, from_dt=from_dt)
    return parsed.date() if parsed else None


def normalize(s: str, remove_punctuation: bool = False) -> str:
    """Normalizes whitespace within the provided string by converting all groups
    of whitespaces into ' ', and uppercases the string."""
    if remove_punctuation:
        translation = str.maketrans(dict.fromkeys(string.punctuation, ' '))
        label_without_punctuation = s.translate(translation)
        if not label_without_punctuation.isspace():
            s = label_without_punctuation

    if s is None or s == '' or s.isspace():
        raise ValueError("Cannot normalize None or empty/whitespace string")
    return ' '.join(s.split()).upper()


def normalize_truncated(message: str) -> str:
    """Truncates |message| to length used for string fields in schema"""
    return normalize(message)[:255]


def is_str_field_none(s: str) -> bool:
    """Returns True if the string value should be parsed as None."""
    return normalize(s, remove_punctuation=True) in {
        'N A',
        'NONE',
        'NONE SET',
        'NOT SPECIFIED'
    }


_FIRST_CAP_REGEX = re.compile('(.)([A-Z][a-z]+)')
_ALL_CAP_REGEX = re.compile('([a-z0-9])([A-Z])')


def to_snake_case(capital_case_name: str) -> str:
    """Converts a capital case string (i.e. 'SupervisionViolationResponse'
    to a snake case string (i.e. 'supervision_violation_response'). See
    https://stackoverflow.com/questions/1175208/
    elegant-python-function-to-convert-camelcase-to-snake-case.
    """
    s1 = _FIRST_CAP_REGEX.sub(r'\1_\2', capital_case_name)
    return _ALL_CAP_REGEX.sub(r'\1_\2', s1).lower()
