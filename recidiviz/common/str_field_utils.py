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
from dateutil.relativedelta import relativedelta

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


# TODO(2365): All usages of this function should pass in a datetime, otherwise
#  the value returned for strings like '9M 10D' depend on the date the code is
#  being executed, leading to flaky tests.
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


def parse_days_from_duration_pieces(
        years_str: Optional[str] = None,
        months_str: Optional[str] = None,
        days_str: Optional[str] = None,
        start_dt_str: Optional[str] = None) -> int:
    """Returns a number of days specified by the given duration strings. If a
    start date is specified, will calculate the number of days after that date,
    otherwise, assumes that a month is 30 days long and a year is 365.25 days
    long. All calculations are rounded to the nearest full day.

    Args:
        years_str: A string representation of an int number of years
        months_str: A string representation of an int number of months
        days_str: A string representation of an int number of days
        start_dt_str: A string representation of a date to start counting from.

    Returns:
        A number of days specified by the duration strings.

    Raises:
        ValueError: If one of the provided strings cannot be properly parsed.
    """

    if not any((years_str, months_str, days_str)):
        raise ValueError(
            'One of (years_str, months_str, days_str) must be nonnull')

    years = parse_int(years_str) if years_str else 0
    months = parse_int(months_str) if months_str else 0
    days = parse_int(days_str) if days_str else 0

    if start_dt_str and not is_str_field_none(start_dt_str):
        start_dt = parse_datetime(start_dt_str)
        if start_dt:
            end_dt = start_dt + relativedelta(years=years,
                                              months=months,
                                              days=days)
            if end_dt:
                return (end_dt - start_dt).days

    return int(years * 365.25) + (months * 30) + days


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

    # Only special-case strings that start with a - (to avoid parsing regular
    # timestamps like '2016-05-14') and that include non punctuation (to avoid
    # ingested values like '--')
    if date_string.startswith('-') and _has_non_punctuation(date_string):
        parsed = parse_datetime_with_negative_component(date_string, settings)
    else:
        parsed = dateparser.parse(
            date_string, languages=['en'], settings=settings)
    if parsed:
        return parsed

    raise ValueError("cannot parse date: %s" % date_string)


def _has_non_punctuation(date_string: str) -> bool:
    return any(ch not in string.punctuation for ch in date_string)


def parse_datetime_with_negative_component(date_string: str,
                                           settings: Dict[str, Any]):
    """Handles relative date strings that have negative values in them, like
    '2 year -5month'.

    Instead of parsing the entire relative string all at once as in
    parse_datetime, we parse each individual component one by one, converting
    negative components to look ahead in time instead of backwards.

    '5month' is parsed by dateparser as '5 months ago'. dateparser does not
    automatically convert '-5month' into '-5 months ago' or 'in 5 months', so we
    manually convert '-5month' into 'in 5month' to achieve the desired effect.
    """
    parsed_date = None
    latest_relative = settings.get('RELATIVE_BASE', datetime.datetime.now())

    components = date_string.split(' ')
    for component in components:
        settings['RELATIVE_BASE'] = latest_relative

        updated_component = component
        if updated_component.startswith('-'):
            updated_component = updated_component.replace('-', 'in ')
        parsed_date = dateparser.parse(
            updated_component, languages=['en'], settings=settings)

        latest_relative = parsed_date

    return parsed_date


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
