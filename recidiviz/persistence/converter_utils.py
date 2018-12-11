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
# ============================================================================
"""Utils for converting individual data fields."""

import datetime

from recidiviz.common import global_map
from recidiviz.ingest import scraper_utils


def normalize(s):
    """
    Normalizes whitespace within the provided string by converting all groups
    of whitespaces into ' '.

    Args:
        s: The string to be normalized

    Return:
        (str): Normalized string
    """
    if s is None:
        raise ValueError(
            'function normalize should never be called with None')
    if s == '' or s.isspace():
        return ''
    return ' '.join(s.split()).upper()


def enum_contains_value(enum_type, value):
    return global_map.enum_contains_value(enum_type, value)


def string_to_enum(enum_type, value):
    """
    Converts the given value into an enum of the provided enum_type

    Args:
        enum_type: The enum type to convert to.
        value: The string representation of an enum of the provided enum_type

    Return:
        (enum) Converted enum of type enum_type.
    """
    return global_map.convert_value_to_enum(
        enum_type, normalize(value))


def race_is_actually_ethnicity(ingest_person):
    return enum_contains_value('ethnicity', ingest_person.race) and \
        not enum_contains_value('race', ingest_person.race)


def parse_date_or_error(date_string):
    """
    Parses a string into a datetime object.

    Args:
        date_string: The string to be parsed.

    Return:
        (datetime) Datetime representation of the provided string.
    """
    if date_string == '' or date_string.isspace():
        return None
    parsed_date = scraper_utils.parse_date_string(date_string)
    if not parsed_date:
        raise ValueError('cannot parse date: %s' % parsed_date)
    return datetime.datetime.combine(parsed_date, datetime.time())


def calculate_birthdate_from_age(age):
    """
    Creates a birthdate from the given year. We estimate a person's birthdate by
    subtracting their age from the current year and setting their birthdate
    to the first day of that year.

    Args:
        age: Int representation of an age.

    Return:
        (datetime) January 1st of the calculated birth year.
    """
    if age == '' or age.isspace():
        return None
    try:
        birth_year = datetime.date.today().year - int(age)
        return datetime.date(year=birth_year, month=1, day=1)
    except Exception:
        raise ValueError('cannot parse age: %s' % age)


def time_string_to_days(time_string):
    """
    Converts the given string into an int number number of days

    Args:
        time_string: The string to convert into int.

    Return:
        (int) number of days converted from time_string
    """
    if time_string == '' or time_string.isspace():
        return 0
    try:
        # TODO: use dateparser in Python3 (#176)
        return int(time_string)
    except Exception:
        raise ValueError('cannot parse time duration: %s' % time_string)


def split_full_name(full_name):
    """Splits a full name into given and surnames.

    Args:
        full_name: (str)
    Returns:
        a pair of strings (surname, given_names)
    """
    if full_name == '' or full_name.isspace():
        return None
    full_name = normalize(full_name)
    if ',' in full_name:
        names = full_name.split(',')
        if len(names) == 2 and all(names):
            return tuple(names)
    names = full_name.split()
    if len(names) >= 2:
        return names[-1], ' '.join(names[:-1])
    raise ValueError('cannot parse full name: %s' % full_name)


def parse_dollar_amount(dollar_string):
    """
    Parses a string and returns an int dollar amount

    Args:
        dollar_string: str to convert into a dollar amount

    Return:
        (int) whole number of dollars converted from input
    """
    if dollar_string == '' or dollar_string.isspace() \
            or 'NO' in dollar_string.upper():
        return 0
    try:
        clean_string = ''.join(
            dollar_string.replace('$', '').replace(',', '').split())
        return int(float(clean_string))
    except Exception:
        raise ValueError('cannot parse dollar value: %s' % dollar_string)


def verify_is_bool(possible_bool):
    """
    Verifies that the input is a boolean

    Args:
        possible_bool: argument to evaluate

    returns:
        True if argument is a bool, otherwise False
    """
    if isinstance(possible_bool, bool):
        return possible_bool
    raise ValueError('expected bool but got %s: %s' %
                     (type(possible_bool), possible_bool))
