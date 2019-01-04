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
from distutils.util import strtobool  # pylint: disable=no-name-in-module
import dateparser


from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.mappable_enum import EnumParsingError
from recidiviz.common.constants.person import Ethnicity, Race


def fn(func, field_name, proto, default=None):
    """Return the result of applying the given function to the field on the
    proto, returning |default| if the proto field is unset.
    """
    if not proto.HasField(field_name):
        return default
    return func(getattr(proto, field_name))


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


def race_is_actually_ethnicity(ingest_person):
    if ingest_person.HasField('ethnicity'):
        return False
    if not ingest_person.HasField('race'):
        return False

    try:
        Ethnicity.from_str(ingest_person.race)
        race_is_ethnicity = True
    except EnumParsingError:
        race_is_ethnicity = False

    try:
        Race.from_str(ingest_person.race)
        race_is_already_set_correctly = True
    except EnumParsingError:
        race_is_already_set_correctly = False

    return race_is_ethnicity and not race_is_already_set_correctly


def parse_date(date_string):
    """
    Parses a string into a datetime object.

    Args:
        date_string: The string to be parsed.

    Return:
        (datetime) Datetime representation of the provided string.
    """
    if date_string == '' or date_string.isspace():
        return None
    parsed_date = dateparser.parse(date_string)
    if not parsed_date:
        raise ValueError('cannot parse date: %s' % parsed_date)
    return parsed_date


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
        birth_year = datetime.datetime.now().date().year - int(age)
        return datetime.date(year=birth_year, month=1, day=1)
    except Exception:
        raise ValueError('cannot parse age: %s' % age)


def parse_days(time_string):
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
        return int(time_string)
    except ValueError:
        # dateparser.parse interprets the string '1 YEAR 2 DAYS' to mean the
        # datetime 1 year and 2 days ago.
        date_ago = dateparser.parse(time_string)
        if date_ago:
            return (datetime.datetime.now() - date_ago).days

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


def parse_bond_amount_and_infer_type(amount):
    if amount.upper().startswith('NO'):
        return None, BondType.NO_BOND

    if 'DENIED' in amount.upper():
        return None, BondType.BOND_DENIED

    parsed_amount = parse_dollars(amount)
    if parsed_amount == 0:
        return None, BondType.NO_BOND
    return parsed_amount, BondType.CASH


def parse_dollars(dollar_string):
    """
    Parses a string and returns an int dollar amount

    Args:
        dollar_string: str to convert into a dollar amount

    Return:
        (int) whole number of dollars converted from input
    """
    if dollar_string == '' or dollar_string.isspace():
        return 0
    try:
        clean_string = ''.join(
            dollar_string.replace('$', '').replace(',', '').split())
        return int(float(clean_string))
    except Exception:
        raise ValueError('cannot parse dollar value: %s' % dollar_string)


def parse_bool(bool_string):
    """Parse a string and returns a bool."""
    try:
        return bool(strtobool(bool_string))
    except Exception:
        raise ValueError('cannot parse bool value: %s' % bool_string)
