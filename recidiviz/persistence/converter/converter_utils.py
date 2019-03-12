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
import locale
from distutils.util import strtobool  # pylint: disable=no-name-in-module
from typing import Optional

from recidiviz.common import common_utils, date
from recidiviz.common.common_utils import normalize
from recidiviz.common.constants.bond import (BOND_STATUS_MAP, BOND_TYPE_MAP,
                                             BondStatus, BondType)
from recidiviz.common.constants.person import Ethnicity, Race

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

def fn(func, field_name, proto, *additional_func_args, default=None):
    """Return the result of applying the given function to the field on the
    proto, returning |default| if the proto field is unset or the function
    returns None.
    """
    value = None
    if proto.HasField(field_name):
        value = func(getattr(proto, field_name), *additional_func_args)
    return value if value is not None else default


def parse_external_id(id_str):
    """If the supplied |id_str| is generated, returns None. Otherwise
    returns the normalized version of the provided |id_str|"""
    if common_utils.is_generated_id(id_str):
        return None
    return normalize(id_str)


def race_is_actually_ethnicity(ingest_person, enum_overrides):
    if ingest_person.HasField('ethnicity'):
        return False
    if not ingest_person.HasField('race'):
        return False

    return Ethnicity.can_parse(ingest_person.race, enum_overrides) and \
        not Race.can_parse(ingest_person.race, enum_overrides)


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


def parse_days(time_string: str, from_dt: Optional[datetime.datetime] = None):
    """
    Converts the given string into an int number number of days

    Args:
        time_string: The string to convert into int.
        from_dt: Datetime to use as base for any relative dates.

    Return:
        (int) number of days converted from time_string
    """
    if time_string == '' or time_string.isspace():
        return 0
    try:
        return int(time_string)
    except ValueError:
        # Parses the string '1 YEAR 2 DAYS' to mean 1 year and 2 days prior.
        current_dt = from_dt or datetime.datetime.now()
        past_dt = date.parse_datetime(time_string, from_dt=current_dt)
        if past_dt:
            return (current_dt - past_dt).days

    raise ValueError('cannot parse time duration: %s' % time_string)


def parse_bond_amount_and_check_for_type_and_status_info(amount):
    """Parses bond (amount) string and returns |int| value. If (amount) string
    is not a numeric value, checks if the text value contains information about
    bond type or bond status.

    Returns:
        Tuple of bond amount, bond type, and bond status.
    """
    bond_type = BOND_TYPE_MAP.get(amount.upper(), None)
    bond_status = BOND_STATUS_MAP.get(
        amount.upper(), BondStatus.UNKNOWN_FOUND_IN_SOURCE)

    if bond_type is not None \
            or bond_status != BondStatus.UNKNOWN_FOUND_IN_SOURCE:
        return None, bond_type, bond_status

    parsed_amount = parse_dollars(amount)
    if int(parsed_amount) == 0:
        return None, BondType.NO_BOND, BondStatus.UNKNOWN_FOUND_IN_SOURCE
    return parsed_amount, BondType.CASH, BondStatus.INFERRED_SET


def parse_dollars(dollar_string):
    """
    Parses a string and returns an int dollar amount

    Args:
        dollar_string: str to convert into a dollar amount

    Return:
        (int) whole number of dollars converted from input
    """
    clean_string = dollar_string.strip(' ').strip('$')
    if not clean_string:
        return 0
    try:
        return int(locale.atof(clean_string))
    except Exception:
        raise ValueError('cannot parse dollar value: %s' % dollar_string)


def parse_bool(bool_string):
    """Parse a string and returns a bool."""
    try:
        return bool(strtobool(bool_string))
    except Exception:
        raise ValueError('cannot parse bool value: %s' % bool_string)


def parse_int(int_string):
    """Parse a string and returns an int."""
    try:
        return int(normalize(int_string))
    except Exception:
        raise ValueError('cannot parse int value: %s' % int_string)
