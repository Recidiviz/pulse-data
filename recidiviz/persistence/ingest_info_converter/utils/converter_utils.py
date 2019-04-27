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
"""Utils for converting individual data fields."""
import datetime
import locale
from typing import Optional, Tuple

from recidiviz.common import common_utils
from recidiviz.common.constants.bond import (BOND_STATUS_MAP, BOND_TYPE_MAP,
                                             BondStatus, BondType)
from recidiviz.common.str_field_utils import parse_dollars, normalize

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
        raise ValueError("Cannot parse age: %s" % age)


def parse_bond_amount_type_and_status(
        provided_amount: str, provided_bond_type: Optional[BondType] = None,
        provided_status: Optional[BondStatus] = None) -> \
        Tuple[Optional[int], Optional[BondType], BondStatus]:
    """Returns bond amount, bond type, and bond status, setting any missing
    values that can be inferred from the other fields.
    """
    # Amount field can sometimes contain type and status info instead of being
    # a numeric value
    type_from_amount = None
    status_from_amount = None
    if provided_amount:
        type_from_amount = BOND_TYPE_MAP.get(provided_amount.upper(), None)
        status_from_amount = BOND_STATUS_MAP.get(provided_amount.upper(), None)

    # If provided_amount was a non-numeric value but was not included in
    # BOND_TYPE_MAP or BOND_STATUS_MAP, the below call will throw (ValueError).
    # This is intentional, to ensure all values that should be converted are
    # properly captured.
    amount = None
    if provided_amount is not None \
            and type_from_amount is None and status_from_amount is None:
        amount = parse_dollars(provided_amount)

    bond_type = provided_bond_type or type_from_amount
    status = provided_status or status_from_amount

    # Infer missing fields from known fields
    if bond_type is None and amount is not None:
        bond_type = BondType.CASH

    if status is None and bond_type in (BondType.DENIED, BondType.NOT_REQUIRED):
        status = BondStatus.SET

    # Fall back on default status if no other status was set
    if status is None:
        status = BondStatus.PRESENT_WITHOUT_INFO

    return (amount, bond_type, status)  # type: ignore
