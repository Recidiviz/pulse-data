# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Contains helper aliases and functions for various attrs validators that can be passed to the `validator=` arg of
any attr field. For example:

@attr.s
class MyClass:
  name: Optional[str] = attr.ib(validator=is_opt(str))
  is_valid: bool = attr.ib(validator=is_bool)
"""

import datetime
import logging
from typing import Any, Callable, Optional, Set, Type

import attr
import pytz

from recidiviz.common.constants.states import StateCode
from recidiviz.utils.types import T, assert_type


def state_exempted_validation(
    validator: Callable[[Any, attr.Attribute, T], None],
    *,
    exempted_states: Set[StateCode],
) -> Callable[[Any, attr.Attribute, T], None]:
    """A wrapper around any attr field validator that can be used to exempt certain
    states from the validation. In order to use this validator, the class with the field
    you're validating must also have a hydrated state_code field.
    """

    def _wrapper(instance: Any, attribute: attr.Attribute, value: T) -> None:
        if not hasattr(instance, "state_code"):
            raise ValueError(f"Class [{type(instance)}] does not have state_code")

        state_code = StateCode(assert_type(getattr(instance, "state_code"), str))

        try:
            validator(instance, attribute, value)
        except Exception as e:
            if state_code not in exempted_states:
                raise e

            logging.warning(
                "Found exempted validation error for state [%s]: %s", state_code, e
            )

    return _wrapper


def is_opt(cls_type: Type) -> Callable:
    """Returns an attrs validator that checks if the value is an instance of |cls_type| or None."""
    return attr.validators.optional(attr.validators.instance_of(cls_type))


def is_non_empty_str(_instance: Any, _attribute: attr.Attribute, value: str) -> None:
    if not isinstance(value, str):
        raise ValueError(f"Expected value type str, found {type(value)}.")
    if not value:
        raise ValueError("String value should not be empty.")


def is_utc_timezone_aware_datetime(
    _instance: Any, _attribute: attr.Attribute, value: Optional[datetime.datetime]
) -> None:
    if value:
        if value.tzinfo is None:
            raise ValueError("Expected timezone value to not be empty")
        if value.tzinfo not in (pytz.UTC, datetime.timezone.utc):
            raise ValueError(
                f"Expected timezone value to be UTC, found: {value.tzinfo}"
            )


def is_not_future_date(
    _instance: Any, _attribute: attr.Attribute, value: datetime.date
) -> None:
    today = datetime.date.today()
    if value > today:
        raise ValueError(
            f"Datetime with {value} has a date in the future. Today is {today}"
        )


def is_opt_not_future_date(
    _instance: Any, _attribute: attr.Attribute, value: Optional[datetime.date]
) -> None:
    if value is not None:
        is_not_future_date(_instance, _attribute, value)


def is_not_future_datetime(
    _instance: Any, _attribute: attr.Attribute, value: datetime.datetime
) -> None:
    """Checks that the given value is a datetime that is not in the future.

    The check matches the value's timezone if it exists. Otherwise we check
    against a non-timezone aware UTC now.
    """
    if value.tzinfo:
        now = datetime.datetime.now(tz=value.tzinfo)
    else:
        now = datetime.datetime.utcnow()
    if value > now:
        raise ValueError(
            f"Datetime field with value {value} is in the future. It is now {now}"
        )


def is_opt_not_future_datetime(
    _instance: Any, _attribute: attr.Attribute, value: Optional[datetime.datetime]
) -> None:
    if value is not None:
        is_not_future_datetime(_instance, _attribute, value)


# String field validators
is_str = attr.validators.instance_of(str)
is_opt_str = is_opt(str)

# Int field validators
is_int = attr.validators.instance_of(int)
is_opt_int = is_opt(int)

# Date field validators
is_date = attr.validators.instance_of(datetime.date)
is_opt_date = is_opt(datetime.date)

# Datetime field validators
is_datetime = attr.validators.instance_of(datetime.datetime)
is_opt_datetime = is_opt(datetime.datetime)

# Boolean field validators
is_bool = attr.validators.instance_of(bool)
is_opt_bool = is_opt(bool)

# List field validators
is_list = attr.validators.instance_of(list)
is_opt_list = is_opt(list)

# Dict field validators
is_dict = attr.validators.instance_of(dict)
is_opt_dict = is_opt(dict)
