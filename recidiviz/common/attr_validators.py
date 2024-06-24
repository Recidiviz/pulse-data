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
import re
from typing import Any, Callable, Optional, Type

import attr
import pytz
from more_itertools import one


class IsOptionalValidator:
    def __init__(self, expected_cls_type: Type) -> None:
        self._expected_cls_type = expected_cls_type

    def __call__(self, instance: Any, attribute: attr.Attribute, value: Any) -> None:
        return attr.validators.optional(
            attr.validators.instance_of(self._expected_cls_type)
        )(instance, attribute, value)


def is_opt(cls_type: Type) -> Callable:
    """Returns an attrs validator that checks if the value is an instance of |cls_type|
    or None."""
    return IsOptionalValidator(cls_type)


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


def is_opt_valid_email(_instance: Any, _attribute: attr.Attribute, value: str) -> None:
    if value is not None:
        is_valid_email(_instance, _attribute, value)


def is_valid_email(_instance: Any, _attribute: attr.Attribute, value: str) -> None:
    """
    Checks if the given value is a valid email based on certain conditions
    Raises an error if an email fails to meet a requirement otherwise returns None
    """
    invalid_characters = re.compile(r"[(),:;<>[\]\\]")
    suspicious_usernames = ["x", "unknown", "none", "noname", "nobody"]
    whitespace_pattern = re.compile(r"\s")
    if "@" not in value:
        raise ValueError(
            f"Incorrect format:Email field with {value} missing '@' symbol"
        )
    if value.count("@") != 1:
        raise ValueError(
            f"Incorrect format:Email field with {value} has more than one '@' symbol"
        )
    if re.search(whitespace_pattern, value):
        raise ValueError(
            f"Incorrect format:Email field with {value} contains whitespace"
        )

    local_part, _ = value.split("@")  # only does this when value.count('@') is 1
    if not local_part:
        raise ValueError(
            f"Incorrect format:Email field with {value} has no text before '@' symbol"
        )
    matches = re.findall(invalid_characters, value)

    if matches:  # If email contains invalid character
        invalid_chars = ",".join(matches)
        raise ValueError(
            f"Incorrect format: Email field with {value} contains invalid character {invalid_chars}"
        )

    if local_part.lower() in suspicious_usernames:
        raise ValueError(f"Email has a suspicious username {local_part}")


class AppearsWithValidator:
    """
    Validator to ensure that a specified field appears together with the current field.

    This validator enforces that both fields are either set (non-None) or unset (None) together,
    and ensures that the specified related field also includes an `AppearsWithValidator`
    referencing the current field.
    """

    def __init__(self, field_name: str):
        self.field_name = field_name

    def __call__(
        self, instance: Any, attribute: attr.Attribute, value: Optional[Any]
    ) -> None:
        if not hasattr(instance, self.field_name):
            raise ValueError(
                f"{self.field_name} is currently not an attribute of {type(instance)}. "
                f"Fields '{self.field_name}' and '{attribute.name}' should both be attributes of {type(instance)}"
            )

        other_field = one(
            f for f in attr.fields(instance.__class__) if f.name == self.field_name
        )

        if repr(AppearsWithValidator(attribute.name)) not in str(other_field.validator):
            raise ValueError(
                f"Field {self.field_name} does not have 'appears_with' validator"
            )

        other_value = getattr(instance, self.field_name)
        if (value is None) != (other_value is None):
            raise ValueError(
                f"Fields of {type(instance)}: '{attribute.name}' and '{self.field_name}' must both be set or both be None. "
                f"Current values: {attribute.name}={value}, {self.field_name}={other_value}"
            )

    def __repr__(self) -> str:
        return f"AppearsWithValidator field_name:{self.field_name}"


def appears_with(field_name: str) -> AppearsWithValidator:
    return AppearsWithValidator(field_name)


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


class IsListOfValidator:
    def __init__(self, list_item_expected_type: Type) -> None:
        self._list_item_expected_type = list_item_expected_type

    def __call__(self, instance: Any, attribute: attr.Attribute, value: Any) -> None:
        if not isinstance(value, list):
            raise ValueError(
                f"Found value for list type field [{attribute.name}] on class "
                f"[{type(instance)}] which has non-list type [{type(value)}]."
            )
        for item in value:
            if not isinstance(item, self._list_item_expected_type):
                raise ValueError(
                    f"Found item in list type field [{attribute.name}] on class "
                    f"[{type(instance)}] which is not the expected type "
                    f"[{self._list_item_expected_type}]: {type(item)}"
                )


def is_list_of(list_item_expected_type: Type) -> IsListOfValidator:
    return IsListOfValidator(list_item_expected_type)


# Dict field validators
is_dict = attr.validators.instance_of(dict)
is_opt_dict = is_opt(dict)
