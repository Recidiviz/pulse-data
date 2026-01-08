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


class DateBelowLowerBoundError(ValueError):
    """Raised when a date value is below the minimum allowed date."""


class DateAboveUpperBoundError(ValueError):
    """Raised when a date value is above the maximum allowed date."""


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


def is_opt_utc_timezone_aware_datetime(
    _instance: Any, _attribute: attr.Attribute, value: Optional[datetime.datetime]
) -> None:
    if value is not None:
        is_utc_timezone_aware_datetime(_instance, _attribute, value)


def is_utc_timezone_aware_datetime(
    _instance: Any, _attribute: attr.Attribute, value: datetime.datetime
) -> None:
    if not isinstance(value, datetime.datetime):
        raise ValueError(f"Expected datetime, found {type(value)}")
    if value.tzinfo is None:
        raise ValueError("Expected timezone value to not be empty")
    if value.tzinfo not in (pytz.UTC, datetime.timezone.utc):
        raise ValueError(f"Expected timezone value to be UTC, found: {value.tzinfo}")


# TODO(#38836): Deprecate in favor of is_reasonable_past_date
def is_not_future_date(
    _instance: Any, _attribute: attr.Attribute, value: datetime.date
) -> None:
    today = datetime.date.today()
    if value > today:
        raise ValueError(
            f"Datetime with {value} has a date in the future. Today is {today}"
        )


# TODO(#38836): Deprecate in favor of is_opt_reasonable_past_date
def is_opt_not_future_date(
    _instance: Any, _attribute: attr.Attribute, value: Optional[datetime.date]
) -> None:
    if value is not None:
        is_not_future_date(_instance, _attribute, value)


# TODO(#38836): Deprecate in favor of is_reasonable_past_datetime
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
        now = datetime.datetime.now(tz=pytz.UTC).replace(tzinfo=None)

    if value > now:
        raise ValueError(
            f"Datetime field with value {value} is in the future. It is now {now}"
        )


# TODO(#38836): Deprecate in favor of is_opt_reasonable_past_datetime
def is_opt_not_future_datetime(
    _instance: Any, _attribute: attr.Attribute, value: Optional[datetime.datetime]
) -> None:
    if value is not None:
        is_not_future_datetime(_instance, _attribute, value)


def is_reasonable_date(
    min_allowed_date_inclusive: datetime.date, max_allowed_date_exclusive: datetime.date
) -> Callable:
    """Validator for a non-optional date field that enforces that the date is within a
    reasonable set of bounds.

    Args:
        min_allowed_date_inclusive: The minimum allowed date for this field, inclusive.
        max_allowed_date_exclusive: The maximum allowed date for this field, exclusive.
    """
    return IsReasonableDateValidator(
        min_allowed_date_inclusive=min_allowed_date_inclusive,
        max_allowed_date_exclusive=max_allowed_date_exclusive,
        allow_nulls=False,
    )


def is_opt_reasonable_date(
    min_allowed_date_inclusive: datetime.date, max_allowed_date_exclusive: datetime.date
) -> Callable:
    """Validator for an optional date field that enforces that the date is within a
    reasonable set of bounds, if it is nonnull.

    Args:
        min_allowed_date_inclusive: The minimum allowed date for this field, inclusive.
        max_allowed_date_exclusive: The maximum allowed date for this field, exclusive.
    """
    return IsReasonableDateValidator(
        min_allowed_date_inclusive=min_allowed_date_inclusive,
        max_allowed_date_exclusive=max_allowed_date_exclusive,
        allow_nulls=True,
    )


def is_opt_reasonable_datetime(
    min_allowed_datetime_inclusive: datetime.datetime,
    max_allowed_datetime_exclusive: datetime.datetime,
) -> Callable:
    """Validator for an optional datetime field that enforces that the datetime is within a
    reasonable set of bounds, if it is nonnull.

    Args:
        min_allowed_date_inclusive: The minimum allowed date for this field, inclusive.
        max_allowed_date_exclusive: The maximum allowed date for this field, exclusive.
    """
    return IsReasonableDatetimeValidator(
        min_allowed_datetime_inclusive=min_allowed_datetime_inclusive,
        max_allowed_datetime_exclusive=max_allowed_datetime_exclusive,
        allow_nulls=True,
    )


# TODO(#38869): Extend to allow dates to be some fixed window in the future, perhaps
#  naming this is_reasonable_past_or_soon_date?
def is_reasonable_past_date(min_allowed_date_inclusive: datetime.date) -> Callable:
    """Validator for a non-optional date field that enforces that the date falls after
    a reasonable lower bound and on/before the current date (in UTC time).

    Args:
        min_allowed_date_inclusive: The minimum allowed date for this field, inclusive.
    """
    return IsReasonablePastDateValidator(
        min_allowed_date_inclusive=min_allowed_date_inclusive,
        allow_nulls=False,
    )


# TODO(#38869): Extend to allow dates to be some fixed window in the future, perhaps
#  naming this is_opt_reasonable_past_or_soon_date?
def is_opt_reasonable_past_date(min_allowed_date_inclusive: datetime.date) -> Callable:
    """Validator for a date field that enforces that the date falls after a reasonable
    lower bound and on/before the current date (in UTC time), if it is nonnull.

    Args:
        min_allowed_date_inclusive: The minimum allowed date for this field, inclusive.
    """
    return IsReasonablePastDateValidator(
        min_allowed_date_inclusive=min_allowed_date_inclusive,
        allow_nulls=True,
    )


# TODO(#38869): Extend to allow dates to be some fixed window in the future, perhaps
#  naming this is_reasonable_past_or_soon_datetime?
def is_reasonable_past_datetime(
    min_allowed_datetime_inclusive: datetime.datetime,
) -> Callable:
    """Validator for a non-optional datetime field that enforces that the date falls
    after a reasonable lower bound and on/before the current datetime (in UTC time if a
    datetime with a timezone is not provided, otherwise in the provided timezone).

    Args:
        min_allowed_datetime_inclusive: The minimum allowed date for this field,
            inclusive.
    """
    return IsReasonablePastDatetimeValidator(
        min_allowed_datetime_inclusive=min_allowed_datetime_inclusive,
        allow_nulls=False,
    )


# TODO(#38869): Extend to allow dates to be some fixed window in the future, perhaps
#  naming this is_opt_reasonable_past_or_soon_datetime?
def is_opt_reasonable_past_datetime(
    min_allowed_datetime_inclusive: datetime.datetime,
) -> Callable:
    """Validator for a datetime field that enforces that the date falls after a
    reasonable lower bound and on/before the current datetime (in UTC time if a
    datetime with a timezone is not provided, otherwise in the provided timezone), if
    it is nonnull.

    Args:
        min_allowed_datetime_inclusive: The minimum allowed date for this field,
            inclusive.
    """
    return IsReasonablePastDatetimeValidator(
        min_allowed_datetime_inclusive=min_allowed_datetime_inclusive,
        allow_nulls=True,
    )


# TODO(#55651): Delete once all usages have been migrated to is_opt_valid_email
def is_opt_valid_email_legacy(
    _instance: Any, _attribute: attr.Attribute, value: str
) -> None:
    if value is not None:
        is_valid_email_legacy(_instance, _attribute, value)


# TODO(#55651): Delete once all usages have been migrated to is_valid_email
def is_valid_email_legacy(
    _instance: Any, _attribute: attr.Attribute, value: str
) -> None:
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


def is_opt_valid_phone_number(
    _instance: Any, _attribute: attr.Attribute, value: str
) -> None:
    if value is not None:
        is_valid_phone_number(_instance, _attribute, value)


def is_valid_phone_number(
    _instance: Any, _attribute: attr.Attribute, string_value: str
) -> None:
    """
    Validates phone number matches NANP (North American Number Plan) format, i.e.
    it is a valid US/Canada phone number:
    - 10 digits, or 11 digits with leading 1
    - Area code (NXX): N=2-9, X=0-8, X=0-9, cannot end in 11
    - Exchange code (NXX): N=2-9, cannot end in 11
    """
    digits = string_value

    # Handle 11-digit numbers (must have leading 1)
    if len(digits) == 11:
        if digits[0] != "1":
            raise ValueError(
                f"Incorrect format: Phone number field with {string_value} is not valid"
            )
        digits = digits[1:]
    elif len(digits) != 10:
        raise ValueError(
            f"Incorrect format: Phone number field with {string_value} is not valid"
        )

    # Area code (indices 0-2) cannot end in "11"
    if digits[1:3] == "11":
        raise ValueError(
            f"Incorrect format: Phone number field with {string_value} has invalid area code"
        )

    # Exchange code (indices 3-5) cannot end in "11"
    if digits[4:6] == "11":
        raise ValueError(
            f"Incorrect format: Phone number field with {string_value} has invalid exchange code"
        )


INVALID_EMAIL_CHARACTERS = re.compile(r"[(),:;<>[\]\\]")
WHITESPACE_PATTERN = re.compile(r"\s")


def is_opt_valid_email(_instance: Any, _attribute: attr.Attribute, value: str) -> None:
    if value is not None:
        is_valid_email(_instance, _attribute, value)


def is_valid_email(instance: Any, attribute: attr.Attribute, value: str) -> None:
    """
    Checks if the given value is a valid email based on certain conditions
    Raises an error if an email fails to meet a requirement otherwise returns None
    """
    error_message_prefix = (
        f"Incorrectly formatted [{attribute.name}] value [{value}] on "
        f"[{type(instance).__name__}]:"
    )

    if "@" not in value:
        raise ValueError(f"{error_message_prefix} missing '@' symbol")
    if value.count("@") != 1:
        raise ValueError(f"{error_message_prefix}: more than one '@' symbol")
    if re.search(WHITESPACE_PATTERN, value):
        raise ValueError(f"{error_message_prefix}: contains whitespace")

    local_part, domain = value.split("@")  # only does this when value.count('@') is 1
    if not local_part:
        raise ValueError(f"{error_message_prefix}: has no text before '@' symbol")

    if not domain:
        raise ValueError(f"{error_message_prefix}: has no text after '@' symbol")

    if "." not in domain:
        raise ValueError(f"{error_message_prefix}: has no '.' in domain")

    if domain.startswith("."):
        raise ValueError(f"{error_message_prefix}: domain starts with '.'")
    if domain.endswith("."):
        raise ValueError(f"{error_message_prefix}: domain ends with '.'")

    matches = re.findall(INVALID_EMAIL_CHARACTERS, value)

    if matches:  # If email contains invalid character
        invalid_chars = ",".join(matches)
        raise ValueError(
            f"Incorrect format: Email field with {value} contains invalid character {invalid_chars}"
        )

    suspicious_usernames = ["x", "unknown", "none", "noname", "nobody", "test"]
    if local_part.lower() in suspicious_usernames:
        raise ValueError(
            f"{error_message_prefix}: has a suspicious username {local_part}"
        )


# String field validators
is_str = attr.validators.instance_of(str)
is_opt_str = is_opt(str)

# Int field validators
is_int = attr.validators.instance_of(int)
is_opt_int = is_opt(int)


def is_positive_int(instance: Any, attribute: attr.Attribute, value: int) -> None:
    """Validator that ensures the field value is a positive integer."""
    # First validate that it's an integer
    is_int(instance, attribute, value)

    if value < 1:
        raise ValueError(
            f"Field [{attribute.name}] on [{type(instance).__name__}] must be a "
            f"positive integer. Found value [{value}]"
        )


def is_opt_positive_int(
    instance: Any, attribute: attr.Attribute, value: int | None
) -> None:
    """Validator that ensures the field value is a positive integer or None."""
    # First validate that it's an optional integer
    is_opt_int(instance, attribute, value)
    if value is None:
        return

    if value < 1:
        raise ValueError(
            f"Field [{attribute.name}] on [{type(instance).__name__}] must be a "
            f"positive integer. Found value [{value}]"
        )


# Date field validators
def is_date(instance: Any, attribute: attr.Attribute, value: Any) -> None:
    """Validates that a field is a `date`, but NOT a `datetime` object."""
    if isinstance(value, datetime.datetime):
        raise TypeError(
            f"Found datetime value [{value}] on field [{attribute.name}] on class "
            f"[{type(instance).__name__}]. Expected the value to be a date, not a "
            f"datetime."
        )

    attr.validators.instance_of(datetime.date)(instance, attribute, value)


def is_opt_date(instance: Any, attribute: attr.Attribute, value: Any) -> None:
    """Validates that a field is a `date`, but NOT a `datetime` object. Allows None
    values.
    """
    if value is None:
        return

    is_date(instance, attribute, value)


def is_str_iso_formatted_date(
    _instance: Any, attribute: attr.Attribute, value: str
) -> None:
    # First validate that the value is a string
    try:
        is_str(_instance, attribute, value)
    except TypeError as e:
        raise TypeError(
            f"Field [{attribute.name}] must be a string in ISO-format. Found [{value}], which is [{type(value)}]."
        ) from e

    # Validate that that string is an ISO-formatted date
    try:
        datetime.date.fromisoformat(value)
    except ValueError as e:
        raise ValueError(
            f"Field [{attribute.name}] given a value [{value}] which does not appear to be in ISO format."
        ) from e


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

# Tuple field validators
is_tuple = attr.validators.instance_of(tuple)
is_opt_tuple = is_opt(tuple)


# Set field validators
is_set = attr.validators.instance_of(set)
is_opt_set = is_opt(set)


class IsSetOfValidator:
    def __init__(self, set_item_expected_type: Type) -> None:
        self._set_item_expected_type = set_item_expected_type

    def __call__(self, instance: Any, attribute: attr.Attribute, value: Any) -> None:
        if not isinstance(value, set):
            raise ValueError(
                f"Found value for set type field [{attribute.name}] on class "
                f"[{type(instance)}] which has non-set type [{type(value)}]."
            )
        for item in value:
            if not isinstance(item, self._set_item_expected_type):
                raise ValueError(
                    f"Found item in set type field [{attribute.name}] on class "
                    f"[{type(instance)}] which is not the expected type "
                    f"[{self._set_item_expected_type}]: {type(item)}"
                )


def is_set_of(set_item_expected_type: Type) -> IsSetOfValidator:
    return IsSetOfValidator(set_item_expected_type)


def _assert_value_falls_in_bounds(
    instance: Any,
    attribute: attr.Attribute,
    value: datetime.date | datetime.datetime,
    min_allowed_date_inclusive: datetime.date | datetime.datetime,
    max_allowed_date_exclusive: datetime.date | datetime.datetime,
) -> None:
    if value < min_allowed_date_inclusive:
        raise DateBelowLowerBoundError(
            f"Found [{attribute.name}] value on class [{type(instance).__name__}] with "
            f"value [{value.isoformat()}] which is less than "
            f"[{min_allowed_date_inclusive.isoformat()}], the (inclusive) min allowed "
            f"date."
        )

    if value >= max_allowed_date_exclusive:
        raise DateAboveUpperBoundError(
            f"Found [{attribute.name}] value on class [{type(instance).__name__}] with "
            f"value [{value.isoformat()}] which is greater than or equal to "
            f"[{max_allowed_date_exclusive.isoformat()}], the (exclusive) max allowed "
            f"date."
        )


class IsNotSetAlongWithValidator:
    def __init__(self, other_attribute: str) -> None:
        self._other_attribute = other_attribute

    def __call__(self, instance: Any, attribute: attr.Attribute, value: Any) -> None:
        if value and getattr(instance, self._other_attribute):
            raise TypeError(
                f"{attribute.name} and {self._other_attribute} cannot both be set on class [{type(instance).__name__}]"
            )


def is_not_set_along_with(other_attribute: str) -> IsNotSetAlongWithValidator:
    return IsNotSetAlongWithValidator(other_attribute)


class IsReasonableDateValidator:
    """Validator class for checking if a date field is within a reasonable set of
    bounds.
    """

    def __init__(
        self,
        min_allowed_date_inclusive: datetime.date,
        max_allowed_date_exclusive: datetime.date,
        allow_nulls: bool,
    ) -> None:
        self.min_allowed_date_inclusive = min_allowed_date_inclusive
        self.max_allowed_date_exclusive = max_allowed_date_exclusive
        self.allow_nulls = allow_nulls

    def __call__(self, instance: Any, attribute: attr.Attribute, value: Any) -> None:
        if value is None and self.allow_nulls:
            return

        if isinstance(value, datetime.datetime):
            raise TypeError(
                f"Found datetime value [{value}] on field [{attribute.name}] on class "
                f"[{type(instance).__name__}]. Expected the value to be a date, not a "
                f"datetime."
            )

        if not isinstance(value, datetime.date):
            raise TypeError(
                f"Found [{attribute.name}] value on class [{type(instance).__name__}] "
                f"with unexpected type [{type(value).__name__}]: {value}"
            )

        _assert_value_falls_in_bounds(
            instance,
            attribute,
            value,
            min_allowed_date_inclusive=self.min_allowed_date_inclusive,
            max_allowed_date_exclusive=self.max_allowed_date_exclusive,
        )


class IsReasonablePastDateValidator:
    """Validator class for checking if a date field is within a reasonable set of
    bounds strictly in the past (no future dates).
    """

    def __init__(
        self, min_allowed_date_inclusive: datetime.date, allow_nulls: bool
    ) -> None:
        self.min_allowed_date_inclusive = min_allowed_date_inclusive
        self.allow_nulls = allow_nulls

    def __call__(self, instance: Any, attribute: attr.Attribute, value: Any) -> None:
        if value is None and self.allow_nulls:
            return

        if isinstance(value, datetime.datetime):
            raise TypeError(
                f"Found datetime value [{value}] on field [{attribute.name}] on class "
                f"[{type(instance).__name__}]. Expected the value to be a date, not a "
                f"datetime."
            )

        if not isinstance(value, datetime.date):
            raise TypeError(
                f"Found [{attribute.name}] value on class [{type(instance).__name__}] "
                f"with unexpected type [{type(value).__name__}]: {value}"
            )

        tomorrow = datetime.datetime.now(tz=pytz.UTC).date() + datetime.timedelta(
            days=1
        )

        _assert_value_falls_in_bounds(
            instance,
            attribute,
            value,
            min_allowed_date_inclusive=self.min_allowed_date_inclusive,
            max_allowed_date_exclusive=tomorrow,
        )


class IsReasonableDatetimeValidator:
    """Validator class for checking if a datetime field is within a reasonable set of
    bounds.
    """

    def __init__(
        self,
        min_allowed_datetime_inclusive: datetime.datetime,
        max_allowed_datetime_exclusive: datetime.datetime,
        allow_nulls: bool,
    ) -> None:
        self.min_allowed_datetime_inclusive = min_allowed_datetime_inclusive
        self.max_allowed_datetime_exclusive = max_allowed_datetime_exclusive
        self.allow_nulls = allow_nulls

    def __call__(self, instance: Any, attribute: attr.Attribute, value: Any) -> None:
        if value is None and self.allow_nulls:
            return

        if not isinstance(value, datetime.datetime):
            raise TypeError(
                f"Found [{attribute.name}] value on class [{type(instance).__name__}] "
                f"with unexpected type [{type(value).__name__}]: {value}"
            )

        _assert_value_falls_in_bounds(
            instance,
            attribute,
            value,
            min_allowed_date_inclusive=self.min_allowed_datetime_inclusive,
            max_allowed_date_exclusive=self.max_allowed_datetime_exclusive,
        )


class IsReasonablePastDatetimeValidator:
    """Validator class for checking if a datetime field is within a reasonable set of
    bounds strictly in the past (no future dates).
    """

    def __init__(
        self, min_allowed_datetime_inclusive: datetime.datetime, allow_nulls: bool
    ) -> None:
        self.min_allowed_datetime_inclusive = min_allowed_datetime_inclusive
        self.allow_nulls = allow_nulls

    def __call__(self, instance: Any, attribute: attr.Attribute, value: Any) -> None:
        if value is None and self.allow_nulls:
            return

        if not isinstance(value, datetime.datetime):
            raise TypeError(
                f"Found [{attribute.name}] value on class [{type(instance).__name__}] "
                f"with unexpected type [{type(value).__name__}]: {value}"
            )

        if value.tzinfo:
            now = datetime.datetime.now(tz=value.tzinfo)
        else:
            now = datetime.datetime.now(tz=pytz.UTC).replace(tzinfo=None)

        _assert_value_falls_in_bounds(
            instance,
            attribute,
            value,
            min_allowed_date_inclusive=self.min_allowed_datetime_inclusive,
            max_allowed_date_exclusive=now,
        )
