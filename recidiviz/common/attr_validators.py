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
        raise ValueError(
            f"Found [{attribute.name}] value on class [{type(instance).__name__}] with "
            f"value [{value.isoformat()}] which is less than "
            f"[{min_allowed_date_inclusive.isoformat()}], the (inclusive) min allowed "
            f"date."
        )

    if value >= max_allowed_date_exclusive:
        raise ValueError(
            f"Found [{attribute.name}] value on class [{type(instance).__name__}] with "
            f"value [{value.isoformat()}] which is greater than or equal to "
            f"[{max_allowed_date_exclusive.isoformat()}], the (exclusive) max allowed "
            f"date."
        )


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
