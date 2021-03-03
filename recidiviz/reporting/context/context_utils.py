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

"""Utilities for working with report-specific context and data preparation."""
from datetime import datetime
from typing import Optional, List
import calendar


def format_greeting(name: Optional[str]) -> str:
    if not name:
        return "Hey there!"
    return f"Hey there, {format_name(name)}!"


def format_name(name: str) -> str:
    return name.title()


def format_date(str_date: str, current_format: str = "%Y-%m-%d") -> str:
    date = datetime.strptime(str_date, current_format)
    return datetime.strftime(date, "%m/%d/%Y")


def format_violation_type(violation_type: str) -> str:
    violation_types = {"NEW_CRIME": "New Crime", "TECHNICAL": "Technical Only"}
    return violation_types[violation_type]


def singular_or_plural(
    prepared_data: dict, value_key: str, text_key: str, singular: str, plural: str
) -> None:
    """Sets the text at the given text key in the prepared_data dictionary to either the singular or plural
    copy, based on the value at the provided value key."""
    value = int(prepared_data[value_key])

    if value == 1:
        prepared_data[text_key] = singular
    else:
        prepared_data[text_key] = plural


def month_number_to_name(month_number: str) -> str:
    """Converts the 1-based month number to the name of the month, titular-capitalized.

    Returns an empty string if month_number is 0. Wraps around as a normal Python array if month_number is negative.
    Raises an IndexError if month_number is >12.

    month_number_to_name("1") -> "January"
    month_number_to_name("5") -> "May"
    month_number_to_name("12") -> "December"
    month_number_to_name("-5") -> "August"
    """
    value = int(month_number)
    return calendar.month_name[value]


def round_float_value_to_int(value: str) -> str:
    """Rounds the given float value to an integer. Values are provided as strings and returned as strings to work
    with the interfaces in data preparation."""
    to_round = float(value)
    rounded = int(round(to_round))
    return str(rounded)


def round_float_value_to_number_of_digits(value: str, num_digits: int) -> str:
    """Rounds the given float value to the number of digits. Values are provided as strings and returned as
    strings to work with the interfaces in data preparation."""
    to_round = float(value)
    if to_round < 1:
        # If the value is less than 1.0, then we want one less digit because the 0 will be one of the digits
        num_digits -= 1
    rounded = round(to_round, ndigits=num_digits)
    return str(rounded)


def align_columns(rows: List[List]) -> str:
    # Reorganize data by columns
    columns = zip(*rows)
    # Compute column widths by taking maximum length of values per column
    column_widths = [max(len(value) for value in col) for col in columns]
    # Formatter outputs string on the left side, padded to width + 4 characters
    formatter = " ".join(["%%-%ds" % (width + 4) for width in column_widths])
    # Print each row using the computed format
    return "\n".join([formatter % tuple(row) for row in rows])
