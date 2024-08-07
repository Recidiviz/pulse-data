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
import calendar
import json
from datetime import datetime
from typing import List, Optional


def format_greeting(name: Optional[str]) -> str:
    if not name:
        return "Hey!"
    return f"Hey, {format_given_name(name)}!"


def format_given_name(name: str) -> str:
    return name.title()


def format_full_name(full_name: str, last_name_first: bool = False) -> str:
    """Extracts normalized text from a `full_name` JSON blob"""
    name_parts = json.loads(full_name)
    # TODO(#7957): We shouldn't be converting to title-case because there
    # are many names whose preferred casing is not that. Once we figure out
    # how to access the original name casing, we should use that wherever possible.
    given_names = name_parts.get("given_names", "").title()
    surname = name_parts.get("surname", "").title()
    if last_name_first:
        # avoid superfluous comma if one part is missing
        return ", ".join([n for n in (surname, given_names) if n])

    # strip to remove superfluous space if one part is missing
    return " ".join([given_names, surname]).strip()


def format_date(str_date: str, current_format: str = "%Y-%m-%d") -> str:
    date = datetime.strptime(str_date, current_format)
    return datetime.strftime(date, "%m/%d/%Y")


def format_violation_type(violation_type: str) -> str:
    violation_types = {"NEW_CRIME": "New Crime", "TECHNICAL": "Technical Only"}
    return violation_types[violation_type]


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


def align_columns(rows: List[List]) -> str:
    # Reorganize data by columns
    columns = zip(*rows)
    # Compute column widths by taking maximum length of values per column
    column_widths = [max(len(value) for value in col) for col in columns]
    # Formatter outputs string on the left side, padded to width + 4 characters
    formatter = " ".join([f"%-{width + 4}s" for width in column_widths])
    # Print each row using the computed format
    return "\n".join([formatter % tuple(row) for row in rows])
