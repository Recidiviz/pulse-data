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
# along with this program.   not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Contains utilities for working with datetimes."""

import calendar
from datetime import datetime
from typing import Optional, Tuple

from recidiviz.common.text_analysis import TextAnalyzer
from recidiviz.justice_counts.bulk_upload.bulk_upload_helpers import (
    fuzzy_match_against_options,
)

MONTH_NAMES = list(calendar.month_name)


def convert_date_range_to_year_month(
    start_date: datetime, end_date: datetime
) -> Tuple[int, Optional[int]]:
    if (
        # this case handles Jan - Nov
        end_date.year == start_date.year
        and end_date.month - start_date.month == 1
    ):
        return (start_date.year, start_date.month)
    if (
        # This case handles Dec (whose end date is Jan 1 of next year)
        end_date.year == start_date.year + 1
        and end_date.month - start_date.month == -11
    ):
        return (start_date.year, start_date.month)
    if end_date.year == start_date.year + 1 and end_date.month == start_date.month:
        return (start_date.year, None)
    raise ValueError(f"Invalid report start and end: {start_date}, {end_date}")


def get_month_value_from_string(month: str, text_analyzer: TextAnalyzer) -> int:
    """Takes as input a string and attempts to find the corresponding month
    index using the calendar module's month_names enum. For instance,
    March -> 3. Uses fuzzy matching to handle typos, such as `Febuary`."""
    column_value = month.title()
    if column_value not in MONTH_NAMES:
        column_value = fuzzy_match_against_options(
            analyzer=text_analyzer,
            category_name="Month",
            text=column_value,
            options=MONTH_NAMES,
        )
    return MONTH_NAMES.index(column_value)


def get_annual_year_from_fiscal_year(fiscal_year: str) -> Optional[str]:
    """Takes as input a string and attempts to find the corresponding year"""
    return fiscal_year[0 : fiscal_year.index("-")]
