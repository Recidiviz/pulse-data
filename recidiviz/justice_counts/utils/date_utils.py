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

import datetime
from typing import Optional, Tuple


def convert_date_range_to_year_month(
    start_date: datetime.date, end_date: datetime.date
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


def get_annual_year_from_fiscal_year(fiscal_year: str) -> Optional[str]:
    """Takes as input a string and attempts to find the corresponding year"""
    return fiscal_year[0 : fiscal_year.index("-")]
