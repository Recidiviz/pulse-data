#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Custom parser functions for US_TN. Can be referenced in an ingest view manifest
like this:
my_flat_field:
    $custom:
        $function: us_tn_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""
from typing import Optional

from dateutil.relativedelta import relativedelta

from recidiviz.common.str_field_utils import (
    parse_datetime,
    parse_days_from_duration_pieces,
    parse_duration_pieces,
)


def get_length_in_days(
    years: str, months: str, days: str, effective_date: str
) -> Optional[str]:
    """Returns the duration in days from a start date with given number of years, months
    and days."""
    if years or months or days:
        return str(
            parse_days_from_duration_pieces(
                years_str=years,
                months_str=months,
                days_str=days,
                start_dt_str=effective_date,
            )
        )
    return None


def get_maximum_date(
    years_str: str, months_str: str, days_str: str, effective_date: str, max_date: str
) -> Optional[str]:
    if years_str or months_str or days_str:
        years, months, _, days, _ = parse_duration_pieces(
            years_str=years_str, months_str=months_str, days_str=days_str
        )
        start_dt = parse_datetime(effective_date)
        if start_dt:
            end_dt = start_dt + relativedelta(years=years, months=months, days=days)
            return str(end_dt)
    return max_date
