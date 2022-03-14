# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Custom parser functions for US_ME. Can be referenced in an ingest view manifest
like this:

my_flat_field:
    $custom:
        $function: us_me_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""

from recidiviz.common.date import safe_strptime
from recidiviz.common.str_field_utils import parse_days_from_duration_pieces, parse_int


def total_days_from_ymd(
    years: str = None,
    months: str = None,
    days: str = None,
    start_date: str = None,
) -> str:
    """Returns a total number of days equal to the given number of years, months, and days, as a string.

    If a start_date is provided, will add the components to that date to compute an end date and compute the difference.
    If not, will simply multiply and sum them together arithmetically.

    If the start_date is 9999-12-31, then it will set it to None.

    If all of years, months, and days are none, this returns "0".
    """
    # Date format is "YYYY-MM-DD HH:MM:SS UTC"
    format_est_sent_date = safe_strptime(start_date, "%Y-%m-%d %H:%M:%S")
    if format_est_sent_date and format_est_sent_date.year >= 9900:
        start_date = None
    if not years and not months and not days:
        return "0"
    return str(parse_days_from_duration_pieces(years, months, days, start_date))


def compute_earned_time(
    days_earned: str = None, days_lost: str = None, days_restored: str = None
) -> str:
    """Returns the total number of days earned time available for this sentence, as a string.

    Equal to days earned minus days lost plus days restored.
    """
    earned = parse_int(days_earned) if days_earned else 0
    lost = parse_int(days_lost) if days_lost else 0
    restored = parse_int(days_restored) if days_restored else 0
    return str(earned - lost + restored)
