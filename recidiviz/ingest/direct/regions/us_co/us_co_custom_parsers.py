# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Custom parser functions for US_CO. Can be referenced in an ingest view manifest
like this:

my_flat_field:
    $custom:
        $function: us_co_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""

from datetime import datetime
from typing import Optional

from recidiviz.common.date import calendar_unit_date_diff
from recidiviz.common.str_field_utils import parse_datetime


# TODO(#55775): We could likely use this function directly in the mappings, but that would require a bit of a refactor
# of the ingest view to allow us to chose which date field to use instead of just nulling out/dropping invalid dates
def null_far_future_date(date: str) -> Optional[str]:
    """
    Some placeholder / erroneous dates appear very far in the future, and result in parsing
    errors. With this function, we null out dates that are 100+ years in the future, since
    these are either incorrect or simply indicating a life sentence. Note that this isn't used
    on '9999-' magic dates, which are handled within the view logic itself.
    """
    future_date = parse_datetime(date)
    if future_date is not None:
        distance_to_date = future_date - datetime.now()
        if distance_to_date.days / 365 < 100:
            return date
    return None


def date_diff_in_days(start: str, end: str) -> Optional[str]:
    if (
        start > end
        or null_far_future_date(start) is None
        or null_far_future_date(end) is None
    ):
        return None
    result = calendar_unit_date_diff(start_date=start, end_date=end, time_unit="days")
    if result is None:
        return None
    return str(result)
