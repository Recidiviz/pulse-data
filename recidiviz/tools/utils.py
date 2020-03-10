# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Utils methods for all scripts in the tools directory."""

import datetime
from typing import Optional


def is_date_str(potential_date_str: str) -> bool:
    """Returns True if the string is an ISO-formatted date, (e.g. '2019-09-25'), False otherwise."""
    try:
        datetime.datetime.strptime(potential_date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def is_between_date_strs_inclusive(
        *, upper_bound_date: Optional[str], lower_bound_date: Optional[str], date_of_interest: str) -> bool:
    """Returns true if the provided |date_of_interest| is between the provided |upper_bound_date| and
    |lower_bound_date|.
    """

    if (lower_bound_date is None or date_of_interest >= lower_bound_date) \
            and (upper_bound_date is None or date_of_interest <= upper_bound_date):
        return True
    return False
