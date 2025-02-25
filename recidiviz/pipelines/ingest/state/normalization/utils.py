# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""
Utility functions for normalization.
"""
import datetime
from typing import TypeVar

# Ensures that MyPy can check that get_min_max_fields uses comparable
# and consistent types. Ex: we can't compare an int to a date
_MinMaxType = TypeVar("_MinMaxType", datetime.date, datetime.datetime, int)


def get_min_max_fields(
    min_field: _MinMaxType | None, max_field: _MinMaxType | None
) -> tuple[_MinMaxType | None, _MinMaxType | None]:
    """
    Given two fields that represent the min and max of a concept,
    returns the min and max of the two fields.
    If both values do not exist, returns the input.
    """
    if min_field and max_field:
        return min(min_field, max_field), max(min_field, max_field)
    return min_field, max_field
