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
"""Misc helpers / constants for ingest.

TODO(#20930): Move the contents of this file to more relevant locations
"""
import datetime
from typing import Any

UPPER_BOUND_DATETIME_COL_NAME = "__upper_bound_datetime_inclusive"
LOWER_BOUND_DATETIME_COL_NAME = "__lower_bound_datetime_exclusive"
MATERIALIZATION_TIME_COL_NAME = "__materialization_time"


def to_string_value_converter(
    field_name: str,
    value: Any,
) -> str:
    """Converts all values to strings."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (bool, int)):
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()

    raise ValueError(
        f"Unexpected value type [{type(value)}] for field [{field_name}]: {value}"
    )
