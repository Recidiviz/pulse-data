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

"""Utilities used by ingest models."""
import datetime
from typing import Any

from recidiviz.common import str_field_utils


def date_converter_or_today(value: Any) -> datetime.date:
    """Converts a value to a datetime.date, if possible. If the value is
    falsy, datetime.date.today() is returned. If the value is already
    a datetime.date, it is returned directly, otherwise the value is
    sent to the string utils dateparser.
    """
    if not value:
        return datetime.date.today()

    if isinstance(value, datetime.date):
        return value

    parsed_date = str_field_utils.parse_date(value)
    if not parsed_date:
        raise ValueError(f"Failed to parse {value} as a date")

    return parsed_date
