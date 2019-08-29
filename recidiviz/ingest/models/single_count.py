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

"""Object to hold a single count that's ingested."""
import datetime
from typing import Any, Optional

import attr

from recidiviz.common import str_field_utils
from recidiviz.common.constants.person_characteristics import Ethnicity, \
    Gender, Race


def _date_converter(value: Any) -> datetime.date:
    if not value:
        return datetime.date.today()

    if isinstance(value, datetime.date):
        return value

    parsed_date = str_field_utils.parse_date(value)
    if not parsed_date:
        raise ValueError(f"Failed to parse {value} as a date")

    return parsed_date


@attr.s(frozen=True)
class SingleCount:
    """Single count measure"""

    # The count.
    count: int = attr.ib(converter=int)
    # Optionally, this count can be ethnicity, race, or gender specific.
    ethnicity: Optional[Ethnicity] = attr.ib(
        default=None, converter=attr.converters.optional(Ethnicity))
    gender: Optional[Gender] = attr.ib(
        default=None, converter=attr.converters.optional(Gender))
    race: Optional[Race] = attr.ib(
        default=None, converter=attr.converters.optional(Race))
    # Date, or today
    date: datetime.date = attr.ib(default=None, converter=_date_converter)
