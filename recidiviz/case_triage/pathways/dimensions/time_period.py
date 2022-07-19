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
# ============================================================================
""" Time period dimension """
import enum
from typing import Dict, List

from recidiviz.case_triage.pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.pathways.dimensions.dimension_transformer import (
    register_dimension_transformer,
)


class TimePeriod(enum.Enum):
    """Enum for available time periods"""

    MONTHS_0_6 = "months_0_6"
    MONTHS_7_12 = "months_7_12"
    MONTHS_13_24 = "months_13_24"
    MONTHS_25_60 = "months_25_60"

    @classmethod
    def month_map(cls) -> Dict[str, int]:
        return {member.value: int(member.value.split("_")[1]) for member in cls}

    @classmethod
    def period_range(cls, time_period: str) -> List[str]:
        month_map = cls.month_map()

        return [
            member.value
            for member in TimePeriod
            if month_map[member.value] <= month_map[time_period]
        ]


def dimension_transformer(values: List[str]) -> List[str]:
    if len(values) == 1:
        return TimePeriod.period_range(values[0])

    return [member.value for member in TimePeriod if member.value in values]


register_dimension_transformer(
    Dimension.TIME_PERIOD,
    dimension_transformer,
)
