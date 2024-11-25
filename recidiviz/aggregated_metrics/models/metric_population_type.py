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
"""Constants related to a MetricPopulationType."""
from enum import Enum


class MetricPopulationType(Enum):
    """The type of population over which to calculate metrics."""

    INCARCERATION = "INCARCERATION"
    SUPERVISION = "SUPERVISION"
    JUSTICE_INVOLVED = "JUSTICE_INVOLVED"
    # Use `CUSTOM` enum for ad-hoc population definitions
    CUSTOM = "CUSTOM"

    @property
    def population_name_short(self) -> str:
        return self.value.lower()

    @property
    def population_name_title(self) -> str:
        return self.value.title().replace("_", " ")
