# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Dimension subclasses used for Law Enforcement system metrics."""

import enum

from recidiviz.justice_counts.dimensions.base import DimensionBase


class SheriffBudgetType(DimensionBase, enum.Enum):
    PATROL = "PATROL"
    DETENTION = "DETENTION"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/law_enforcement/budget/type"

    @property
    def dimension_value(self) -> str:
        return self.value


class CallType(DimensionBase, enum.Enum):
    EMERGENCY = "EMERGENCY"
    NON_EMERGENCY = "NON_EMERGENCY"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/law_enforcement/calls_for_service/type"

    @property
    def dimension_value(self) -> str:
        return self.value
