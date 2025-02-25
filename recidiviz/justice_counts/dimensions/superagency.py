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

"""Dimension subclasses used for superagency metrics."""

import enum

from recidiviz.justice_counts.dimensions.base import DimensionBase


class FundingType(DimensionBase, enum.Enum):
    STATE_APPROPRIATIONS = "State Appropriations"
    COUNTY_OR_MUNICIPAL_APPROPRIATIONS = "County or Municipal Appropriations"
    GRANTS = "Grants"
    OTHER = "Other Funding"
    UNKNOWN = "Unknown Funding"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/superagency/funding/type"


class ExpenseType(DimensionBase, enum.Enum):
    PERSONNEL = "Personnel"
    TRAINING = "Training"
    FACILITIES_AND_EQUIPMENT = "Facilities and Equipment"
    OTHER = "Other Expenses"
    UNKNOWN = "Unknown Expenses"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/superagency/expenses/type"


class StaffType(DimensionBase, enum.Enum):
    FILLED = "Filled Positions (Any Staff Type)"
    VACANT = "Vacant Positions (Any Staff Type)"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/superagency/staff/type"
