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
"""Dimension subclasses used for defense metrics."""

import enum

from recidiviz.justice_counts.dimensions.base import DimensionBase


class FundingType(DimensionBase, enum.Enum):
    STATE_APPROPRIATION = "State Appropriation"
    COUNTY_OR_MUNICIPAL_APPROPRIATION = "County or Municipal Appropriation"
    GRANTS = "Grants"
    FEES = "Fees"
    OTHER = "Other Funding"
    UNKNOWN = "Unknown Funding"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/funding/defense/type"


class StaffType(DimensionBase, enum.Enum):
    LEGAL = "Legal Staff"
    ADMINISTRATIVE = "Administrative Staff"
    INVESTIGATIVE = "Investigative Staff"
    OTHER = "Other Staff"
    UNKNOWN = "Unknown Unknown"
    VACANT = "Vacant Positions (Any Staff Type)"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/defense/staff/type"


class CaseAppointedSeverityType(DimensionBase, enum.Enum):
    FELONY = "Felony Cases"
    MISDEMEANOR = "Misdemeanor Cases"
    OTHER = "Other Cases"
    UNKNOWN = "Unknown Cases"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/appointed/severity/prosecution/type"
