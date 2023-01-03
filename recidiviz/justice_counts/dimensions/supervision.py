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
"""Dimension subclasses used for Supervision metrics."""

import enum

from recidiviz.justice_counts.dimensions.base import DimensionBase


class SupervisionFundingType(DimensionBase, enum.Enum):
    STATE_APPROPRIATION = "State Appropriation"
    COUNTY_MUNICIPAL_APPROPRIATION = "County or Municipal Appropriation"
    GRANTS = "Grants"
    FINES_FEES = "Fines and Fees"
    OTHER = "Other Funding"
    UNKNOWN = "Unknown Funding"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/funding/type"


class SupervisionStaffType(DimensionBase, enum.Enum):
    SUPERVISION = "Supervision Staff"
    MANAGEMENT_AND_OPERATIONS = "Management and Operations Staff"
    CLINICAL_OR_MEDICAL = "Clinical or Medical Staff"
    PROGRAMMATIC = "Programmatic Staff"
    OTHER = "Other Staff"
    UNKNOWN = "Unknown Staff"
    VACANT = "Vacant Positions (Any Staff Type)"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/staff/supervision/type"


class SupervisionViolationType(DimensionBase, enum.Enum):
    TECHNICAL = "Technical"
    NEW_OFFENSE = "New Offense"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/violation/supervision/type"


class SupervisionCaseType(DimensionBase, enum.Enum):
    ACTIVE = "Active"
    PASSIVE = "Passive"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/case/type"


class SupervisionDailyPopulationType(DimensionBase, enum.Enum):
    ACTIVE = "People on Active Supervision"
    ADMINISTRATIVE = "People on Administrative Supervision"
    ABSCONDED = "People who have Absconded from Supervision"
    HOLD_OR_SANCTION = "People Incarcerated on a Hold or Sanction"
    OTHER = "Other Status"
    UNKNOWN = "Unknown Status"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/daily_population/type"


class SupervisionTerminationType(DimensionBase, enum.Enum):
    SUCCESSFUL = "Successful"
    UNSUCCESSFUL_OR_REVOKED = "Unsuccessful or Revoked"
    OTHER = "Other"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/termination/type"


class NewOffenseType(DimensionBase, enum.Enum):
    VIOLENT = "Violent"
    PROPERTY = "Property"
    DRUG = "Drug"
    OTHER = "Other"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/offense/type"
