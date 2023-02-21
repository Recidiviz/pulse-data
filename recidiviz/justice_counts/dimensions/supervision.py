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


class FundingType(DimensionBase, enum.Enum):
    STATE_APPROPRIATION = "State Appropriations"
    COUNTY_MUNICIPAL_APPROPRIATION = "County or Municipal Appropriations"
    GRANTS = "Grants"
    FINES_FEES = "Fines and Fees"
    OTHER = "Other Funding"
    UNKNOWN = "Unknown Funding"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/funding/type"


class StaffType(DimensionBase, enum.Enum):
    SUPERVISION = "Supervision Staff"
    MANAGEMENT_AND_OPERATIONS = "Management and Operations Staff"
    CLINICAL_AND_MEDICAL = "Clinical and Medical Staff"
    PROGRAMMATIC = "Programmatic Staff"
    OTHER = "Other Staff"
    UNKNOWN = "Unknown Staff"
    VACANT = "Vacant Positions (Any Staff Type)"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/staff/supervision/type"


class ViolationType(DimensionBase, enum.Enum):
    TECHNICAL = "Technical Violations"
    ABSCONDING = "Absconding Violations"
    NEW_OFFENSE = "New Offense Violations"
    OTHER = "Other Violations"
    UNKNOWN = "Unknown Violations"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/violation/supervision/type"


class DailyPopulationType(DimensionBase, enum.Enum):
    ACTIVE = "People on Active Supervision"
    ADMINISTRATIVE = "People on Administrative Supervision"
    ABSCONDED = "People who have Absconded from Supervision"
    HOLD_OR_SANCTION = "People Incarcerated on a Hold or Sanction while on Supervision"
    OTHER = "Other Supervision Status"
    UNKNOWN = "Unknown Supervision Status"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/daily_population/type"


class DischargeType(DimensionBase, enum.Enum):
    SUCCESSFUL = "Successful Completions of Supervision"
    NEUTRAL = "Neutral Discharges from Supervision"
    UNSUCCESSFUL = "Unsuccessful Discharges from Supervision"
    OTHER = "Other Discharges from Supervision"
    UNKNOWN = "Unknown Discharges from Supervision"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/discharge/type"


class NewOffenseType(DimensionBase, enum.Enum):
    VIOLENT = "Violent"
    PROPERTY = "Property"
    DRUG = "Drug"
    OTHER = "Other"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/offense/type"


class RevocationType(DimensionBase, enum.Enum):
    TECHNICAL = "Revocations for Technical Violations"
    NEW_OFFENSE = "Revocations for New Offense Violations"
    OTHER = "Revocations for other reasons"
    UNKNOWN = "Revocations for unknown reasons"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/revocation/type"
