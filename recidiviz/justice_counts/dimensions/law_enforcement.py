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


class CallType(DimensionBase, enum.Enum):
    EMERGENCY = "Emergency Calls"
    NON_EMERGENCY = "Non-emergency Calls"
    OTHER = "Other Calls"
    UNKNOWN = "Unknown Calls"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/law_enforcement/calls_for_service/type"


class FundingType(DimensionBase, enum.Enum):
    STATE_APPROPRIATION = "State Appropriation"
    COUNTY_APPROPRIATION = "County or Municipal Appropriation"
    ASSET_FORFEITURE = "Asset Forfeiture"
    GRANTS = "Grants"
    OTHER = "Other Funding"
    UNKNOWN = "Unknown Funding"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/law_enforcement/funding/type"


class ForceType(DimensionBase, enum.Enum):
    PHYSICAL = "Physical Force"
    RESTRAINT = "Restraint"
    FIREARM = "Firearm"
    OTHER_WEAPON = "Other Weapon"
    OTHER = "Other Force"
    UNKNOWN = "Unknown Force"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/law_enforcement/officer_use_of_force_incidents/type"


class ComplaintType(DimensionBase, enum.Enum):
    EXCESSIVE_USES_OF_FORCE = "Excessive Uses of Force"
    DISCRIMINATION = "Discrimination or Racial Bias"
    OTHER = "Other Complaints"
    UNKNOWN = "Unknown Complaints"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/law_enforcement/complaint/type"


class StaffType(DimensionBase, enum.Enum):
    LAW_ENFORCEMENT_OFFICERS = "Sworn/Uniformed Police Officers"
    CIVILIAN_STAFF = "Civilian Staff"
    MENTAL_HEALTH = "Mental Health and Crisis Intervention Team Staff"
    VICTIM_ADVOCATES = "Victim Advocate Staff"
    OTHER = "Other Staff"
    UNKNOWN = "Unknown Staff"
    VACANT = "Vacant Positions (Any Staff Type)"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/law_enforcement/staff/type"
