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
    PATROL = "Patrol"
    DETENTION = "Detention"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/law_enforcement/budget/type"


class CallType(DimensionBase, enum.Enum):
    EMERGENCY = "Emergency"
    NON_EMERGENCY = "Non-emergency"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/law_enforcement/calls_for_service/type"


class OffenseType(DimensionBase, enum.Enum):
    PERSON = "Person"
    PROPERTY = "Property"
    DRUG = "Drug"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/law_enforcement/reported_crime/type"


class ForceType(DimensionBase, enum.Enum):
    PHYSICAL = "Physical"
    RESTRAINT = "Restraint"
    VERBAL = "Verbal"
    WEAPON = "Weapon"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/law_enforcement/officer_use_of_force_incidents/type"


class LawEnforcementStaffType(DimensionBase, enum.Enum):
    LAW_ENFORCEMENT_OFFICERS = "Law enforcement officers"
    CIVILIAN_STAFF = "Civilian staff"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/law_enforcement/staff/type"
