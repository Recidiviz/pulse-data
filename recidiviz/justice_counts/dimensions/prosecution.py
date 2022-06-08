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
"""Dimension subclasses used for prosecution metrics."""

import enum

from recidiviz.justice_counts.dimensions.base import DimensionBase


class ProsecutionAndDefenseStaffType(DimensionBase, enum.Enum):
    ATTORNEY = "Attorney"
    SUPERVISOR = "Supervisor"
    ADMINISTRATIVE = "Administrative"
    SUPPORT_OR_EXPERT = "Support or Expert"
    OTHER = "Other"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/staff/prosecution_defense/type"


class CaseSeverityType(DimensionBase, enum.Enum):
    FELONY = "Felony"
    MISDEMEANOR = "Misdemeanor"
    INFRACTION = "Infraction"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/severity/prosecution/type"


class DispositionType(DimensionBase, enum.Enum):
    ACQUITTAL = "Acquittal"
    CONVICTION_PLEA = "Conviction - Plea"
    CONVICTION_TRIAL = "Conviction - Trial"
    DISMISSAL = "Dismissal"
    DIVERTED_OR_DEFERRED = "Diverted or Deferred"
    REJECTED = "Rejected"
    TRANSFER = "Transfer"
    OTHER = "Other"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/disposition/type"
