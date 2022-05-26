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
    ATTORNEY = "ATTORNEY"
    SUPERVISOR = "SUPERVISOR"
    ADMINISTRATIVE = "ADMINISTRATIVE"
    SUPPORT_OR_EXPERT = "SUPPORT_OR_EXPERT"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/staff/prosecution_defense/type"


class CaseSeverityType(DimensionBase, enum.Enum):
    FELONY = "FELONY"
    MISDEMEANOR = "MISDEMEANOR"
    INFRACTION = "INFRACTION"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/severity/prosecution/type"


class DispositionType(DimensionBase, enum.Enum):
    REJECTION = "REJECTION"
    DISMISSAL = "DISMISSAL"
    DEFERRAL_OR_DIVERION = "DEFERRAL_OR_DIVERSION"
    AQUITAL = "AQUITAL"
    CONVICTION_PLEA = "CONVICTION_PLEA"
    CONVICTION_TRIAL = "CONVICTION_TRIAL"
    TRANSFER = "TRANSFER"
    OTHER = "OTHER"
    UNKNOWN = "UNKOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/disposition/type"
