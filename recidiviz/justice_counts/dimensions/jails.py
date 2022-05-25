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
"""Dimension subclasses used for Jail system metrics."""


import enum

from recidiviz.justice_counts.dimensions.base import DimensionBase


class ReadmissionType(DimensionBase, enum.Enum):
    NEW_OFFENSE = "NEW_OFFENSE"
    VIOLATION_OF_CONDITIONS = "VIOLATION_OF_CONDITIONS"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/reported_crime/type"


class JailPopulationType(DimensionBase, enum.Enum):
    PRETRIAL = "PRETRIAL"
    SENTENCED = "SENTENCED"
    TRANSFER_OR_HOLD = "TRANSFER_OR_HOLD"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/population/type"


class ReleaseType(DimensionBase, enum.Enum):
    SCENTENCE_COMPLETION = "SCENTENCE_COMPLETION"
    PRETRIAL_RELEASE = "PRETRIAL_RELEASE"
    TRANSFER = "TRANSFER"
    UNAPPROVED_ABSENCE = "UNAPPROVED_ABSENCE"
    COMPASSIONATE = "COMPASSIONATE"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/release/type"


class JailForceType(DimensionBase, enum.Enum):
    PHYSICAL = "PHYSICAL"
    RESTRAINT = "RESTRAINT"
    VERBAL = "VERBAL"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/force/type"


class JailStaffType(DimensionBase, enum.Enum):
    SECURITY = "SECURITY"
    SUPPORT = "SUPPORT"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/staff/jails/type"
