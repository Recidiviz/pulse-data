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


class SupervisionStaffType(DimensionBase, enum.Enum):
    SUPERVISION_OFFICERS = "SUPERVISION_OFFICERS"
    SUPPORT = "SUPPORT"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/staff/supervision/type"


class SupervisionViolationType(DimensionBase, enum.Enum):
    TECHNICAL = "TECHNICAL"
    NEW_OFFENSE = "NEW_OFFENSE"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/violation/supervision/type"


class SupervisionCaseType(DimensionBase, enum.Enum):
    ACTIVE = "ACTIVE"
    PASSIVE = "PASSIVE"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/case/type"


class SupervisionIndividualType(DimensionBase, enum.Enum):
    ACTIVE = "ACTIVE"
    PASSIVE = "PASSIVE"
    ABSCONDER_STATUS = "ABSCONDER_STATUS"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/individual/type"


class SupervisionTerminationType(DimensionBase, enum.Enum):
    SUCCESSFUL = "SUCCESSFUL"
    UNSUCCESSFUL_OR_REVOKED = "UNSUCCESSFUL_OR_REVOKED"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/termination/type"


class NewOffenseType(DimensionBase, enum.Enum):
    VIOLENT = "VIOLENT"
    PROPERTY = "PROPERTY"
    DRUG = "DRUG"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/supervision/offense/type"
