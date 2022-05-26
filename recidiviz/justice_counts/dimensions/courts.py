# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Defines all Justice Counts dimensions for Courts and Pretrial"""
import enum

from recidiviz.justice_counts.dimensions.base import DimensionBase


class CourtStaffType(DimensionBase, enum.Enum):
    CLERK = "CLERK"
    JUDICIAL = "JUDICIAL"
    ADMINISTRATIVE = "ADMINISTRATIVE"
    SUPPORT = "SUPPORT"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/courts/staff/type"


class CourtReleaseType(DimensionBase, enum.Enum):
    ROR = "ROR"
    MONETARY_BAIL = "MONETARY_BAIL"
    SUPERVISION_OR_EM = "SUPERVISION_OR_EM"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/courts/release/type"


class SentenceType(DimensionBase, enum.Enum):
    DEATH = "DEATH"
    LIFE = "LIFE"
    INCARCERATION = "INCARCERATION"
    SUPERVISION = "SUPERVISION"
    FINANCIAL_OBLIGATIONS = "FINANCIAL_OBLIGATIONS"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/courts/sentence/type"


class CourtsCaseSeverityType(DimensionBase, enum.Enum):
    FELONY = "FELONY"
    MISDEMEANOR = "MISDEMEANOR"
    INFRACTION = "INFRACTION"
    APPEAL = "APPEAL"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/courts/case/severity/type"


class CourtCaseType(DimensionBase, enum.Enum):
    VIOLENT = "VIOLENT"
    NON_VIOLENT = "NON_VIOLENT"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/courts/case/type"
