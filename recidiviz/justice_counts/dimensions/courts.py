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
    CLERK = "Clerk"
    JUDICIAL = "Judicial"
    ADMINISTRATIVE = "Administrative"
    SUPPORT = "Support"
    OTHER = "Other"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/courts/staff/type"


class CourtReleaseType(DimensionBase, enum.Enum):
    ROR = "ROR Release"
    MONETARY_BAIL = "Monetary Bail Release"
    SUPERVISION_OR_EM = "Supervision or EM"
    OTHER = "Other"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/courts/release/type"


class SentenceType(DimensionBase, enum.Enum):
    DEATH = "Death"
    LIFE = "Life"
    INCARCERATION = "Incarceration"
    SUPERVISION = "Supervision"
    FINANCIAL_OBLIGATIONS = "Financial Obligations"
    OTHER = "Other"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/courts/sentence/type"


class CourtsCaseSeverityType(DimensionBase, enum.Enum):
    FELONY = "Felony"
    MISDEMEANOR = "Misdemeanor"
    INFRACTION = "Infraction"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/courts/case/severity/type"


class CourtCaseType(DimensionBase, enum.Enum):
    VIOLENT = "Violent"
    NON_VIOLENT = "Non-violent"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/courts/case/type"
