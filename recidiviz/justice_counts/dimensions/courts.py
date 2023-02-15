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


class StaffType(DimensionBase, enum.Enum):
    JUDGES = "Judges"
    LEGAL = "Legal Staff"
    SECURITY = "Security Staff"
    ADMINISTRATIVE = "Support or Administrative Staff"
    ADVOCATE = "Victim Advocate Staff"
    OTHER = "Other Staff"
    UNKNOWN = "Unknown Staff"
    VACANT = "Vacant Positions (Any Staff Type)"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/courts/staff/type"


class ReleaseType(DimensionBase, enum.Enum):
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


class CaseSeverityType(DimensionBase, enum.Enum):
    FELONY = "Felony Criminal Case Filings"
    MISDEMEANOR = "Misdemeanor or Infraction Criminal Case Filings"
    OTHER = "Other Criminal Case Filings"
    UNKNOWN = "Unknown Criminal Case Filings"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/courts/case/severity/type"
