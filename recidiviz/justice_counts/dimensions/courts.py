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


class FundingType(DimensionBase, enum.Enum):
    STATE_APPROPRIATION = "State Appropriation"
    COUNTY_OR_MUNICIPAL_APPROPRIATION = "County or Municipal Appropriation"
    GRANTS = "Grants"
    OTHER = "Other Funding"
    UNKNOWN = "Unknown Funding"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/courts/funding/type"


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
    ON_OWN = "On Own Recognizance"
    MONETARY_BAIL = "Monetary Bail"
    NON_MONETARY_BAIL = "Non-Monetary Bail"
    OTHER = "Other Pretrial Releases"
    UNKNOWN = "Unknown Pretrial Releases"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/courts/release/type"


class SentenceType(DimensionBase, enum.Enum):
    PRISON = "Prison Sentences"
    JAIL = "Jail Sentences"
    SPLIT = "Split Sentences"
    SUSPENDED = "Suspended Sentences"
    COMMUNITY_SUPERVISION = "Community Supervision Only Sentences"
    FINES_FEES = "Fines or Fees Only Sentences"
    OTHER = "Other Sentences"
    UNKNOWN = "Unknown Sentences"

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
