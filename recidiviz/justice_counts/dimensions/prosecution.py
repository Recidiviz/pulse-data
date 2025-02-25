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


class StaffType(DimensionBase, enum.Enum):
    LEGAL_STAFF = "Legal Staff"
    ADVOCATE_STAFF = "Victim-Witness Advocate Staff"
    ADMINISTRATIVE = "Administrative Staff"
    INVESTIGATIVE_STAFF = "Investigative Staff"
    OTHER = "Other Staff"
    UNKNOWN = "Unknown Staff"
    VACANT_POSITIONS = "Vacant Positions (Any Staff Type)"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/staff/prosecution_defense/type"


class CaseDeclinedSeverityType(DimensionBase, enum.Enum):
    FELONY = "Felony Cases Declined"
    MISDEMEANOR = "Misdemeanor Cases Declined"
    OTHER = "Other Cases Declined"
    UNKNOWN = "Unknown Cases Declined"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/declined/severity/prosecution/type"


class ReferredCaseSeverityType(DimensionBase, enum.Enum):
    FELONY = "Felony Cases Referred"
    MISDEMEANOR = "Misdemeanor Cases Referred"
    OTHER = "Other Cases Referred"
    UNKNOWN = "Unknown Cases Referred"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/severity/referred/prosecution/type"


class DivertedCaseSeverityType(DimensionBase, enum.Enum):
    FELONY = "Felony Cases Diverted/Deferred"
    MISDEMEANOR = "Misdemeanor Cases Diverted/Deferred"
    OTHER = "Other Cases Diverted/Deferred"
    UNKNOWN = "Unknown Cases Diverted/Deferred"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/severity/diverted/prosecution/type"


class FundingType(DimensionBase, enum.Enum):
    STATE_APPROPRIATIONS = "State Appropriations"
    COUNTY_OR_MUNICIPAL_APPROPRIATIONS = "County or Municipal Appropriations"
    GRANTS = "Grants"
    OTHER = "Other Funding"
    UNKNOWN = "Unknown Funding"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/funding/prosecution/type"


class ProsecutedCaseSeverityType(DimensionBase, enum.Enum):
    FELONY = "Felony Cases Prosecuted"
    MISDEMEANOR = "Misdemeanor Cases Prosecuted"
    OTHER = "Other Cases Prosecuted"
    UNKNOWN = "Unknown Cases Prosecuted"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/severity/prosecuted/prosecution/type"
