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


class FundingType(DimensionBase, enum.Enum):
    STATE_APPROPRIATION = "State Appropriation"
    COUNTY_MUNICIPAL = "County or Municipal Appropriation"
    GRANTS = "Grants"
    COMMISSARY_FEES = "Commissary and Fees"
    CONTRACT_BEDS = "Contract Beds (Funding)"
    OTHER = "Other Funding"
    UNKNOWN = "Unknown Funding"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/funding/type"


class ExpenseType(DimensionBase, enum.Enum):
    PERSONNEL = "Personnel"
    TRAINING = "Training"
    FACILITIES = "Facilities and Equipment"
    HEALTH_CARE = "Health Care for People Who Are Incarcerated"
    CONTRACT_BEDS = "Contract Beds (Expenses)"
    OTHER = "Other Expenses"
    UNKNOWN = "Unknown Expenses"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/expense/type"


class ReadmissionType(DimensionBase, enum.Enum):
    NEW_ADMISSION = "New Admission"
    VIOLATION_OF_CONDITIONS = "Violation of Conditions"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/readmissions/type"


class PopulationType(DimensionBase, enum.Enum):
    PRETRIAL = "Pretrial"
    SENTENCED = "Sentenced"
    TRANSFER_OR_HOLD = "Transfer or Hold"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/population/type"


class ReleaseType(DimensionBase, enum.Enum):
    SENTENCE_COMPLETION = "Sentence Completion"
    PRETRIAL_RELEASE = "Pretrial Release"
    TRANSFER = "Transfer"
    UNAPPROVED_ABSENCE = "Unapproved Absence"
    COMPASSIONATE = "Compassionate"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/release/type"


class StaffType(DimensionBase, enum.Enum):
    SECURITY = "Security"
    SUPPORT = "Support"
    OTHER = "Other"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/staff/type"


class GrievancesUpheldType(DimensionBase, enum.Enum):
    LIVING_CONDITIONS = "Living Conditions"
    PERSONAL_SAFETY = "Personal Safety"
    DISCRIMINATION = "Discrimination, Racial Bias, or Religious Practices"
    ACCESS_TO_HEALTH_CARE = "Access to Health Care"
    LEGAL = "Legal"
    OTHER = "Other Grievance"
    UNKNOWN = "Unknown Grievance"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/grievances/type"
