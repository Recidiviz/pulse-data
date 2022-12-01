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


class PrisonsReadmissionType(DimensionBase, enum.Enum):
    NEW_CONVICTION = "New Conviction"
    RETURN_FROM_PROBATION = "Return from Probation"
    RETURN_FROM_PAROLE = "Return from Parole"
    OTHER_READMISSIONS = "Other Readmissions"
    UNKNOWN_READMISSIONS = "Unknown Post-Adjudication Readmission"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/prisons/readmissions/type"


class PrisonsExpenseType(DimensionBase, enum.Enum):
    PERSONNEL = "Personnel"
    TRAINING = "Training"
    FACILITIES_AND_EQUIPMENT = "Facilities and Equipment"
    HEALTH_CARE = "Health Care for People Who Are Incarcerated"
    CONTRACT_BEDS = "Contract Beds"
    UNKNOWN = "Unknown Expenses"
    OTHER = "Other Expenses"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/prisons/expenses/type"


class JailsReadmissionType(DimensionBase, enum.Enum):
    NEW_ADMISSION = "New Admission"
    VIOLATION_OF_CONDITIONS = "Violation of Conditions"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/readmissions/type"


class JailPopulationType(DimensionBase, enum.Enum):
    PRETRIAL = "Pretrial"
    SENTENCED = "Sentenced"
    TRANSFER_OR_HOLD = "Transfer or Hold"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/population/type"


class JailReleaseType(DimensionBase, enum.Enum):
    SENTENCE_COMPLETION = "Sentence Completion"
    PRETRIAL_RELEASE = "Pretrial Release"
    TRANSFER = "Transfer"
    UNAPPROVED_ABSENCE = "Unapproved Absence"
    COMPASSIONATE = "Compassionate"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/release/type"


class PrisonsReleaseType(DimensionBase, enum.Enum):
    SENTENCE_COMPLETION = "Sentence Completion"
    TO_PAROLE_SUPERVISION = "To Parole Supervision"
    TO_PROBATION_SUPERVISION = "To Probation Supervision"
    DEATH = "Death"
    TRANSFER = "Transfer"
    UNAPPROVED_ABSENCE = "Unapproved Absence"
    COMPASSIONATE_RELEASE = "Compassionate Release"
    UNKNOWN = "Unknown"
    OTHER = "Other"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/prisons/release/type"


class CorrectionalFacilityForceType(DimensionBase, enum.Enum):
    PHYSICAL = "Physical"
    RESTRAINT = "Restraint"
    VERBAL = "Verbal"
    WEAPON_INVOLVED = "Weapon"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/correctional_facility/force/type"


class PrisonsOffenseType(DimensionBase, enum.Enum):
    PERSON = "Person"
    PROPERTY = "Property"
    DRUG = "Drug"
    PUBLIC_ORDER = "Public Order"
    OTHER = "Other"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/prisons/offense/type"


class PrisonsFundingType(DimensionBase, enum.Enum):
    STATE_APPROPRIATION = "State Appropriation"
    GRANTS = "Grants"
    COMMISSARY_AND_FEES = "Commissary and Fees"
    CONTRACT_BEDS = "Contract Beds"
    OTHER = "Other Funding"
    UNKNOWN = "Unknown Funding"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/prisons/funding/type"


class PrisonsStaffType(DimensionBase, enum.Enum):
    SECURITY = "Security Staff"
    MANAGEMENT_AND_OPERATIONS = "Management and Operations Staff"
    CLINICAL_OR_MEDICAL = "Clinical or Medical Staff"
    PROGRAMMATIC = "Programmatic Staff"
    VACANT = "Vacant Positions (Any Staff Type)"
    OTHER = "Other Staff"
    UNKNOWN = "Unknown Staff"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/prisons/staff/type"


class JailsStaffType(DimensionBase, enum.Enum):
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
        return "metric/prisons/grievances/type"
