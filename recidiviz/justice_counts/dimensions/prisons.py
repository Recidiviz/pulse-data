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
    NEW_CONVICTION = "Readmissions for a New Conviction"
    RETURN_FROM_PROBATION = "Readmissions from Probation"
    RETURN_FROM_PAROLE = "Readmissions from Parole"
    OTHER_COMMUNITY_SUPERVISION = "Readmissions from Other Community Supervision"
    OTHER = "Other Readmissions"
    UNKNOWN = "Unknown Readmissions"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/prisons/readmissions/type"


class ExpenseType(DimensionBase, enum.Enum):
    PERSONNEL = "Personnel"
    TRAINING = "Training"
    FACILITIES_AND_EQUIPMENT = "Facilities and Equipment"
    HEALTH_CARE = "Health Care for People Who Are Incarcerated"
    CONTRACT_BEDS = "Contract Beds (Expenses)"
    OTHER = "Other Expenses"
    UNKNOWN = "Unknown Expenses"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/prisons/expenses/type"


class ReleaseType(DimensionBase, enum.Enum):
    TO_PROBATION_SUPERVISION = "Releases from Prison to Probation Supervision"
    TO_PAROLE_SUPERVISION = "Releases from Prison to Parole Supervision"
    TO_COMMUNITY_SUPERVISION = "Releases from Prison to Other Community Supervision That Is Not Probation or Parole"
    NO_CONTROL = "Releases from Prison to no Additional Correctional Control"
    DEATH = "Releases from Prison due to Death"
    OTHER = "Other Releases from Prison"
    UNKNOWN = "Unknown Releases from Prison"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/prisons/release/type"


class FundingType(DimensionBase, enum.Enum):
    STATE_APPROPRIATION = "State Appropriations"
    GRANTS = "Grants"
    COMMISSARY_AND_FEES = "Commissary and Fees"
    CONTRACT_BEDS = "Contract Beds (Funding)"
    OTHER = "Other Funding"
    UNKNOWN = "Unknown Funding"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/prisons/funding/type"


class StaffType(DimensionBase, enum.Enum):
    SECURITY = "Security Staff"
    MANAGEMENT_AND_OPERATIONS = "Management and Operations Staff"
    CLINICAL_AND_MEDICAL = "Clinical and Medical Staff"
    PROGRAMMATIC = "Programmatic Staff"
    OTHER = "Other Staff"
    UNKNOWN = "Unknown Staff"
    VACANT = "Vacant Positions (Any Staff Type)"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/prisons/staff/type"


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
