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


class PopulationType(DimensionBase, enum.Enum):
    PRETRIAL = "Pretrial"
    SENTENCED = "Sentenced"
    TRANSFER_OR_HOLD = "Transfer or Hold"
    UNKNOWN = "Unknown"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/population/type"


class PreAdjudicationReleaseType(DimensionBase, enum.Enum):
    AWAITING_TRIAL = "Pre-adjudication Releases to Own Recognizance Awaiting Trial"
    BAIL = "Pre-adjudication Releases to Monetary Bail"
    DEATH = "Pre-adjudication Releases Due to Death"
    AWOL = "Pre-adjudication Releases Due to Escape or AWOL Status"
    OTHER = "Other Pre-adjudication Releases"
    UNKNOWN = "Unknown Pre-adjudication Releases"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/pre_adjudication/release/type"

    @classmethod
    def human_readable_name(cls) -> str:
        return "Pre-adjudication Release Type"


class PostAdjudicationReleaseType(DimensionBase, enum.Enum):
    PROBATION = "Post-adjudication Releases to Probation Supervision"
    PAROLE = "Post-adjudication Releases to Parole Supervision"
    COMMUNITY_SUPERVISION = "Post-adjudication Releases to Other Community Supervision That Is Not Probation or Parole"
    NO_ADDITIONAL_CONTROL = (
        "Post-adjudication Releases with No Additional Correctional Control"
    )
    DEATH = "Post-adjudication Releases Due to Death"
    AWOL = "Post-adjudication Releases Due to Escape or AWOL Status"
    OTHER = "Other Post-adjudication Releases"
    UNKNOWN = "Unknown Post-adjudication Releases"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/post_adjudication/release/type"

    @classmethod
    def human_readable_name(cls) -> str:
        return "Post-adjudication Release Type"


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


class BehavioralHealthNeedType(DimensionBase, enum.Enum):
    MENTAL_HEALTH = "Mental Health Needs"
    SUBSTANCE_USE = "Substance Use Needs"
    CO_OCCURRING_DISORDERS = "Co-Occurring Substance Use and Mental Health Needs"
    OTHER = "Other Behavioral Health Needs"
    UNKNOWN = "Unknown Behavioral Health Needs"
    NONE = "No Behavioral Health Needs"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/jails/behavioral_health/type"
