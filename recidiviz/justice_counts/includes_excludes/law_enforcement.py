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
"""Includes/Excludes definition for law enforcement agencies """

import enum


# Expenses
class LawEnforcementExpensesIncludesExcludes(enum.Enum):
    FISCAL_YEAR = "Expenses for single fiscal year"
    BIENNIUM_FUNDING = "Biennium expenses"
    MULTI_YEAR_EXPENSES = (
        "Multi-year expenses that will not be fully spent this fiscal year"
    )
    STAFF_FUNDING = "Expenses for agency staff"
    EQUIPMENT = "Expenses for the purchase of law enforcement equipment"
    CONSTRUCTION = "Expenses for construction of law enforcement facilities (e.g., offices, temporary detention facilities, garages, etc.)"
    MAINTENANCE = (
        "Expenses for the maintenance of law enforcement equipment and facilities"
    )
    OTHER = "Expenses for other purposes not captured by the listed categories"
    JAILS = "Expenses for the operation of jails"
    SUPERVISION = "Expenses for the operation of community supervision services"


class LawEnforcementPersonnelIncludesExcludes(enum.Enum):
    SALARIES = "Salaries"
    BENEFITS = "Benefits"
    RETIREMENT = "Retirement contributions"
    INDIVIDUAL_CONTRACTORS = (
        "Costs of individuals contracted to work for the law enforcement agency"
    )
    COMPANY_CONTRACTS = (
        "Costs of companies contracted to work for the law enforcement agency"
    )


class LawEnforcementTrainingIncludesExcludes(enum.Enum):
    ANNUAL = "Annual training"
    ACADEMY = "Training academy"
    SPECIALIZED = "Specialized training"
    CONTINUING_EDUCATION = "Continuing education"
    EXTERNAL = "External training or professional development opportunities (conferences, classes, etc.)"
    FREE = "Courses or programs offered at no cost to individuals or the department"


class LawEnforcementFacilitiesIncludesExcludes(enum.Enum):
    OPERATIONS = "Law enforcement facility operations"
    MAINTENANCE = "Law enforcement facility maintenance"
    RENOVATION = "Law enforcement facility renovation"
    CONSTRUCTION = "Law enforcement facility construction"
    VEHICLES = "Vehicles"
    UNIFORMS = "Uniforms"
    EQUIPMENT = (
        "Equipment (e.g., offices, temporary detention facilities, garages, etc.)"
    )
    WEAPONS = "Weapons"


# Funding


class LawEnforcementFundingIncludesExcludes(enum.Enum):
    FISCAL_YEAR = "Funding for single fiscal year"
    BIENNIUM_FUNDING = "Biennium funding"
    MULTI_YEAR_APPROPRIATIONS = (
        "Multi-year appropriations that will not be fully spent this fiscal year"
    )
    STAFF_FUNDING = "Funding for agency staff"
    EQUIPMENT = "Funding for the purchase of law enforcement equipment"
    FACILITIES = "Funding for construction of law enforcement facilities (e.g., offices, temporary detention facilities, garages, etc.)"
    MAINTENANCE = (
        "Funding for the maintenance of law enforcement equipment and facilities"
    )
    JAIL_OPERATIONS = "Expenses for the operation of jails"
    SUPERVISION_SERVICES = (
        "Expenses for the operation of community supervision services"
    )
    JUVENILE_JAIL_OPERATIONS = "Expenses for the operation of juvenile jails"
    OTHER = "Funding for other purposes not captured by the listed categories"


class LawEnforcementStateAppropriationIncludesExcludes(enum.Enum):
    FINALIZED = "Finalized state appropriations"
    PROPOSED = "Proposed state appropriations"
    PRELIMINARY = "Preliminary state appropriations"


class LawEnforcementCountyOrMunicipalAppropriation(enum.Enum):
    FINALIZED = "Finalized county or municipal appropriations"
    PROPOSED = "Proposed county or municipal appropriations"
    PRELIMINARY = "Preliminary county or municipal appropriations"


class LawEnforcementAssetForfeitureIncludesExcludes(enum.Enum):
    OPERATING_BUDGET = "Assets seized and allocated into operating budget"
    JUDICIAL_DECISION = "Assets seized due to judicial decision"
    CRIMINAL_CONVICTION = "Assets seized due to criminal conviction"
    AUCTIONS = "Funding from forfeited asset auctions"


class LawEnforcementGrantsIncludesExcludes(enum.Enum):
    LOCAL = "Local grants"
    STATE = "State grants"
    FEDERAL = "Federal grants"
    PRIVATE = "Private or foundation grants"


# Staff
class LawEnforcementStaffIncludesExcludes(enum.Enum):
    FILLED_POSITIONS = "Filled positions funded within the agency's budget"
    BUDGETED = "Staff positions budgeted but currently vacant"
    FULL_TIME = "Full-time positions funded within the agency's budget"
    PART_TIME = "Part-time positions funded within the agency's budget"
    CONTRACTED = "Contracted positions funded within the agency's budget"
    TEMPORARY = "Temporary positions funded within the agency's budget"
    VOLUNTEER = "Volunteer positions"
    INTERN = "Intern positions"
    NOT_FUNDED = "Positions not funded within the agency’s budget"


class LawEnforcementPoliceOfficersIncludesExcludes(enum.Enum):
    UNIFORMED = "Uniformed police officers (all ranks)"
    DETECTIVES = "Detectives"
    SPECIALIZED_UNITS = "Police officers assigned to specialized units"
    SPECIAL_JURISDICTION = "Special jurisdiction police officers"
    SHERIFFS = "Sheriffs and deputy sheriffs"
    STATE = "State police officers"
    MENTAL_HEALTH = "Sworn/uniformed positions trained in mental health first aid or crisis intervention who do not perform these roles full time"
    VACANT = "Sworn/uniformed staff positions budgeted but currently vacant"
    CRISIS_INTERVENTION = (
        "Sworn/uniformed Crisis Intervention Team staff who perform this role full time"
    )
    VICTIM_ADVOCATE = (
        "Sworn/uniformed victim advocate staff who perform this role full time"
    )


class LawEnforcementCivilianStaffIncludesExcludes(enum.Enum):
    INVESTIGATORS = "Civilian investigators"
    ANALYSTS = "Civilian crime, intelligence, and research analysts"
    CRIME_PREVENTION = "Civilian crime prevention staff"
    COMMUNITY_OUTREACH = "Civilian community outreach staff"
    DISPATCHERS = "Dispatchers and call-takers"
    TRAFFIC = "Civilian parking and traffic enforcement"
    PUBLIC_INFORMATION = "Public information officers"
    RECORDS_MANAGEMENT = "Records management staff"
    ADMIN = "Clerical and administrative staff"
    IT = "Information technology staff"
    FORENSIC = "Forensic and crime lab technicians"
    EQUIPMENT_MANAGEMENT = "Equipment and fleet management staff"


class LawEnforcementMentalHealthStaffIncludesExcludes(enum.Enum):
    SWORN = "Sworn/uniformed Crisis Intervention Team staff"
    NON_SWORN = "Non-sworn/civilian Crisis Intervention Team staff"
    PRACTITIONERS = (
        "Mental health practitioners who collaborate with law enforcement officers"
    )
    PART_TIME = "Staff trained in mental health first aid or crisis intervention who do not perform these roles full time"


class LawEnforcementVictimAdvocateStaffIncludesExcludes(enum.Enum):
    SWORN = "Sworn/uniformed victim advocacy staff"
    NON_SWORN = "Non-sworn/civilian victim advocacy staff"
    COLLABORATORS = "Victim advocates who collaborate with law enforcement officers to provide support services to victims of crime"
    PART_TIME = "Staff trained in victim advocacy support who do not perform these roles full time"


class LawEnforcementVacantStaffIncludesExcludes(enum.Enum):
    SWORN = "Vacant sworn/uniformed police officer positions"
    CIVILIAN = "Vacant civilian staff positions"
    MENTAL_HEALTH = "Vacant mental health/Crisis Intervention Team staff positions"
    VICTIM_ADVOCATE = "Vacant victim advocate staff positions"
    FILLED = "Filled positions"
