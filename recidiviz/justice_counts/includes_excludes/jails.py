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
"""Includes/Excludes definition for jails agencies"""

from enum import Enum


# Funding
# TODO(#17577) implement multiple includes/excludes tables
class FundingIncludesExcludes(Enum):
    SINGLE_YEAR = "Funding for single fiscal year"
    BIENNIUM = "Biennium funding appropriated during the time period"
    MULTI_YEAR = (
        "Multi-year appropriations that are appropriated in during the time period"
    )
    FACILITY_OPERATIONS = "Funding for jail facility operations and maintenance"
    OTHER_FACILITIES = "Funding for operations and maintenance of other facilities within the agency’s jurisdiction (e.g., transitional housing facilities, treatment facilities, etc.)"
    CONSTRUCTION = "Funding for construction or rental of new jail facilities"
    PROGRAMMING = "Funding for agency-run or contracted treatment and programming"
    HEALTH_CARE = "Funding for health care for people in jail facilities"
    FACILITY_STAFF = "Funding for jail facility staff"
    SUPPORT_STAFF = "Funding for central administrative and support staff"
    BEDS_CONTRACTED = (
        "Funding for the operation of private jail beds contracted by the agency"
    )
    CASE_MANAGEMENT_SYSTEMS = "Funding for electronic case management systems"
    OPERATIONS_MAINTENANCE = "Funding for prison facility operations and maintenance"
    JUVENILE = "Funding for juvenile jail facilities"
    NON_JAIL_ACTIVITIES = "Funding for non-jail activities such as pre- or post-adjudication community supervision"
    LAW_ENFORCEMENT = "Funding for law enforcement functions"


class StateAppropriationIncludesExcludes(Enum):
    FINALIZED = "Finalized state appropriations"
    PROPOSED = "Proposed state appropriations"
    PRELIMINARY = "Preliminary state appropriations"
    GRANTS = "Grants from state sources that are not budget appropriations approved by the legislature/governor"


class CountyOrMunicipalAppropriationIncludesExcludes(Enum):
    FINALIZED = "Finalized county or municipal appropriations"
    PROPOSED = "Proposed county or municipal appropriations"
    PRELIMINARY = "Preliminary county or municipal appropriations"


class GrantsIncludesExcludes(Enum):
    LOCAL = "Local grants"
    STATE = "State grants"
    FEDERAL = "Federal grants"
    PRIVATE = "Private or foundation grants"


class CommissaryAndFeesIncludesExcludes(Enum):
    JAIL_COMMISSARIES = "Sales in jail commissaries"
    FEES_INCARCERATED = "Fees charged to people who are incarcerated"
    FEES_VISITORS = "Fees charged to visitors of people who are incarcerated"


class ContractBedsFundingIncludesExcludes(Enum):
    OTHER_COUNTY = "Funding collected from beds contracted by other county agencies"
    SUPERVISION = "Funding collected from beds contracted by supervision agencies"
    STATE = "Funding collected from beds contracted by state agencies"
    FEDERAL = "Funding collected from beds contracted by federal agencies"


# Expenses
# TODO(#17577) Implement multiple includes/excludes tables
class ExpensesIncludesExcludes(Enum):
    SINGLE_YEAR = "Expenses for single fiscal year"
    BIENNIUM = "Biennium funding appropriated during the time period"
    MULTI_YEAR = (
        "Multi-year appropriations that are appropriated in during the time period"
    )
    FACILITY_OPERATIONS = "Expenses for jail facility operations and maintenance"
    OTHER_FACILITIES = "Expenses for operations and maintenance of other facilities within the agency’s jurisdiction (e.g., transitional housing facilities, treatment facilities, etc.)"
    CONSTRUCTION = "Expenses for construction or rental of new jail facilities"
    PROGRAMMING = "Expenses for agency-run or contracted treatment and programming"
    HEALTH_CARE = "Expenses for health care for people in jail facilities"
    FACILITY_STAFF = "Expenses for jail facility staff"
    SUPPORT_STAFF = "Expenses for central administrative and support staff"
    BEDS_CONTRACTED = (
        "Expenses for the operation of private jail beds contracted by the agency"
    )
    CASE_MANAGEMENT_SYSTEMS = "Expenses for electronic case management systems"
    OPERATIONS_MAINTENANCE = "Expenses for prison facility operations and maintenance"
    JUVENILE = "Expenses for juvenile jail facilities"
    NON_JAIL_ACTIVITIES = "Expenses for non-jail activities such as pre- or post-adjudication community supervision"
    LAW_ENFORCEMENT = "Expenses for law enforcement functions"


class PersonnelIncludesExcludes(Enum):
    SALARIES = "Salaries"
    BENEFITS = "Benefits"
    RETIREMENT = "Retirement contributions"
    INDIVIDUALS_CONTRACTED = (
        "Costs for individuals contracted to work in or for the jail agency"
    )
    COMPANIES_CONTRACTED = (
        "Costs for companies contracted to work in or for the jail agency"
    )


class TrainingIncludesExcludes(Enum):
    ANNUAL = "Annual training"
    CONTINUING = "Continuing education"
    ACADEMY = "Training academy"
    SPECIALIZED = "Specialized training"
    EXTERNAL = "External training or professional development opportunities (conferences, classes, etc.)"
    NO_COST_PROGRAMS = (
        "Courses or programs offered at no cost to individuals or the department"
    )


class FacilitiesAndEquipmentIncludesExcludes(Enum):
    OPERATIONS = "Jail facility operations"
    MAINTENANCE = "Jail facility maintenance"
    RENOVATION = "Jail facility renovation"
    CONSTRUCTION = "Jail facility construction"
    EQUIPMENT = "Equipment (e.g., computers, communication, and information technology infrastructure)"


class HealthCareForPeopleWhoAreIncarceratedIncludesExcludes(Enum):
    FACILITY_OPERATION = (
        "Expenses related to the operation of jail facility infirmaries and hospitals"
    )
    SALARIES_PROVIDERS = (
        "Salaries and benefits for medical providers employed by the jail agency"
    )
    CONTRACT_PROVIDERS = "Contracts with providers of medical care"
    EXPENSES_PHYSICAL = "Expenses related to physical medical care"
    EXPENSES_MENTAL = "Expenses related to mental health care"
    TRANSPORT_COSTS = "Costs related to transporting people who are incarcerated to and from hospitals or other health care facilities"


class ContractBedsExpensesIncludesExcludes(Enum):
    OTHER_AGENCIES = "Expenses for beds contracted with other jail agencies"
    STATE = "Expenses for beds contracted with state agencies (i.e., in state prisons)"
    FEDERAL = (
        "Expenses for beds contracted with federal agencies (i.e., in federal prisons)"
    )
    PRIVATE = "Expenses for beds contracted with private prison companies"


# Staff
class StaffIncludesExcludes(Enum):
    FILLED = "Filled positions"
    VACANT = "Staff positions budgeted but currently vacant"
    FULL_TIME = "Full-time positions"
    PART_TIME = "Part-time positions"
    CONTRACTED = "Contracted positions"
    TEMPORARY = "Temporary positions"
    VOLUNTEER = "Volunteer positions"
    INTERN = "Intern positions"


class SecurityStaffIncludesExcludes(Enum):
    CORRECTIONAL_OFFICERS_ALL = "Correctional officers (all ranks)"
    CORRECTIONAL_OFFICERS_SUPERVISORS = "Correctional officer supervisors"
    VACANT = "Security staff budgeted but currently vacant"


class ManagementAndOperationsStaffIncludesExcludes(Enum):
    JAIL_MANAGEMENT = "Jail management (i.e., executive-level staff such as the warden, chiefs, superintendent, etc.)"
    CLERICAL = "Clerical or administrative staff"
    RESEARCH = "Research staff"
    MAINTENANCE = "Maintenance staff"
    VACANT = "Management and operations staff positions budgeted but currently vacant"


class ClinicalAndMedicalStaffIncludesExcludes(Enum):
    DOCTORS = "Medical doctors"
    NURSES = "Nurses"
    DENTISTS = "Dentists"
    CLINICIANS = "Clinicians (e.g., substance use treatment specialists)"
    THERAPISTS = "Therapists (e.g., mental health counselors)"
    PSYCHIATRISTS = "Psychiatrists"
    VACANT = "Clinical or medical staff positions budgeted but currently vacant"


class ProgrammaticStaffIncludesExcludes(Enum):
    VOCATIONAL = "Vocational staff"
    EDUCATIONAL = "Educational staff"
    SUPPORT_PROGRAM = "Therapeutic and support program staff"
    RELIGIOUS = "Religious or cultural program staff"
    VACANT = "Programmatic staff positions budgeted but currently vacant"


class VacantPositionsIncludesExcludes(Enum):
    SECURITY = "Vacant security staff positions"
    MANAGEMENT_AND_OPERATIONS = "Vacant management and operations staff positions"
    CLINICAL_AND_MEDICAL = "Vacant clinical and medical staff positions"
    PROGRAMMATIC = "Vacant programmatic staff positions"
    UNKNOWN = "Vacant staff positions of unknown type"
    FILLED = "Filled positions"
