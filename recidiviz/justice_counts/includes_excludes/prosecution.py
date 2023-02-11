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
"""Includes/Excludes definition for prosecution agencies """

import enum


# Funding
class ProsecutionFundingIncludesExcludes(enum.Enum):
    # TODO(#17577)
    FISCAL_YEAR = "Funding for single fiscal year"
    BIENNIUM_FUNDING = "Biennium funding appropriated during the time period"
    MULTI_YEAR_APPROPRIATIONS = (
        "Multi-year appropriations that are allocated during the time period"
    )
    OFFICE_OPERATIONS = "Funding for office operations and maintenance"
    SERVICES = (
        "Funding for services provided through the office, including victim assistance"
    )
    TREATMENT_AND_PROGRAMMING = "Funding for office-managed treatment and programming"
    CONVICTION_INTEGRITY_UNITS = "Funding for conviction integrity units"
    SPECIALTY_UNIT_OPS = "Funding for specialty unit operations"
    CASE_MANAGEMENT_SYSTEMS = "Funding for electronic case management systems"
    NON_CRIMINAL_CASE_PROCESSING = "Funding for non-criminal case processing"


class ProsecutionFundingStateAppropriationsIncludesExcludes(enum.Enum):
    FINALIZED = "Finalized state appropriations"
    PROPOSED = "Proposed state appropriations"
    PRELIMINARY = "Preliminary state appropriations"
    GRANTS = "Grants from state sources that are not budget appropriations approved by the legislature/governor"


class ProsecutionFundingCountyOrMunicipalAppropriationsIncludesExcludes(enum.Enum):
    FINALIZED = "Finalized county or municipal appropriations"
    PROPOSED = "Finalized county or municipal appropriations"
    PRELIMINARY = "Preliminary county or municipal appropriations"


class ProsecutionFundingGrantsIncludesExcludes(enum.Enum):
    LOCAL = "Local grants"
    STATE = "State grants"
    FEDERAL = "Federal grants"
    PRIVATE_OR_FOUNDATION = "Private or foundation grants"


# Expenses
# TODO(#17577) Implement multiple includes/excludes tables
class ProsecutionExpensesIncludesExcludes(enum.Enum):
    FISCAL_YEAR = "Expenses for single fiscal year"
    BIENNIUM_FUNDING = "Biennium funding appropriated during the time period"
    MULTI_YEAR_APPROPRIATIONS = (
        "Multi-year appropriations that are allocated during the time period"
    )
    OFFICE_OPERATIONS_AND_MAINTENANCE = "Expenses for office operations and maintenance"
    SERVICES_PROVIDED = (
        "Expenses for services provided through the office, including victim assistance"
    )
    OFFICE_MANAGED_TREATMENT_AND_PROGRAMMING = (
        "Expenses for office-managed treatment and programming"
    )
    CONVICTION_INTEGRITY_UNITS = "Expenses for conviction integrity units"
    SPECIALTY_UNIT = "Expenses for specialty unit operations"
    CASE_MANAGEMENT_SYSTEMS = "Expenses for electronic case management systems"
    NON_CRIMINAL = "Expenses for non-criminal case processing"


class ProsecutionPersonnelExpensesIncludesExcludes(enum.Enum):
    SALARIES = "Salaries"
    BENEFITS = "Benefits"
    RETIREMENT_CONTRIBUTIONS = "Retirement contributions"
    CONTRACTS = (
        "Contracts for individuals doing work related to criminal public defense"
    )
    DEFENSE = "Companies or service providers contracted to support work related to criminal public defense"


class ProsecutionTrainingExpensesIncludesExcludes(enum.Enum):
    ANNUAL = "Annual training"
    LEGAL_EDUCATION = "Continuing legal education (CLE)"
    CONTINUING_EDUCATION = "Continuing education – other (not CLE eligible)"
    TRAINING_ACADEMY = "Training academy"
    SPECIALIZED_TRAINING = "Specialized training"
    EXTERNAL_TRAINING = "External training or professional development opportunities (conferences, classes, etc.)"
    FREE_PROGRAMS = "Courses or programs offered at no cost to individuals or the prosecutor's office"


class ProsecutionFacilitiesAndEquipmentExpensesIncludesExcludes(enum.Enum):
    OPERATIONS = "Facility operations"
    MAINTENANCE = "Facility maintenance"
    RENOVATION = "Facility renovation"
    CONSTRUCTION = "Facility construction"
    TECHNOLOGY = "Equipment (e.g., computers, communication, and information technology infrastructure)"


# Staff
class ProsecutionStaffIncludesExcludes(enum.Enum):
    FILLED = "Filled positions"
    VACANT = "Positions budgeted but currently vacant"
    FULL_TIME = "Full-time positions"
    PART_TIME = "Part-time positions"
    CONTRACTED = "Contracted positions"
    TEMPORARY = "Temporary positions"
    VOLUNTEER = "Volunteer positions"
    INTERN = "Intern positions"


class ProsecutionLegalStaffIncludesExcludes(enum.Enum):
    ALL_STAFF = "All legal staff responsible for a criminal caseload (all levels)"
    ATTORNEYS = "Attorneys"
    PARALEGALS = "Paralegals"
    LEGAL_ASSISTANTS = "Legal assistants"


class ProsecutionAdvocateStaffIncludesExcludes(enum.Enum):
    ADVOCATE_STAFF = "Victim-witness advocate staff"
    SUPERVISORS = "Victim-witness advocate supervisors"


class ProsecutionAdministrativeStaffIncludesExcludes(enum.Enum):
    ALL_STAFF = "All administrative staff"
    DATA_STAFF = "Data and analytics staff"
    LEADERSHIP = "Office managers and leadership staff without active caseloads"
    INVESTIGATIVE_STAFF = "Investigative staff"


class ProsecutionInvestigativeStaffIncludesExcludes(enum.Enum):
    ALL_STAFF = "All investigative staff"


class ProsecutionVacantStaffIncludesExcludes(enum.Enum):
    LEGAL = "Vacant legal staff positions"
    ADVOCATE_STAFF = "Vacant victim-witness advocate staff positions"
    ADMINISTRATIVE_STAFF = "Vacant administrative staff positions"
    INVESTIGATIVE_STAFF = "Vacant investigative staff positions"
    FILLED = "Filled positions"


# Caseload
class ProsecutionCaseloadIncludesExcludes(enum.Enum):
    # TODO(#17577)
    OPEN_CASES = "Criminal cases open and active during the sharing period"
    ASSIGNED_CASES = "Criminal cases assigned to an attorney but inactive"
    UNASSIGNED_CASES = "Criminal cases not yet assigned to an attorney"
    LINE_ATTORNEYS = "Line attorneys carrying a caseload"
    SUPERVISING_ATTORNEYS = "Supervising attorneys carrying a caseload"


class ProsecutionFelonyCaseloadIncludesExcludes(enum.Enum):
    # TODO(#17577)
    OPEN_CASES = "Felony cases open and active during the sharing period"
    ASSIGNED_CASES = "Felony cases assigned to an attorney but inactive"
    UNASSIGNED_CASES = "Felony cases not yet assigned to an attorney"
    LINE_ATTORNEYS = "Line attorneys carrying a felony-only caseload"
    SUPERVISING_ATTORNEYS = "Supervising attorneys carrying a felony-only caseload"


class ProsecutionMisdemeanorCaseloadIncludesExcludes(enum.Enum):
    # TODO(#17577)
    OPEN_CASES = "Misdemeanor cases open and active during the sharing period"
    ASSIGNED_CASES = "Misdemeanor cases assigned to an attorney but inactive"
    UNASSIGNED_CASES = "Misdemeanor cases not yet assigned to an attorney"
    LINE_ATTORNEYS = "Line attorneys carrying a misdemeanor-only caseload"
    SUPERVISING_ATTORNEYS = "Supervising attorneys carrying a misdemeanor-only caseload"


class ProsecutionMixedCaseloadIncludesExcludes(enum.Enum):
    # TODO(#17577)
    OPEN_FELONY_CASES = "Felony cases open and active during the sharing period"
    ASSIGNED_FELONY_CASES = "Felony cases assigned to an attorney but inactive"
    OPEN_MISDEMEANOR_CASES = (
        "Misdemeanor cases open and active during the sharing period"
    )
    ASSIGNED_MISDEMEANOR_CASES = (
        "Misdemeanor cases assigned to an attorney but inactive"
    )
    UNASSIGNED_FELONY_CASES = "Felony cases not yet assigned to an attorney"
    UNASSIGNED_MISDEMEANOR_CASES = "Misdemeanor cases not yet assigned to an attorney"
    LINE_ATTORNEYS = "Line attorneys carrying a mixed caseload"
    SUPERVISING_ATTORNEYS = "Supervising attorneys carrying a mixed caseload"


# Cases Referred


class ProsecutionCasesReferredIncludesExcludes(enum.Enum):
    LAW_ENFORCEMENT = "New cases referred by any law enforcement office"
    SUPERVISION = "New cases referred by any supervision agency"
    ANOTHER_JURISDICTION = (
        "New cases transferred from another jurisdiction for prosecution"
    )
    PROSECUTING_OFFICE = "New cases initiated by the prosecuting office itself"
    REOPENED = "Inactive cases reopened"
    INTERNAL_TRANSFER = "Cases transferred internally"
