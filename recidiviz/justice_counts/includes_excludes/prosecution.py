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
class ProsecutionFundingTimeframeIncludesExcludes(enum.Enum):
    FISCAL_YEAR = "Funding for single fiscal year"
    BIENNIUM_FUNDING = "Biennium funding appropriated during the time period"
    MULTI_YEAR_APPROPRIATIONS = (
        "Multi-year appropriations that are allocated during the time period"
    )


class ProsecutionFundingPurposeIncludesExcludes(enum.Enum):
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
    PROPOSED = "Proposed county or municipal appropriations"
    PRELIMINARY = "Preliminary county or municipal appropriations"


class ProsecutionFundingGrantsIncludesExcludes(enum.Enum):
    LOCAL = "Local grants"
    STATE = "State grants"
    FEDERAL = "Federal grants"
    PRIVATE_OR_FOUNDATION = "Private or foundation grants"


# Expenses
class ProsecutionExpensesTimeframeIncludesExcludes(enum.Enum):
    FISCAL_YEAR = "Expenses for single fiscal year"
    BIENNIUM_FUNDING = "Biennium funding appropriated during the time period"
    MULTI_YEAR_APPROPRIATIONS = (
        "Multi-year appropriations that are allocated during the time period"
    )


class ProsecutionExpensesPurposeIncludesExcludes(enum.Enum):
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
    CONTRACTS = "Contracts for individuals doing work related to prosecution"
    DEFENSE = "Companies or service providers contracted to support work related to prosecution"


class ProsecutionTrainingExpensesIncludesExcludes(enum.Enum):
    ANNUAL = "Annual training"
    LEGAL_EDUCATION = "Continuing legal education (CLE)"
    CONTINUING_EDUCATION = "Continuing education – other (not CLE eligible)"
    TRAINING_ACADEMY = "Training academy"
    SPECIALIZED_TRAINING = "Specialized training"
    EXTERNAL_TRAINING = "External training or professional development opportunities (conferences, classes, etc.)"
    FREE_PROGRAMS = "Courses or programs offered at no cost to individuals or the prosecutor's office"


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


# Cases Declined


class ProsecutionCasesDeclinedIncludesExcludes(enum.Enum):
    LACK_OF_EVIDENCE = (
        "The number of new cases declined by the office for lack of evidence"
    )
    LACK_OF_WITNESSES = (
        "The number of new cases declined by the office for lack of witness cooperation"
    )
    LACK_OF_RESOURCES = (
        "The number of new cases declined by the office for lack of resources"
    )
    INTERNAL_TRANSFER = "Cases transferred internally"


# Cases Prosecuted
class ProsecutionCasesProsecutedIncludesExcludes(enum.Enum):
    CASES_CONVICTED = (
        "The number of cases prosecuted by the office that resulted in a conviction"
    )
    NOT_GUILTY = "The number of cases prosecuted by the office that resulted in a not guilty verdict"
    MISTRIAL = (
        "The number of cases prosecuted by the office that resulted in a mistrial"
    )
    UNASSIGNED = "Cases accepted for prosecution but not assigned to an attorney"


# Caseload


class ProsecutionCaseloadDenominatorIncludesExcludes(enum.Enum):
    LINE_ATTORNEYS = "Line attorneys carrying a caseload"
    SUPERVISING_ATTORNEYS = "Supervising attorneys carrying a caseload"


class ProsecutionFelonyCaseloadDenominatorIncludesExcludes(enum.Enum):
    LINE_ATTORNEYS = "Line attorneys carrying a felony-only caseload"
    SUPERVISING_ATTORNEYS = "Supervising attorneys carrying a felony-only caseload"


class ProsecutionMisdemeanorCaseloadDenominatorIncludesExcludes(enum.Enum):
    LINE_ATTORNEYS = "Line attorneys carrying a misdemeanor-only caseload"
    SUPERVISING_ATTORNEYS = "Supervising attorneys carrying a misdemeanor-only caseload"


class ProsecutionMixedCaseloadDenominatorIncludesExcludes(enum.Enum):
    LINE_ATTORNEYS = "Line attorneys carrying a mixed caseload"
    SUPERVISING_ATTORNEYS = "Supervising attorneys carrying a mixed caseload"


# Cases Diverted


class ProsecutionCasesDivertedOrDeferredIncludesExcludes(enum.Enum):
    COMPLETION = "Cases deferred for completion of treatment"
    RESTITUTION = "Cases deferred for restitution payment"
    COMMUNITY_SERVICE = (
        "Cases deferred for completion of community service or schooling"
    )
    SPECIALTY_COURT = "Cases diverted to a specialty court"
    MEDIATION = "Cases diverted to mediation or alternative dispute resolution"
    RETAINED = "Cases diverted or deferred and retained as open cases by the office"


# Cases Disposed
class ProsecutionCasesDisposedIncludesExcludes(enum.Enum):
    DIVERTED = "Criminal cases diverted from traditional case processing"
    DISMISSED = "Cases dismissed"
    PLEA = "Cases resolved by plea"
    TRIAL = "Cases resolved by trial"
    INACTIVE = "Cases marked as inactive, but not closed"
    PENDING = "Pending cases"


# Violations Filed Resulting in Discipline


class ProsecutionViolationsIncludesExcludes(enum.Enum):
    BRADY = "Formal Brady violations"
    DISCOVERY = "Formal discovery violations"
    BIASED_JURY = "Formal violations for biased jury selection"
    INEFFECTIVE_COUNSEL = "Formal violations for ineffective counsel"
    CLIENT_CONFIDENTIALITY = "Formal violations for violating client confidentiality"
    FINANCIAL_CONFLICT_OF_INTEREST = (
        "Formal violations for conflict of interest – financial"
    )
    NON_FINANCIAL_CONFLICT_OF_INTEREST = (
        "Formal violations for conflict of interest – non-financial"
    )
    DISCRIMINATION = "Formal violations for discrimination"
    HARASSMENT = "Formal violations for harassment"
    INFORMAL = "Violations submitted informally or not in writing"
    NO_DISCIPLINARY_ACTION = "Violations not resulting in formal disciplinary action"
    PENDING = "Violations pending investigation and review"
    DUPLICATE = "Duplicate violations filed"
