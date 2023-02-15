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
"""Includes/Excludes definition for defense agencies """

import enum

# Funding


class DefenseFundingIncludesExcludes(enum.Enum):
    FISCAL_YEAR = "Funding for single fiscal year"
    BIENNIUM_FUNDING = "Biennium funding appropriated during the time period"
    MULTI_YEAR_APPROPRIATIONS = (
        "Multi-year appropriations that are allocated during the time period"
    )
    FACILITY_AND_OPERATIONS = (
        "Funding for criminal defense provider facility operations and maintenance"
    )
    STAFF_AND_PERSONNEL = "Funding for criminal defense provider staff and personnel"
    SEPARATE_COUNSEL = "Funding for separate counsel in conflict cases"
    CONTRACTED_PERSONNEL = (
        "Funding for contracted personnel (e.g., expert witnesses, etc.)"
    )
    CASE_MANAGEMENT_SYSTEMS = "Funding for electronic case management systems"
    NON_CRIMINAL_CASES = "Funding for non-criminal case processing"


class FeesFundingIncludesExcludes(enum.Enum):
    FEES = "Fees charged to people who have been assigned criminal defense counsel due to indigence"


# Expenses


class DefenseExpensesIncludesExcludes(enum.Enum):
    SINGLE_YEAR = "Expenses for single fiscal year"
    BIENNIUM = "Biennium funding appropriated during the time period"
    MULTI_YEAR = (
        "Multi-year appropriations that are appropriated in during the time period"
    )
    OPERATIONS = (
        "Expenses for provider facility construction, operations, and maintenance"
    )
    CRIMINAL_DEFENSE_SERVICES = "Expenses for criminal defense services provided for clients through the provider, including transportation and bail services"
    CONTINUING_EDUCATION = "Expenses for continuing legal education"
    STAFF = "Expenses for criminal defense provider staff and personnel"
    CONTRACTED_PERSONNEL = (
        "Expenses for contracted personnel (e.g., expert witnesses, etc.)"
    )
    CASE_MANAGEMENT_SYSTEMS = "Expenses for electronic case management systems"
    NON_CRIMINAL_CASES = "Expenses for non-criminal case processing"


class DefensePersonnelExpensesIncludesExcludes(enum.Enum):
    SALARIES = "Salaries"
    BENEFITS = "Benefits"
    RETIREMENT = "Retirement contributions"
    INDIVIDUALS_CONTRACTED = (
        "Contracts for individuals doing work related to criminal public defense"
    )
    COMPANIES_CONTRACTED = "Companies or service providers contracted to support work related to criminal public defense"


class DefenseTrainingExpensesIncludesExcludes(enum.Enum):
    ANNUAL = "Annual training"
    LEGAL_EDUCATION = "Continuing education"
    SPECIALIZED_TRAINING = "Specialized training"
    FREE_COURSES = (
        "Courses or programs offered at no cost to individuals or the provider"
    )


# Staff
class DefenseLegalStaffIncludesExcludes(enum.Enum):
    LINE_ATTORNEYS = "Line attorneys"
    SUPERVISORY = "Supervisory attorneys"
    PARALEGALS = "Paralegals"
    LEGAL_ASSISTANTS = "Legal assistants"
    CURRENTLY_VACANT = "Legal staff positions budgeted but currently vacant"


class DefenseAdministrativeStaffIncludesExcludes(enum.Enum):
    CLERICAL = "Clerical staff"
    ANALYTIC_STAFF = "Data or analytic staff"
    LEADERSHIP = "Administrative supervisory or leadership staff"
    CURRENTLY_VACANT = "Administrative staff positions budgeted but currently vacant"


class DefenseInvestigativeStaffIncludesExcludes(enum.Enum):
    INVESTIGATIVE = "Investigative staff"
    CURRENTLY_VACANT = "Investigative staff positions budgeted but currently vacant"


class DefenseVacantStaffIncludesExcludes(enum.Enum):
    LEGAL = "Vacant legal staff positions"
    ADMINISTRATIVE = "Vacant administrative staff positions"
    INVESTIGATIVE = "Vacant investigative staff positions"
    UNKNOWN = "Vacant staff positions of unknown type"
    FILLED = "Filled positions"


# Cases Disposed
class DefenseCasesDisposedIncludesExcludes(enum.Enum):
    DIVERTED = "Criminal cases diverted from traditional case processing"
    DISMISSED = "Cases dismissed"
    PLEA = "Cases resolved by plea"
    TRIAL = "Cases resolved by trial"
    INACTIVE = "Cases marked as inactive, but not closed"
    PENDING = "Pending cases"


# Cases Appointed Counsel


class DefenseCasesAppointedCounselIncludesExcludes(enum.Enum):
    NEW = "New criminal cases appointed to attorneys employed by the provider"
    INACTIVE = "Inactive cases reopened"
    TRANSFERRED = "	Criminal cases transferred to conflict counsel"


# Caseload


class DefenseCaseloadNumeratorIncludesExcludes(enum.Enum):
    OPEN = "Criminal cases open and active during the time period"
    ASSIGNED = "Criminal cases assigned to an attorney but inactive"
    NOT_ASSIGNED = "Criminal cases not yet assigned to an attorney"


class DefenseCaseloadDenominatorIncludesExcludes(enum.Enum):
    LINE_ATTORNEYS = "Line attorneys carrying a caseload"
    SUPERVISING_ATTORNEYS = "Supervising attorneys carrying a caseload"
    ON_LEAVE = "Staff on leave whose criminal caseload is being covered by a colleague"


class DefenseFelonyCaseloadNumeratorIncludesExcludes(enum.Enum):
    OPEN_CASES = "Felony cases open and active during the sharing period"
    ASSIGNED_CASES = "Felony cases assigned to an attorney but inactive"
    UNASSIGNED_CASES = "Felony cases not yet assigned to an attorney"


class DefenseFelonyCaseloadDenominatorIncludesExcludes(enum.Enum):
    LINE_ATTORNEYS = "Line attorneys carrying a felony-only caseload"
    SUPERVISING_ATTORNEYS = "Supervising attorneys carrying a felony-only caseload"
    MIXED = "Line or supervising attorneys carrying a mixed (felony and misdemeanor) caseload"


class DefenseMisdemeanorCaseloadNumeratorIncludesExcludes(enum.Enum):
    OPEN_CASES = "Misdemeanor cases open and active during the sharing period"
    ASSIGNED_CASES = "Misdemeanor cases assigned to an attorney but inactive"
    UNASSIGNED_CASES = "Misdemeanor cases not yet assigned to an attorney"


class DefenseMisdemeanorCaseloadDenominatorIncludesExcludes(enum.Enum):
    LINE_ATTORNEYS = "Line attorneys carrying a misdemeanor-only caseload"
    SUPERVISING_ATTORNEYS = "Supervising attorneys carrying a misdemeanor-only caseload"
    MIXED = "Line or supervising attorneys carrying a mixed (felony and misdemeanor) caseload"


class DefenseMixedCaseloadNumeratorIncludesExcludes(enum.Enum):
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


class DefenseMixedCaseloadDenominatorIncludesExcludes(enum.Enum):
    LINE_ATTORNEYS = "Line attorneys carrying a mixed caseload"
    SUPERVISING_ATTORNEYS = "Supervising attorneys carrying a mixed caseload"
    FELONY_ONLY = "Line or supervising attorneys carrying a felony-only caseload"
    MISDEMEANOR_ONLY = (
        "Line or supervising attorneys carrying a misdemeanor-only caseload"
    )


# Complaints
class DefenseComplaintsIncludesExcludes(enum.Enum):
    UPHELD = "Complaints in criminal cases that were upheld or substantiated during the time period"
    RESULT_IN_PUNISHMENT = "Complaints in criminal cases resulting in punishment (e.g., reprimand, disbarment) during the time period"
    UNSUBSTANTIATED = (
        "Complaints in criminal cases that were unsubstantiated during the time period"
    )
    PENDING = "Complaints in criminal cases pending resolution"
    INFORMAL = "Complaints in criminal cases submitted informally or not in writing"
    DUPLICATE = "Duplicate complaints in criminal cases"
