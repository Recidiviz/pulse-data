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
