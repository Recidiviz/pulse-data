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
