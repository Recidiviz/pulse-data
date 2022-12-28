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
"""Includes/Excludes definition for supervision agencies """

from enum import Enum


# Staff
class SupervisionStaffIncludesExcludes(Enum):
    FILLED = "Filled positions"
    VACANT = "Staff positions budgeted but currently vacant"
    FULL_TIME = "Full-time positions"
    PART_TIME = "Part-time positions"
    CONTRACTED = "Contracted positions"
    TEMPORARY = "Temporary positions"
    VOLUNTEER = "Volunteer positions"
    INTERN = "Intern positions"


class SupervisionStaffDimIncludesExcludes(Enum):
    OFFICERS = "Supervision officers (with caseloads)"
    SUPERVISORS = "Supervision supervisors (with caseloads)"
    VACANT = "Any supervision staff positions budgeted but currently vacant"


class SupervisionManagementOperationsStaffIncludesExcludes(Enum):
    MANAGEMENT = "Supervision agency management (i.e., district managers, regional managers who do not carry caseloads as a primary job function)"
    CLERICAL_OR_ADMIN = "Clerical or administrative staff"
    MAINTENANCE = "Maintenance staff"
    RESEARCH = "Research staff"
    VACANT = "Management and operations staff positions budgeted but currently vacant"


class SupervisionClinicalMedicalStaffIncludesExcludes(Enum):
    MEDICAL_DOCTORS = "Medical doctors"
    NURSES = "Nurses"
    DENTISTS = "Dentists"
    CLINICIANS = "Clinicians (e.g., substance use treatment specialists)"
    THERAPISTS = "Therapists (e.g., mental health counselors)"
    PSYCHIATRISTS = "Psychiatrists"
    VACANT = "Clinical or medical staff positions budgeted but currently vacant"


class SupervisionProgrammaticStaffIncludesExcludes(Enum):
    VOCATIONAL = "Vocational staff"
    EDUCATIONAL = "Educational staff"
    THERAPUTIC_AND_SUPPORT = "Therapeutic and support program staff"
    RELIGIOUS = "Religious or cultural program staff"
    VOLUNTEER = "Programmatic staff volunteer positions"
    VACANT = "Programmatic staff positions budgeted but currently vacant"


class SupervisionVacantStaffIncludesExcludes(Enum):
    VACANT_SUPERVISION = "Vacant supervision staff positions"
    VACANT_MANAGEMENT_AND_OPS = "Vacant management and operations"
    VACANT_CLINICAL_OR_MEDICAL = "Vacant clinical or medical staff positions"
    VACANT_PROGRAMMATIC = "Vacant programmatic staff positions"
    VACANT_UNKNOWN = "Vacant staff positions of unknown type"
    FILLED = "Filled positions"
