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
"""Includes/Excludes definition for court agencies """

import enum

# Judges and Staff


class JudgesIncludesExcludes(enum.Enum):
    WITH_CRIMINAL_CASE = "Judges with a criminal case docket"
    WITHOUT_CRIMINAL_CASE = "Judges without a criminal case docket"


class LegalStaffIncludesExcludes(enum.Enum):
    CLERKS = "Law clerks"
    ATTORNEYS = "Staff attorneys"
    PARALEGALS = "Paralegals"
    JUDGES = "Judges"


class SecurityStaffIncludesExcludes(enum.Enum):
    BAILIFFS = "Bailiffs"
    POLICE_OFFICERS = "Court police officers"


class SupportOrAdministrativeStaffIncludesExcludes(enum.Enum):
    CLERKS = "Clerks"
    ANALYTICS = "Data, research, analytics staff"
    IT = "Information technology staff"
    BUDGETARY = "Budgetary staff"
    HR = "Human resources staff"
    RECORDERS = "Court reporters or recorders"


class VictimAdvocateStaffIncludesExcludes(enum.Enum):
    LEGAL = "Victim advocate staff in legal roles"
    NON_LEGAL = "Victim advocate staff in non-legal roles (e.g., witness support services, victim advocacy case managers, etc.)"
    NOT_FULL_TIME = "Staff trained in victim advocacy support who do not perform these roles full-time"


class VacantPositionsIncludesExcludes(enum.Enum):
    JUDGE = "Vacant judge positions"
    NON_JUDICIAL = "Vacant non-judicial legal staff positions"
    SECURITY = "Vacant security staff positions"
    ADMINISTRATIVE = "Vacant support or administrative staff positions"
    ADVOCATE = "Vacant victim advocate staff positions"
    UNKNOWN = "Vacant staff positions of unknown type"
    FILLED = "Filled positions"


# Criminal Case Filings
class CriminalCaseFilingsIncludesExcludes(enum.Enum):
    AUTHORIZED_AGENCY = "New cases filed by any authorized agency (e.g., prosecuting authority, law enforcement agency, etc.)"
    NEW_CHARGES = (
        "Cases filed for new criminal charges for people on community supervision"
    )
    TRANSFERRED_EXTERNAL = (
        "Cases transferred from another jurisdiction for new prosecution"
    )
    VIOLATIONS = "Violations of an existing supervision case"
    REVOCATIONS = "Revocations of an existing supervision case"
    REOPENED = "Inactive cases reopened"
    TRANSFERRED_INTERNAL = "Cases transferred internally"


class FelonyCriminalCaseFilingsIncludesExcludes(enum.Enum):
    FELONY_CHARGE = "Cases with a leading felony charge"


class MisdemeanorOrInfractionCriminalCaseFilingsIncludesExcludes(enum.Enum):
    MISDEMEANOR_CHARGE = "Cases with a leading misdemeanor charge"
    INFRACTION_CHARGE = "Cases with a leading infraction charge"
