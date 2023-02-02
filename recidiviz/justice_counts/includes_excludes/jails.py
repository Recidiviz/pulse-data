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
