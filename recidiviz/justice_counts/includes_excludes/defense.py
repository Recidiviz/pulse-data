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
