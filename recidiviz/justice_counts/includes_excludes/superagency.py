# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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

"""Includes/Excludes definition for superagencies"""

import enum


class SuperagencyFundingTimeframeIncludesExcludes(enum.Enum):
    FISCAL_YEAR = "Funding for single fiscal year"
    BIENNIUM_FUNDING = "Biennium funding appropriated during the time period"
    MULTI_YEAR_APPROPRIATIONS = (
        "Multi-year appropriations that are appropriated during the time period"
    )


class SuperagencyFundingPurposeIncludesExcludes(enum.Enum):
    FACILITY_AND_OPERATIONS = "Funding for agency operations and maintenance"
    STAFF_AND_PERSONNEL = "Funding for agency staff"


class SuperagencyExpensesTimeframeIncludesExcludes(enum.Enum):
    SINGLE_YEAR = "Expenses for single fiscal year"
    BIENNIUM = "Biennium expenses allocated during the time period"
    MULTI_YEAR = "Multi-year expenses that are allocated during the time period"


class SuperagencyExpensesTypeIncludesExcludes(enum.Enum):
    OPERATIONS = "Expenses for agency operations and maintenance"
    STAFF = "Expenses for agency staff"


class SuperagencyPersonnelExpensesIncludesExcludes(enum.Enum):
    SALARIES = "Salaries"
    BENEFITS = "Benefits"
    RETIREMENT = "Retirement contributions"
    COMPANIES_CONTRACTED = "Companies or service providers contracted"


class SuperagencyTrainingExpensesIncludesExcludes(enum.Enum):
    ANNUAL = "Annual training"
    CONTINUING_EDUCATION = "Continuing education"
    SPECIALIZED = "Specialized training"
    EXTERNAL = "External training or professional development opportunities (e.g., conferences, classes, etc.)"
    FREE_COURSES = "Courses or programs offered at no cost to individuals"


class SuperagencyFacilitiesEquipmentExpensesIncludesExcludes(enum.Enum):
    OPERATIONS = "Facility operations"
    MAINTENANCE = "Facility maintenance"
    RENOVATION = "Facility renovation"
    CONSTRUCTION = "Facility construction"
    EQUIPMENT = "Equipment (e.g., computers, communication, and information technology infrastructure)"


class SuperagencyFilledVacantStaffTypeIncludesExcludes(enum.Enum):
    FILLED = "Filled positions"
    VACANT = "Vacant positions"
