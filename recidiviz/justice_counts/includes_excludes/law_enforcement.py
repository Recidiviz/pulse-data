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
"""Includes/Excludes definition for law enforcement agencies """

import enum

# Funding


class LawEnforcementFundingIncludesExcludes(enum.Enum):
    FISCAL_YEAR = "Funding for single fiscal year"
    BIENNIUM_FUNDING = "Biennium funding"
    MULTI_YEAR_APPROPRIATIONS = (
        "Multi-year appropriations that will not be fully spent this fiscal year"
    )
    STAFF_FUNDING = "Funding for agency staff"
    EQUIPMENT = "Funding for the purchase of law enforcement equipment"
    FACILITIES = "Funding for construction of law enforcement facilities (e.g., offices, temporary detention facilities, garages, etc.)"
    MAINTENANCE = (
        "Funding for the maintenance of law enforcement equipment and facilities"
    )
    JAIL_OPERATIONS = "Expenses for the operation of jails"
    SUPERVISION_SERVICES = (
        "Expenses for the operation of community supervision services"
    )
    JUVENILE_JAIL_OPERATIONS = "Expenses for the operation of juvenile jails"
    OTHER = "Funding for other purposes not captured by the listed categories"


class LawEnforcementStateAppropriationIncludesExcludes(enum.Enum):
    FINALIZED = "Finalized state appropriations"
    PROPOSED = "Proposed state appropriations"
    PRELIMINARY = "Preliminary state appropriations"


class LawEnforcementCountyOrMunicipalAppropriation(enum.Enum):
    FINALIZED = "Finalized county or municipal appropriations"
    PROPOSED = "Proposed county or municipal appropriations"
    PRELIMINARY = "Preliminary county or municipal appropriations"


class LawEnforcementAssetForfeitureIncludesExcludes(enum.Enum):
    OPERATING_BUDGET = "Assets seized and allocated into operating budget"
    JUDICIAL_DECISION = "Assets seized due to judicial decision"
    CRIMINAL_CONVICTION = "Assets seized due to criminal conviction"
    AUCTIONS = "Funding from forfeited asset auctions"


class LawEnforcementGrantsIncludesExcludes(enum.Enum):
    LOCAL = "Local grants"
    STATE = "State grants"
    FEDERAL = "Federal grants"
    PRIVATE = "Private or foundation grants"
