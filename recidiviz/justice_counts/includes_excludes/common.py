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
"""Includes/Excludes definitions that are shared across agencies """


import enum


# TODO(#18071)
class FelonyCasesIncludesExcludes(enum.Enum):
    FELONY = "Cases with a leading felony charge"
    MISDEMEANOR = "Cases with a leading misdemeanor or infraction charge"


class MisdemeanorCasesIncludesExcludes(enum.Enum):
    MISDEMEANOR = "Cases with a leading misdemeanor charge"
    FELONY = "Cases with a leading felony charge"
    INFRACTION = "Cases with a leading infraction charge"


class StateAppropriationIncludesExcludes(enum.Enum):
    FINALIZED = "Finalized state appropriations"
    PROPOSED = "Proposed state appropriations"
    PRELIMINARY = "Preliminary state appropriations"
    GRANTS = "Grants from state sources that are not budget appropriations approved by the legislature/governor"


class CountyOrMunicipalAppropriationIncludesExcludes(enum.Enum):
    FINALIZED = "Finalized county or municipal appropriations"
    PROPOSED = "Proposed county or municipal appropriations"
    PRELIMINARY = "Preliminary county or municipal appropriations"


class GrantsIncludesExcludes(enum.Enum):
    LOCAL = "Local grants"
    STATE = "State grants"
    FEDERAL = "Federal grants"
    PRIVATE = "Private or foundation grants"


class StaffIncludesExcludes(enum.Enum):
    FILLED = "Filled positions"
    VACANT = "Staff positions budgeted but currently vacant"
    FULL_TIME = "Full-time positions"
    PART_TIME = "Part-time positions"
    CONTRACTED = "Contracted positions"
    TEMPORARY = "Temporary positions"
    VOLUNTEER = "Volunteer positions"
    INTERN = "Intern positions"


class CasesDismissedIncludesExcludes(enum.Enum):
    FELONY = "Cases with a leading felony charge dismissed"
    MISDEMEANOR = "Cases with a leading misdemeanor charge dismissed"


class CasesResolvedByPleaIncludesExcludes(enum.Enum):
    FELONY = "Cases with a leading felony charge resolved by plea"
    MISDEMEANOR = "Cases with a leading misdemeanor charge resolved by plea"


class CasesResolvedAtTrialIncludesExcludes(enum.Enum):
    FELONY = "Cases with a leading felony charge resolved at trial"
    MISDEMEANOR = "Cases with a leading misdemeanor charge resolved at trial"
