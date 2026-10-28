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
"""Custom enum parsers functions for US_CO. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_co_custom_enum_parsers.<function name>
"""
from typing import Optional

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodHousingUnitType,
)
from recidiviz.common.constants.state.state_staff_role_period import (
    StateStaffRoleSubtype,
    StateStaffRoleType,
)
from recidiviz.ingest.direct.regions.us_co.us_co_custom_parsers import description_code


def parse_housing_unit_type(
    raw_text: str,
) -> Optional[StateIncarcerationPeriodHousingUnitType]:
    """
    Maps |housing_unit_type|, to its corresponding StateIncarcerationPeriodHousingUnitType.
    For now, this maps everything to StateIncarcerationPeriodHousingUnitType.GENERAL, regardless of the raw text received
    """

    return StateIncarcerationPeriodHousingUnitType.GENERAL if raw_text else None


# WORKASSIGNMENT (PTYWRKASG) codes for DCC/parole supervision staff.
# A-prefix codes other than ATG are facility/health/admin staff → INTERNAL_UNKNOWN, as
# are DCC support roles that carry no caseload: C36 Treatment Care Coordinator, C5L
# Community ReEntry Specialist, C5M Employment Training Navigator, and C5N Benefits
# Acquisition Specialist.
_SUPERVISION_OFFICER_CODES = frozenset(
    {
        "ATG",  # Parole Case Manager
        "C22",  # Case Management-PARCOM
        "C49",  # Community Parole-PARCOM
        "C50",  # Community Parole-PVT PARCOM
        "C53",  # Release CM
        "C55",  # Community Parole Officer (CPO)
        "C5R",  # CPO Trainee (SSPT II)
    }
)
_SUPERVISION_OFFICER_SUPERVISOR_CODES = frozenset(
    {
        "C21",  # Parole Supervisor
        "C56",  # Community Parole Supervisor
        "C57",  # Comm Parole Team Ldr
        "C5E",  # Community Referral Supervisor
        "C5F",  # Pre-Release Supervisor
        "C5K",  # Community ReEntry Supervisor
        "C60",  # Local Assigning Supervisor
    }
)
_SUPERVISION_DISTRICT_MANAGER_CODES = frozenset(
    {
        "C14",  # Management-PARCOM
        "C54",  # Comm Parole Mgr
        "C5G",  # Pre-Release Manager
        "C5J",  # Community ReEntry Manager
    }
)
_SUPERVISION_REGIONAL_MANAGER_CODES = frozenset(
    {
        "C61",  # Statewide Assigning Supervisor
    }
)
_SUPERVISION_STATE_LEADERSHIP_CODES = frozenset(
    {
        "C0P",  # Deputy Director
        "C58",  # Assistant Director
        "C59",  # Director
        "C5A",  # Associate Director
    }
)
_ALL_SUPERVISION_CODES = (
    _SUPERVISION_OFFICER_CODES
    | _SUPERVISION_OFFICER_SUPERVISOR_CODES
    | _SUPERVISION_DISTRICT_MANAGER_CODES
    | _SUPERVISION_REGIONAL_MANAGER_CODES
    | _SUPERVISION_STATE_LEADERSHIP_CODES
)


def parse_staff_role_type(
    raw_text: str,
) -> StateStaffRoleType:
    """Maps WORKASSIGNMENT ('Description [CODE]' formatted) to StateStaffRoleType."""
    if description_code(raw_text) in _ALL_SUPERVISION_CODES:
        return StateStaffRoleType.SUPERVISION_OFFICER
    return StateStaffRoleType.INTERNAL_UNKNOWN


def parse_staff_role_subtype(
    raw_text: str,
) -> StateStaffRoleSubtype:
    """Maps WORKASSIGNMENT ('Description [CODE]' formatted) to StateStaffRoleSubtype.

    Work assignments outside DCC/parole (facility, health, and administrative staff)
    have no supervision subtype and map to INTERNAL_UNKNOWN, matching the role type
    they get from parse_staff_role_type. Returning None here would instead raise an
    EnumParsingError for every one of those role periods.
    """
    code = description_code(raw_text)
    if code in _SUPERVISION_STATE_LEADERSHIP_CODES:
        return StateStaffRoleSubtype.SUPERVISION_STATE_LEADERSHIP
    if code in _SUPERVISION_REGIONAL_MANAGER_CODES:
        return StateStaffRoleSubtype.SUPERVISION_REGIONAL_MANAGER
    if code in _SUPERVISION_DISTRICT_MANAGER_CODES:
        return StateStaffRoleSubtype.SUPERVISION_DISTRICT_MANAGER
    if code in _SUPERVISION_OFFICER_SUPERVISOR_CODES:
        return StateStaffRoleSubtype.SUPERVISION_OFFICER_SUPERVISOR
    if code in _SUPERVISION_OFFICER_CODES:
        return StateStaffRoleSubtype.SUPERVISION_OFFICER
    return StateStaffRoleSubtype.INTERNAL_UNKNOWN
