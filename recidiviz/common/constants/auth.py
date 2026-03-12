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
"""Constants needed for auth/roster management."""

from enum import Enum


class RosterPredefinedRoles(Enum):
    # Note: These are inspired by, but do not need to match exactly,
    # recidiviz.common.constants.state.state_staff_role_period.StateStaffRoleSubtype
    CPA_STAFF = "CPA_STAFF"
    # This role is meant for granting JII tablet app access in situations where either
    # the applicable primary role is unknown or there are no primary roles set up
    # in the state (i.e. the JII app was the first tool we launched in the state).
    # It isn't needed in states where JII app access is already tied to an existing role.
    FACILITIES_JII_APP_VIEWER = "FACILITIES_JII_APP_VIEWER"
    FACILITIES_LEADERSHIP = "FACILITIES_LEADERSHIP"
    FACILITIES_LINE_STAFF = "FACILITIES_LINE_STAFF"
    FACILITIES_MANAGER = "FACILITIES_MANAGER"
    FACILITIES_NON_PRIMARY_STAFF = "FACILITIES_NON_PRIMARY_STAFF"
    FACILITIES_SEGREGATION_STAFF = "FACILITIES_SEGREGATION_STAFF"
    INCARCERATION_REENTRY_LINE_STAFF = "INCARCERATION_REENTRY_LINE_STAFF"
    INCARCERATION_REENTRY_MANAGER = "INCARCERATION_REENTRY_MANAGER"
    INCARCERATION_REENTRY_NON_PRIMARY_STAFF = "INCARCERATION_REENTRY_NON_PRIMARY_STAFF"
    LEGISLATIVE_STAFF = "LEGISLATIVE_STAFF"
    PSI_STAFF = "PSI_STAFF"
    STATE_DATA_TEAM = "STATE_DATA_TEAM"
    STATE_LEADERSHIP = "STATE_LEADERSHIP"
    SUPERVISION_LINE_STAFF = "SUPERVISION_LINE_STAFF"
    SUPERVISION_NON_PRIMARY_STAFF = "SUPERVISION_NON_PRIMARY_STAFF"
    SUPERVISION_OFFICER_SUPERVISOR = "SUPERVISION_OFFICER_SUPERVISOR"
    SUPERVISION_LEADERSHIP = "SUPERVISION_LEADERSHIP"
    SUPERVISION_REGIONAL_LEADERSHIP = "SUPERVISION_REGIONAL_LEADERSHIP"
    UNKNOWN = "UNKNOWN"
