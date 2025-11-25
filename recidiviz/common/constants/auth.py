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
    SUPERVISION_LINE_STAFF = "SUPERVISION_LINE_STAFF"
    SUPERVISION_OFFICER_SUPERVISOR = "SUPERVISION_OFFICER_SUPERVISOR"
    SUPERVISION_LEADERSHIP = "SUPERVISION_LEADERSHIP"
    FACILITIES_LINE_STAFF = "FACILITIES_LINE_STAFF"
    FACILITIES_CASE_MANAGER = "FACILITIES_CASE_MANAGER"
    UNKNOWN = "UNKNOWN"
