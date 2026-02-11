# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Constants for the Auth endpoints."""

PREDEFINED_ROLES = [
    "facilities_leadership",
    "facilities_line_staff",
    "facilities_non_primary_staff",
    "facilities_manager",
    "facilities_segregation_staff",
    "legislative_staff",
    "psi_staff",
    "state_data_team",
    "state_leadership",
    "supervision_leadership",
    "supervision_line_staff",
    "supervision_non_primary_staff",
    "supervision_officer_supervisor",
    "supervision_regional_leadership",
    "unknown",
    "cpa_staff",
    # This role is meant for granting JII tablet app access in situations where either
    # the applicable primary role is unknown or there are no primary roles set up
    # in the state (i.e. the JII app was the first tool we launched in the state).
    # It isn't needed in states where JII app access is already tied to an existing role.
    "facilities_jii_app_viewer",
]
