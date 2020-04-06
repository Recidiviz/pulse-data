# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Manages state-specific methodology decisions made throughout the calculation pipelines."""
# TODO(2995): Make a state config file for every state and every one of these state-specific calculation methodologies


def supervision_types_distinct_for_state(state_code: str) -> bool:
    """For some states, we want to track DUAL supervision as distinct from both PAROLE and PROBATION. For others, a
    person can be on multiple types of supervision simultaneously and contribute to counts for both types.

        - US_MO: True
        - US_ND: False

    Returns whether our calculations should consider supervision types as distinct for the given state_code.
    """
    return state_code.upper() == 'US_MO'


def use_supervision_periods_for_pre_incarceration_supervision_type_for_state(state_code: str) -> bool:
    """For some states, we should determine the supervision type prior to someone's incarceration by looking at the
    information on the preceding supervision periods. For other states, we should determine the supervision type prior
    to incarceration by looking only at the incarceration period admission reason.

        - US_MO: True
        - US_ND: False # TODO(2938): Decide if we want date matching logic for US_ND

    Returns whether we should use the supervision periods to determine pre-incarceration supervision type for the given
    state_code.
    """
    return state_code.upper() == 'US_MO'


def default_to_supervision_period_officer_for_revocation_details_for_state(state_code: str) -> bool:
    """For some states, if there's no officer information coming from the source_supervision_violation_response,
    we should default to the officer information on the overlapping supervision period for the revocation details.

        - US_MO: True
        - US_ND: False

    Returns whether our calculations should use supervising officer information for revocation details.
    """
    return state_code.upper() == 'US_MO'


def drop_temporary_custody_incarceration_periods_for_state(state_code: str) -> bool:
    """For some states, we should always disregard incarceration periods of temporary custody in the calculation
    pipelines.

        - US_MO: False
        - US_ND: True

    Returns whether our calculations should drop temporary custody periods for the given state.
    """
    return state_code.upper() == 'US_ND'
