# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests that all states with defined state-specific delegates are supported in the
state_calculation_config_manager functions."""
import os.path
from inspect import getmembers, isfunction
from typing import Set

from recidiviz.calculator.pipeline.utils.state_utils import (
    state_calculation_config_manager,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod

STATE_UTILS_PATH = os.path.dirname(state_calculation_config_manager.__file__)


def _get_supported_states() -> Set[StateCode]:
    """Determines which states have directories containing state-specific delegates in
    the state_utils directory."""
    directories = [
        dir_item
        for dir_item in os.listdir(STATE_UTILS_PATH)
        if os.path.isdir(os.path.join(STATE_UTILS_PATH, dir_item))
    ]
    supported_states: Set[StateCode] = set()

    for directory in directories:
        try:
            state_code = StateCode(directory.upper())
            supported_states.add(state_code)
        except ValueError:
            continue

    if not supported_states:
        raise ValueError(
            "Found zero supported states, which should never happen. If "
            "the location of the state-specific state utils directories "
            "have moved to a new location please update the "
            "STATE_UTILS_PATH."
        )

    return supported_states


def test_get_state_specific_delegate_functions() -> None:
    """Tests that we can call all functions in the state_calculation_config_manager
    file with all of the state codes that we expect to be supported."""
    supported_states = _get_supported_states()

    for state_delegate_function_name, _ in getmembers(
        state_calculation_config_manager, isfunction
    ):
        for state in supported_states:
            if (
                state_delegate_function_name
                == state_calculation_config_manager.get_state_specific_case_compliance_manager.__name__
            ):
                # The signature of this function differs from the rest
                test_sp = StateSupervisionPeriod.new_with_defaults(
                    state_code=state.value,
                )

                getattr(state_calculation_config_manager, state_delegate_function_name)(
                    None, test_sp, None, None, None, None, None, None, None, None
                )
            else:
                _ = getattr(
                    state_calculation_config_manager, state_delegate_function_name
                )(state.value)
