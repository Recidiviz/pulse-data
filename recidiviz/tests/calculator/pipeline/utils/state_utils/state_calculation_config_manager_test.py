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
import unittest
from inspect import getmembers, isfunction
from typing import Any, Dict, Set, Type

import mock

from recidiviz.calculator.pipeline.utils.entity_normalization.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils import (
    state_calculation_config_manager,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    StateSpecificDelegateContainer,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_commitment_from_supervision_utils import (
    UsXxCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_delegate import (
    UsXxIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_period_normalization_delegate import (
    UsXxIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_program_assignment_normalization_delegate import (
    UsXxProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_supervision_period_normalization_delegate import (
    UsXxSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_violation_response_normalization_delegate import (
    UsXxViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_violations_delegate import (
    UsXxViolationDelegate,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod

STATE_UTILS_PATH = os.path.dirname(state_calculation_config_manager.__file__)

# The StateSpecificDelegateContainer that should be used in state-agnostic tests
STATE_DELEGATES_FOR_TESTS = StateSpecificDelegateContainer(
    state_code=StateCode.US_XX,
    ip_normalization_delegate=UsXxIncarcerationNormalizationDelegate(),
    sp_normalization_delegate=UsXxSupervisionNormalizationDelegate(),
    program_assignment_normalization_delegate=UsXxProgramAssignmentNormalizationDelegate(),
    violation_response_normalization_delegate=UsXxViolationResponseNormalizationDelegate(),
    commitment_from_supervision_delegate=UsXxCommitmentFromSupervisionDelegate(),
    violation_delegate=UsXxViolationDelegate(),
    incarceration_delegate=UsXxIncarcerationDelegate(),
    supervision_delegate=UsXxSupervisionDelegate(),
)


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
            elif (
                state_delegate_function_name
                == state_calculation_config_manager.get_required_state_specific_delegates.__name__
            ):
                getattr(state_calculation_config_manager, state_delegate_function_name)(
                    state.value, {}
                )
            else:
                _ = getattr(
                    state_calculation_config_manager, state_delegate_function_name
                )(state.value)


class TestGetRequiredStateSpecificDelegates(unittest.TestCase):
    """Tests the get_required_state_specific_delegates function."""

    def setUp(self) -> None:
        self.state_specific_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.state_utils"
            ".state_calculation_config_manager.get_all_state_specific_delegates"
        )
        self.mock_get_state_delegate_container = (
            self.state_specific_delegate_patcher.start()
        )
        self.mock_get_state_delegate_container.return_value = STATE_DELEGATES_FOR_TESTS

    def test_get_required_state_specific_delegates(self) -> None:
        required_delegates = {
            StateSpecificIncarcerationNormalizationDelegate,
            StateSpecificSupervisionNormalizationDelegate,
            StateSpecificViolationResponseNormalizationDelegate,
        }

        delegates = (
            state_calculation_config_manager.get_required_state_specific_delegates(
                state_code="US_XX", required_delegates=required_delegates
            )
        )

        expected_delegates = {
            StateSpecificIncarcerationNormalizationDelegate.__name__: STATE_DELEGATES_FOR_TESTS.ip_normalization_delegate,
            StateSpecificSupervisionNormalizationDelegate.__name__: STATE_DELEGATES_FOR_TESTS.sp_normalization_delegate,
            StateSpecificViolationResponseNormalizationDelegate.__name__: STATE_DELEGATES_FOR_TESTS.violation_response_normalization_delegate,
        }

        self.assertEqual(expected_delegates, delegates)

    def test_get_required_state_specific_delegates_no_delegates(self) -> None:
        required_delegates: Set[Type[StateSpecificDelegate]] = set()

        delegates = (
            state_calculation_config_manager.get_required_state_specific_delegates(
                state_code="US_XX", required_delegates=required_delegates
            )
        )

        expected_delegates: Dict[str, Any] = {}

        self.assertEqual(expected_delegates, delegates)
