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
from typing import Any, Dict, List, Sequence, Set, Type, Union, no_type_check
from unittest.mock import MagicMock

import mock

from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.execution_utils import TableRow
from recidiviz.calculator.pipeline.utils.state_utils import (
    state_calculation_config_manager,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_required_state_specific_delegates,
    get_state_specific_case_compliance_manager,
    get_state_specific_supervision_delegate,
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
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod

STATE_UTILS_PATH = os.path.dirname(state_calculation_config_manager.__file__)

# The state-specific delegates that should be used in state-agnostic tests
STATE_DELEGATES_FOR_TESTS: Dict[str, StateSpecificDelegate] = {
    "StateSpecificIncarcerationNormalizationDelegate": UsXxIncarcerationNormalizationDelegate(),
    "StateSpecificSupervisionNormalizationDelegate": UsXxSupervisionNormalizationDelegate(),
    "StateSpecificProgramAssignmentNormalizationDelegate": UsXxProgramAssignmentNormalizationDelegate(),
    "StateSpecificViolationResponseNormalizationDelegate": UsXxViolationResponseNormalizationDelegate(),
    "StateSpecificCommitmentFromSupervisionDelegate": UsXxCommitmentFromSupervisionDelegate(),
    "StateSpecificViolationDelegate": UsXxViolationDelegate(),
    "StateSpecificIncarcerationDelegate": UsXxIncarcerationDelegate(),
    "StateSpecificSupervisionDelegate": UsXxSupervisionDelegate(),
}


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


@no_type_check
def test_get_required_state_specific_delegates() -> None:
    """Tests that we can call all functions in the state_calculation_config_manager
    file with all of the state codes that we expect to be supported."""
    supported_states = _get_supported_states()
    for state in supported_states:
        get_required_state_specific_delegates(state.value, [], entity_kwargs={})

        test_sp = StateSupervisionPeriod.new_with_defaults(state_code=state.value)

        get_state_specific_case_compliance_manager(
            person=None,
            supervision_period=test_sp,
            case_type=None,
            start_of_supervision=None,
            assessments=None,
            supervision_contacts=None,
            violation_responses=None,
            incarceration_sentences=None,
            incarceration_period_index=None,
            supervision_delegate=None,
        )

        get_state_specific_supervision_delegate(state.value)


class TestGetRequiredStateSpecificDelegates(unittest.TestCase):
    """Tests the get_required_state_specific_delegates function."""

    @mock.patch(
        "recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager"
        "._get_state_specific_violation_response_normalization_delegate"
    )
    @mock.patch(
        "recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager"
        "._get_state_specific_supervision_period_normalization_delegate"
    )
    @mock.patch(
        "recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager"
        "._get_state_specific_incarceration_period_normalization_delegate"
    )
    def test_get_required_state_specific_delegates(
        self,
        get_incarceration_delegate: MagicMock,
        get_supervision_delegate: MagicMock,
        get_violation_response_delegate: MagicMock,
    ) -> None:
        get_incarceration_delegate.return_value = STATE_DELEGATES_FOR_TESTS[
            StateSpecificIncarcerationNormalizationDelegate.__name__
        ]
        get_supervision_delegate.return_value = STATE_DELEGATES_FOR_TESTS[
            StateSpecificSupervisionNormalizationDelegate.__name__
        ]
        get_violation_response_delegate.return_value = STATE_DELEGATES_FOR_TESTS[
            StateSpecificViolationResponseNormalizationDelegate.__name__
        ]

        required_delegates = [
            StateSpecificIncarcerationNormalizationDelegate,
            StateSpecificSupervisionNormalizationDelegate,
            StateSpecificViolationResponseNormalizationDelegate,
        ]
        entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {}

        delegates = (
            state_calculation_config_manager.get_required_state_specific_delegates(
                state_code="US_XX",
                required_delegates=required_delegates,
                entity_kwargs=entity_kwargs,
            )
        )
        expected_delegates = {
            StateSpecificIncarcerationNormalizationDelegate.__name__: STATE_DELEGATES_FOR_TESTS[
                StateSpecificIncarcerationNormalizationDelegate.__name__
            ],
            StateSpecificSupervisionNormalizationDelegate.__name__: STATE_DELEGATES_FOR_TESTS[
                StateSpecificSupervisionNormalizationDelegate.__name__
            ],
            StateSpecificViolationResponseNormalizationDelegate.__name__: STATE_DELEGATES_FOR_TESTS[
                StateSpecificViolationResponseNormalizationDelegate.__name__
            ],
        }

        self.assertEqual(expected_delegates, delegates)

    def test_get_required_state_specific_delegates_no_delegates(self) -> None:
        required_delegates: List[Type[StateSpecificDelegate]] = []
        entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {}
        delegates = (
            state_calculation_config_manager.get_required_state_specific_delegates(
                state_code="US_XX",
                required_delegates=required_delegates,
                entity_kwargs=entity_kwargs,
            )
        )

        expected_delegates: Dict[str, Any] = {}

        self.assertEqual(expected_delegates, delegates)
