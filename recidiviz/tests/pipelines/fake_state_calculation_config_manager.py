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
"""Implements fake versions of the methods in state_calculation_config_manager.py and
provides helpers for mocking delegates.
"""
from datetime import date
from types import FunctionType, ModuleType
from typing import Any, Dict, List, Optional, Set

from mock import patch

from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateIncarcerationPeriod,
    StatePerson,
    StateStaffSupervisorPeriod,
    StateSupervisionContact,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateAssessment,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.metrics.utils.supervision_case_compliance_manager import (
    StateSupervisionCaseComplianceManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.assessment_normalization_manager import (
    StateSpecificAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.staff_role_period_normalization_manager import (
    StateSpecificStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.pipelines.utils.state_utils import state_calculation_config_manager
from recidiviz.pipelines.utils.state_utils.state_specific_commitment_from_supervision_delegate import (
    StateSpecificCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_assessment_normalization_delegate import (
    UsXxAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_commitment_from_supervision_utils import (
    UsXxCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_incarceration_delegate import (
    UsXxIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_incarceration_period_normalization_delegate import (
    UsXxIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_sentence_normalization_delegate import (
    UsXxSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_staff_role_period_normalization_delegate import (
    UsXxStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_supervision_period_normalization_delegate import (
    UsXxSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_violation_response_normalization_delegate import (
    UsXxViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_violations_delegate import (
    UsXxViolationDelegate,
)
from recidiviz.utils.range_querier import RangeQuerier

# pylint:disable=unused-argument


def get_state_specific_staff_role_period_normalization_delegate(
    state_code: str,
    staff_supervisor_periods: List[StateStaffSupervisorPeriod],
) -> StateSpecificStaffRolePeriodNormalizationDelegate:
    if state_code == StateCode.US_XX.value:
        return UsXxStaffRolePeriodNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def get_state_specific_violation_response_normalization_delegate(
    state_code: str,
    incarceration_periods: List[StateIncarcerationPeriod],
) -> StateSpecificViolationResponseNormalizationDelegate:
    if state_code == StateCode.US_XX.value:
        return UsXxViolationResponseNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def get_state_specific_sentence_normalization_delegate(
    state_code: str,
) -> StateSpecificSentenceNormalizationDelegate:
    if state_code == StateCode.US_XX.value:
        return UsXxSentenceNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def get_state_specific_incarceration_period_normalization_delegate(
    state_code: str,
) -> StateSpecificIncarcerationNormalizationDelegate:
    if state_code == StateCode.US_XX.value:
        return UsXxIncarcerationNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def get_state_specific_supervision_period_normalization_delegate(
    state_code: str,
    assessments: List[StateAssessment],
    incarceration_periods: List[StateIncarcerationPeriod],
    # TODO(#30199): Remove MO sentence statuses table dependency in favor of
    #  state_sentence_status_snapshot data
    us_mo_sentence_statuses_list: Optional[List[Dict[str, Any]]],
) -> StateSpecificSupervisionNormalizationDelegate:
    if state_code == StateCode.US_XX.value:
        return UsXxSupervisionNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def get_state_specific_assessment_normalization_delegate(
    state_code: str,
    person: StatePerson,
) -> StateSpecificAssessmentNormalizationDelegate:
    if state_code == StateCode.US_XX.value:
        return UsXxAssessmentNormalizationDelegate()
    raise ValueError(f"Unexpected state code [{state_code}]")


def get_state_specific_case_compliance_manager(
    person: StatePerson,
    supervision_period: NormalizedStateSupervisionPeriod,
    case_type: StateSupervisionCaseType,
    start_of_supervision: date,
    assessments_by_date: RangeQuerier[date, NormalizedStateAssessment],
    supervision_contacts_by_date: RangeQuerier[date, StateSupervisionContact],
    violation_responses: List[NormalizedStateSupervisionViolationResponse],
    incarceration_period_index: NormalizedIncarcerationPeriodIndex,
    supervision_delegate: StateSpecificSupervisionDelegate,
) -> Optional[StateSupervisionCaseComplianceManager]:
    return None


def get_state_specific_commitment_from_supervision_delegate(
    state_code: str,
) -> StateSpecificCommitmentFromSupervisionDelegate:
    if state_code == StateCode.US_XX.value:
        return UsXxCommitmentFromSupervisionDelegate()
    raise ValueError(f"Unexpected state code [{state_code}]")


def get_state_specific_violation_delegate(
    state_code: str,
) -> StateSpecificViolationDelegate:
    if state_code == StateCode.US_XX.value:
        return UsXxViolationDelegate()
    raise ValueError(f"Unexpected state code [{state_code}]")


def get_state_specific_incarceration_delegate(
    state_code: str,
) -> StateSpecificIncarcerationDelegate:
    if state_code == StateCode.US_XX.value:
        return UsXxIncarcerationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def get_state_specific_supervision_delegate(
    state_code: str,
) -> StateSpecificSupervisionDelegate:
    if state_code == StateCode.US_XX.value:
        return UsXxSupervisionDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def get_all_delegate_getter_fn_names() -> Set[str]:
    """Gets all delegate getter function names in state_calculation_config_manager.py."""
    fn_names = set()
    for fn_name in dir(state_calculation_config_manager):
        # TODO(#30363): Delete this exemption once we make all getter functions public
        #  in state_calculation_config_manager.py.
        if fn_name.startswith("_"):
            continue
        # TODO(#30363): Delete this exemption once we delete this function
        if fn_name == "get_required_state_specific_metrics_producer_delegates":
            continue
        original_fn = getattr(state_calculation_config_manager, fn_name)
        if not isinstance(original_fn, FunctionType):
            continue
        fn_names.add(fn_name)
    return fn_names


def start_pipeline_delegate_getter_patchers(module_to_mock: ModuleType) -> List[Any]:
    """Mocks all state_calculation_config_manager.py delegate getter methods imported
    in |module_to_mock| to their corresponding functions in this file. Returns the list
    of patcher objects which should be stopped in a test `tearDown()` method.
    """
    patchers = []
    for fn_name in get_all_delegate_getter_fn_names():
        if not hasattr(module_to_mock, fn_name):
            continue

        if fn_name not in globals():
            raise ValueError(
                f"Expected a function with name [{fn_name}] to be defined in this file."
            )

        # Get the mock version of the getter fn that's defined in this file
        mock_function = globals()[fn_name]
        delegate_patcher = patch(f"{module_to_mock.__name__}.{fn_name}", mock_function)
        delegate_patcher.start()
        patchers.append(delegate_patcher)
    return patchers