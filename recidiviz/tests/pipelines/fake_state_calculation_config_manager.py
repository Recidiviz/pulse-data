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
from typing import Any, List, Optional, Set

from mock import patch

from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateIncarcerationPeriod,
    StatePerson,
    StateSentence,
    StateStaffSupervisorPeriod,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateAssessment,
    NormalizedStateIncarcerationSentence,
    NormalizedStatePerson,
    NormalizedStateSupervisionContact,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionSentence,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.assessment_normalization_manager import (
    StateSpecificAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.staff_role_period_normalization_manager import (
    StateSpecificStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.ingest.state.normalization.state_specific_normalization_delegate import (
    StateSpecificNormalizationDelegate,
)
from recidiviz.pipelines.metrics.utils.supervision_case_compliance_manager import (
    StateSupervisionCaseComplianceManager,
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
from recidiviz.utils.range_querier import RangeQuerier

# pylint:disable=unused-argument


def get_state_specific_staff_role_period_normalization_delegate(
    state_code: str,
    staff_supervisor_periods: List[StateStaffSupervisorPeriod],
) -> StateSpecificStaffRolePeriodNormalizationDelegate:
    return StateSpecificStaffRolePeriodNormalizationDelegate()


def get_state_specific_violation_response_normalization_delegate(
    state_code: str,
    incarceration_periods: List[StateIncarcerationPeriod],
) -> StateSpecificViolationResponseNormalizationDelegate:
    return StateSpecificViolationResponseNormalizationDelegate()


def get_state_specific_sentence_normalization_delegate(
    state_code: str,
) -> StateSpecificSentenceNormalizationDelegate:
    return StateSpecificSentenceNormalizationDelegate()


def get_state_specific_incarceration_period_normalization_delegate(
    state_code: str, incarceration_sentences: List[NormalizedStateIncarcerationSentence]
) -> StateSpecificIncarcerationNormalizationDelegate:
    return StateSpecificIncarcerationNormalizationDelegate()


def get_state_specific_supervision_period_normalization_delegate(
    state_code: str,
    assessments: List[StateAssessment],
    supervision_sentences: List[NormalizedStateSupervisionSentence],
    incarceration_periods: List[StateIncarcerationPeriod],
    sentences: List[StateSentence],
) -> StateSpecificSupervisionNormalizationDelegate:
    return StateSpecificSupervisionNormalizationDelegate()


def get_state_specific_assessment_normalization_delegate(
    state_code: str,
    person: StatePerson,
) -> StateSpecificAssessmentNormalizationDelegate:
    return StateSpecificAssessmentNormalizationDelegate()


def get_state_specific_case_compliance_manager(
    person: NormalizedStatePerson,
    supervision_period: NormalizedStateSupervisionPeriod,
    case_type: StateSupervisionCaseType,
    start_of_supervision: date,
    assessments_by_date: RangeQuerier[date, NormalizedStateAssessment],
    supervision_contacts_by_date: RangeQuerier[date, NormalizedStateSupervisionContact],
    violation_responses: List[NormalizedStateSupervisionViolationResponse],
    incarceration_period_index: NormalizedIncarcerationPeriodIndex,
    supervision_delegate: StateSpecificSupervisionDelegate,
) -> Optional[StateSupervisionCaseComplianceManager]:
    return None


def get_state_specific_commitment_from_supervision_delegate(
    state_code: str,
) -> StateSpecificCommitmentFromSupervisionDelegate:
    return StateSpecificCommitmentFromSupervisionDelegate()


def get_state_specific_violation_delegate(
    state_code: str,
) -> StateSpecificViolationDelegate:
    return StateSpecificViolationDelegate()


def get_state_specific_incarceration_delegate(
    state_code: str,
) -> StateSpecificIncarcerationDelegate:
    return StateSpecificIncarcerationDelegate()


class GenericTestStateSupervisionDelegate(StateSpecificSupervisionDelegate):
    def assessment_types_to_include_for_class(
        self, assessment_class: StateAssessmentClass
    ) -> Optional[List[StateAssessmentType]]:
        """For unit tests, we support all types of assessments."""
        if assessment_class == StateAssessmentClass.RISK:
            return [
                StateAssessmentType.LSIR,
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            ]
        return None


def get_state_specific_supervision_delegate(
    state_code: str,
) -> StateSpecificSupervisionDelegate:
    return GenericTestStateSupervisionDelegate()


def get_state_specific_normalization_delegate(
    state_code: str,
) -> StateSpecificNormalizationDelegate:
    return StateSpecificNormalizationDelegate()


def get_all_delegate_getter_fn_names() -> Set[str]:
    """Gets all delegate getter function names in state_calculation_config_manager.py."""
    fn_names = set()
    for fn_name in dir(state_calculation_config_manager):
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
