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
"""Manages state-specific methodology decisions made throughout the calculation pipelines."""
from datetime import date
from typing import Dict, List, Optional, Type

import attr

from recidiviz.calculator.pipeline.metrics.utils.supervision_case_compliance_manager import (
    StateSupervisionCaseComplianceManager,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities import (
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.program_assignment_normalization_manager import (
    StateSpecificProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_commitment_from_supervision_delegate import (
    StateSpecificCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_commitment_from_supervision_delegate import (
    UsIdCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_incarceration_delegate import (
    UsIdIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_incarceration_period_normalization_delegate import (
    UsIdIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_program_assignment_normalization_delegate import (
    UsIdProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_compliance import (
    UsIdSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_delegate import (
    UsIdSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_period_normalization_delegate import (
    UsIdSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_violation_response_normalization_delegate import (
    UsIdViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_violations_delegate import (
    UsIdViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_commitment_from_supervision_delegate import (
    UsMeCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_incarceration_delegate import (
    UsMeIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_incarceration_period_normalization_delegate import (
    UsMeIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_program_assignment_normalization_delegate import (
    UsMeProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_supervision_delegate import (
    UsMeSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_supervision_period_normalization_delegate import (
    UsMeSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_violation_response_normalization_delegate import (
    UsMeViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_violations_delegate import (
    UsMeViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_commitment_from_supervision_delegate import (
    UsMoCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_incarceration_delegate import (
    UsMoIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_incarceration_period_normalization_delegate import (
    UsMoIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_program_assignment_normalization_delegate import (
    UsMoProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_delegate import (
    UsMoSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_period_normalization_delegate import (
    UsMoSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violation_response_normalization_delegate import (
    UsMoViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violations_delegate import (
    UsMoViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_commitment_from_supervision_delegate import (
    UsNdCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_incarceration_delegate import (
    UsNdIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_incarceration_period_normalization_delegate import (
    UsNdIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_program_assignment_normalization_delegate import (
    UsNdProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_compliance import (
    UsNdSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_delegate import (
    UsNdSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_period_normalization_delegate import (
    UsNdSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_violation_response_normalization_delegate import (
    UsNdViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_violations_delegate import (
    UsNdViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_commitment_from_supervision_delegate import (
    UsPaCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_incarceration_delegate import (
    UsPaIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_incarceration_period_normalization_delegate import (
    UsPaIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_program_assignment_normalization_delegate import (
    UsPaProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_compliance import (
    UsPaSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_delegate import (
    UsPaSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_period_normalization_delegate import (
    UsPaSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_violation_response_normalization_delegate import (
    UsPaViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_violations_delegate import (
    UsPaViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_commitment_from_supervision_delegate import (
    UsTnCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_incarceration_delegate import (
    UsTnIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_incarceration_period_normalization_delegate import (
    UsTnIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_program_assignment_normalization_delegate import (
    UsTnProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_supervision_delegate import (
    UsTnSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_supervision_period_normalization_delegate import (
    UsTnSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_violation_response_normalization_delegate import (
    UsTnViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_violations_delegate import (
    UsTnViolationDelegate,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateIncarcerationSentence,
    StatePerson,
    StateSupervisionContact,
)


@attr.s(frozen=True)
class StateSpecificDelegateContainer:
    """Stores all state-specific delegates required for running pipelines."""

    state_code: StateCode = attr.ib()

    ip_normalization_delegate: StateSpecificIncarcerationNormalizationDelegate = (
        attr.ib()
    )
    sp_normalization_delegate: StateSpecificSupervisionNormalizationDelegate = attr.ib()
    program_assignment_normalization_delegate: StateSpecificProgramAssignmentNormalizationDelegate = (
        attr.ib()
    )
    violation_response_normalization_delegate: StateSpecificViolationResponseNormalizationDelegate = (
        attr.ib()
    )
    commitment_from_supervision_delegate: StateSpecificCommitmentFromSupervisionDelegate = (
        attr.ib()
    )

    violation_delegate: StateSpecificViolationDelegate = attr.ib()
    incarceration_delegate: StateSpecificIncarcerationDelegate = attr.ib()
    supervision_delegate: StateSpecificSupervisionDelegate = attr.ib()


def get_required_state_specific_delegates(
    state_code: str,
    required_delegates: List[Type[StateSpecificDelegate]],
) -> Dict[str, StateSpecificDelegate]:
    """Returns a dictionary where the keys are the names of all of the delegates
    listed in |required_delegates|, and the values are the state-specific
    implementation of that delegate."""
    all_delegates_for_state = get_all_state_specific_delegates(state_code)

    return {
        delegate.__class__.__base__.__name__: delegate
        for delegate in all_delegates_for_state.__dict__.values()
        if delegate.__class__.__base__ in required_delegates
    }


def get_all_state_specific_delegates(
    state_code: str,
) -> StateSpecificDelegateContainer:
    return StateSpecificDelegateContainer(
        state_code=StateCode(state_code),
        ip_normalization_delegate=_get_state_specific_incarceration_period_normalization_delegate(
            state_code
        ),
        sp_normalization_delegate=_get_state_specific_supervision_period_normalization_delegate(
            state_code
        ),
        program_assignment_normalization_delegate=_get_state_specific_program_assignment_normalization_delegate(
            state_code
        ),
        violation_response_normalization_delegate=_get_state_specific_violation_response_normalization_delegate(
            state_code
        ),
        commitment_from_supervision_delegate=_get_state_specific_commitment_from_supervision_delegate(
            state_code
        ),
        violation_delegate=_get_state_specific_violation_delegate(state_code),
        incarceration_delegate=_get_state_specific_incarceration_delegate(state_code),
        supervision_delegate=get_state_specific_supervision_delegate(state_code),
    )


def get_state_specific_case_compliance_manager(
    person: StatePerson,
    supervision_period: NormalizedStateSupervisionPeriod,
    case_type: StateSupervisionCaseType,
    start_of_supervision: date,
    assessments: List[StateAssessment],
    supervision_contacts: List[StateSupervisionContact],
    violation_responses: List[NormalizedStateSupervisionViolationResponse],
    incarceration_sentences: List[StateIncarcerationSentence],
    incarceration_period_index: NormalizedIncarcerationPeriodIndex,
    supervision_delegate: StateSpecificSupervisionDelegate,
) -> Optional[StateSupervisionCaseComplianceManager]:
    """Returns a state-specific SupervisionCaseComplianceManager object, containing information about whether the
    given supervision case is in compliance with state-specific standards. If the state of the
    supervision_period does not have state-specific compliance calculations, returns None."""
    state_code = supervision_period.state_code.upper()
    if state_code == StateCode.US_ID.value:
        return UsIdSupervisionCaseCompliance(
            person,
            supervision_period,
            case_type,
            start_of_supervision,
            assessments,
            supervision_contacts,
            violation_responses,
            incarceration_sentences,
            incarceration_period_index,
            supervision_delegate,
        )
    if state_code == StateCode.US_ND.value:
        return UsNdSupervisionCaseCompliance(
            person,
            supervision_period,
            case_type,
            start_of_supervision,
            assessments,
            supervision_contacts,
            violation_responses,
            incarceration_sentences,
            incarceration_period_index,
            supervision_delegate,
        )
    if state_code == StateCode.US_PA.value:
        return UsPaSupervisionCaseCompliance(
            person,
            supervision_period,
            case_type,
            start_of_supervision,
            assessments,
            supervision_contacts,
            violation_responses,
            incarceration_sentences,
            incarceration_period_index,
            supervision_delegate,
        )

    return None


def _get_state_specific_incarceration_period_normalization_delegate(
    state_code: str,
) -> StateSpecificIncarcerationNormalizationDelegate:
    """Returns the type of IncarcerationNormalizationDelegate that should be used for
    normalizing StateIncarcerationPeriod entities from a given |state_code|."""
    if state_code == StateCode.US_ID.value:
        return UsIdIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnIncarcerationNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_supervision_period_normalization_delegate(
    state_code: str,
) -> StateSpecificSupervisionNormalizationDelegate:
    """Returns the type of SupervisionNormalizationDelegate that should be used for
    normalizing StateSupervisionPeriod entities from a given |state_code|."""
    if state_code == StateCode.US_ID.value:
        return UsIdSupervisionNormalizationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeSupervisionNormalizationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoSupervisionNormalizationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdSupervisionNormalizationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaSupervisionNormalizationDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnSupervisionNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_program_assignment_normalization_delegate(
    state_code: str,
) -> StateSpecificProgramAssignmentNormalizationDelegate:
    """Returns the type of ProgramAssignmentNormalizationDelegate that should be used for
    normalizing StateProgramAssignment entities from a given |state_code|."""
    if state_code == StateCode.US_ID.value:
        return UsIdProgramAssignmentNormalizationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeProgramAssignmentNormalizationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoProgramAssignmentNormalizationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdProgramAssignmentNormalizationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaProgramAssignmentNormalizationDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnProgramAssignmentNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_commitment_from_supervision_delegate(
    state_code: str,
) -> StateSpecificCommitmentFromSupervisionDelegate:
    """Returns the type of StateSpecificCommitmentFromSupervisionDelegate that should be used for
    commitment from supervision admission calculations in a given |state_code|."""
    if state_code == StateCode.US_ID.value:
        return UsIdCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnCommitmentFromSupervisionDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_violation_delegate(
    state_code: str,
) -> StateSpecificViolationDelegate:
    """Returns the type of StateSpecificViolationDelegate that should be used for
    violation calculations in a given |state_code|."""
    if state_code == StateCode.US_ID.value:
        return UsIdViolationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeViolationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoViolationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdViolationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaViolationDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnViolationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_violation_response_normalization_delegate(
    state_code: str,
) -> StateSpecificViolationResponseNormalizationDelegate:
    """Returns the type of StateSpecificViolationResponseNormalizationDelegate that should be used for
    violation calculations in a given |state_code|."""
    if state_code == StateCode.US_ID.value:
        return UsIdViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnViolationResponseNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_incarceration_delegate(
    state_code: str,
) -> StateSpecificIncarcerationDelegate:
    """Returns the type of StateSpecificIncarcerationDelegate that should be used for
    incarceration calculations in a given |state_code|."""
    if state_code == StateCode.US_ID.value:
        return UsIdIncarcerationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeIncarcerationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoIncarcerationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdIncarcerationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaIncarcerationDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnIncarcerationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


# TODO(#10891): Make this a private method once it's no longer being called from
#  outside of this file
def get_state_specific_supervision_delegate(
    state_code: str,
) -> StateSpecificSupervisionDelegate:
    """Returns the type of StateSpecificSupervisionDelegate that should be used for
    supervision calculations in a given |state_code|."""
    if state_code == StateCode.US_ID.value:
        return UsIdSupervisionDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeSupervisionDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoSupervisionDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdSupervisionDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaSupervisionDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnSupervisionDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")
