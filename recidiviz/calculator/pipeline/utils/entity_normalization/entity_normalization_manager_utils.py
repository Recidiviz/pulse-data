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
"""Utils for the normalization of state entities for calculations."""
import datetime
from typing import List, Optional, Tuple, Type, Union

from recidiviz.calculator.pipeline.utils.entity_normalization.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.incarceration_period_normalization_manager import (
    IncarcerationPeriodNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.program_assignment_normalization_manager import (
    ProgramAssignmentNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_period_normalization_manager import (
    SupervisionPeriodNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_violation_responses_normalization_manager import (
    ViolationResponseNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.period_utils import (
    find_earliest_date_of_period_ending_in_death,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_incarceration_delegate,
    get_state_specific_incarceration_period_normalization_delegate,
    get_state_specific_program_assignment_normalization_delegate,
    get_state_specific_supervision_period_normalization_delegate,
    get_state_specific_violation_response_normalization_delegate,
)
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateProgramAssignment,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolationResponse,
)

# All EntityNormalizationManagers
NORMALIZATION_MANAGERS: List[Type[EntityNormalizationManager]] = [
    IncarcerationPeriodNormalizationManager,
    ProgramAssignmentNormalizationManager,
    SupervisionPeriodNormalizationManager,
    ViolationResponseNormalizationManager,
]


def entity_normalization_managers_for_calculations(
    state_code: str,
    incarceration_periods: Optional[List[StateIncarcerationPeriod]],
    supervision_periods: Optional[List[StateSupervisionPeriod]],
    normalized_violation_responses: Optional[List[StateSupervisionViolationResponse]],
    field_index: CoreEntityFieldIndex,
    incarceration_sentences: Optional[List[StateIncarcerationSentence]],
    supervision_sentences: Optional[List[StateSupervisionSentence]],
) -> Tuple[
    Optional[IncarcerationPeriodNormalizationManager],
    Optional[SupervisionPeriodNormalizationManager],
]:
    """Helper for returning the IncarcerationNormalizationManager and
    SupervisionNormalizationManager needed to access normalized incarceration and
    supervision periods for calculations.

    DISCLAIMER: IP normalization may rely on normalized StateSupervisionPeriod
    entities for some states. Tread carefully if you are implementing any changes to
    SP normalization that may create circular dependencies between these processes.
    """
    state_ip_normalization_manager_delegate = (
        get_state_specific_incarceration_period_normalization_delegate(state_code)
    )
    state_sp_normalization_manager_delegate = (
        get_state_specific_supervision_period_normalization_delegate(state_code)
    )
    state_incarceration_delegate = get_state_specific_incarceration_delegate(state_code)

    if supervision_periods is None:
        if (
            state_ip_normalization_manager_delegate.normalization_relies_on_supervision_periods()
        ):
            raise ValueError(
                f"IP normalization for {state_code} relies on "
                "StateSupervisionPeriod entities. This pipeline must provide "
                "supervision_periods to be run on this state."
            )

    if normalized_violation_responses is None:
        if (
            state_ip_normalization_manager_delegate.normalization_relies_on_violation_responses()
        ):
            raise ValueError(
                f"IP normalization for {state_code} relies on "
                "StateSupervisionViolationResponse entities. This pipeline must "
                "provide violation_responses to be run on this state."
            )

    if supervision_periods is not None and (
        incarceration_sentences is None or supervision_sentences is None
    ):
        if state_sp_normalization_manager_delegate.normalization_relies_on_sentences():
            raise ValueError(
                f"SP normalization for {state_code} relies on "
                "StateIncarcerationSentence and StateSupevisionSentence entities."
                "This pipeline must provide sentences to be run on this state."
            )

    if supervision_periods is not None and incarceration_periods is None:
        if (
            state_sp_normalization_manager_delegate.normalization_relies_on_incarceration_periods()
        ):
            raise ValueError(
                f"SP normalization for {state_code} relies on StateIncarcerationPeriod entities."
                "This pipeline must provide incarceration periods to be run on this state."
            )

    all_periods: List[Union[StateIncarcerationPeriod, StateSupervisionPeriod]] = []
    all_periods.extend(supervision_periods or [])
    all_periods.extend(incarceration_periods or [])

    # The normalization functions need to know if this person has any periods that
    # ended because of death to handle any open periods or periods that extend past
    # their death date accordingly.
    earliest_death_date: Optional[
        datetime.date
    ] = find_earliest_date_of_period_ending_in_death(periods=all_periods)

    sp_normalization_manager: Optional[SupervisionPeriodNormalizationManager] = None
    supervision_period_index: Optional[NormalizedSupervisionPeriodIndex] = None

    if supervision_periods is not None:
        sp_normalization_manager = SupervisionPeriodNormalizationManager(
            supervision_periods=supervision_periods,
            delegate=state_sp_normalization_manager_delegate,
            earliest_death_date=earliest_death_date,
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences,
            incarceration_periods=incarceration_periods,
        )

        supervision_period_index = (
            sp_normalization_manager.normalized_supervision_period_index_for_calculations()
        )

    ip_normalization_manager = (
        IncarcerationPeriodNormalizationManager(
            incarceration_periods=incarceration_periods,
            normalization_delegate=state_ip_normalization_manager_delegate,
            incarceration_delegate=state_incarceration_delegate,
            normalized_supervision_period_index=supervision_period_index,
            violation_responses=normalized_violation_responses,
            earliest_death_date=earliest_death_date,
            field_index=field_index,
        )
        if incarceration_periods is not None
        else None
    )

    return ip_normalization_manager, sp_normalization_manager


def normalized_violation_responses_for_calculations(
    state_code: str,
    violation_responses: List[StateSupervisionViolationResponse],
) -> List[StateSupervisionViolationResponse]:
    """Instantiates the violation response manager and its appropriate delegate. Then
    returns normalized violation responses."""

    delegate = get_state_specific_violation_response_normalization_delegate(state_code)
    violation_response_manager = ViolationResponseNormalizationManager(
        violation_responses, delegate
    )
    return violation_response_manager.normalized_violation_responses_for_calculations()


def normalized_program_assignments_for_calculations(
    state_code: str, program_assignments: List[StateProgramAssignment]
) -> List[StateProgramAssignment]:
    """Instantiates the program assignment manager and its appropriate delegate. Then
    returns normalized program assignments."""

    delegate = get_state_specific_program_assignment_normalization_delegate(state_code)
    program_assignment_manager = ProgramAssignmentNormalizationManager(
        program_assignments, delegate
    )
    return program_assignment_manager.normalized_program_assignments_for_calculations()
