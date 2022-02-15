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
from typing import Dict, List, Optional, Sequence, Tuple, Type, Union

from recidiviz.calculator.pipeline.utils.entity_normalization.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.incarceration_period_normalization_manager import (
    IncarcerationPeriodNormalizationManager,
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.program_assignment_normalization_manager import (
    ProgramAssignmentNormalizationManager,
    StateSpecificProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
    SupervisionPeriodNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
    ViolationResponseNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.period_utils import (
    find_earliest_date_of_period_ending_in_death,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.persistence.entity.base_entity import Entity
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


def all_normalized_entities(
    ip_normalization_delegate: StateSpecificIncarcerationNormalizationDelegate,
    sp_normalization_delegate: StateSpecificSupervisionNormalizationDelegate,
    violation_response_normalization_delegate: StateSpecificViolationResponseNormalizationDelegate,
    program_assignment_normalization_delegate: StateSpecificProgramAssignmentNormalizationDelegate,
    incarceration_delegate: StateSpecificIncarcerationDelegate,
    incarceration_periods: List[StateIncarcerationPeriod],
    supervision_periods: List[StateSupervisionPeriod],
    violation_responses: List[StateSupervisionViolationResponse],
    program_assignments: List[StateProgramAssignment],
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    field_index: CoreEntityFieldIndex,
) -> Dict[str, Sequence[Entity]]:
    """Normalizes all entities that have corresponding comprehensive managers.

    Returns a dictionary mapping the entity class name to the list of normalized
    entities.
    """
    normalized_violation_responses = normalized_violation_responses_for_calculations(
        violation_response_normalization_delegate=violation_response_normalization_delegate,
        violation_responses=violation_responses,
    )

    normalize_program_assignments = normalized_program_assignments_for_calculations(
        program_assignment_normalization_delegate=program_assignment_normalization_delegate,
        program_assignments=program_assignments,
    )

    (
        ip_normalization_manager,
        sp_normalization_manager,
    ) = entity_normalization_managers_for_periods(
        ip_normalization_delegate=ip_normalization_delegate,
        sp_normalization_delegate=sp_normalization_delegate,
        incarceration_delegate=incarceration_delegate,
        incarceration_periods=incarceration_periods,
        supervision_periods=supervision_periods,
        normalized_violation_responses=normalized_violation_responses,
        field_index=field_index,
        incarceration_sentences=incarceration_sentences,
        supervision_sentences=supervision_sentences,
    )

    if not ip_normalization_manager:
        raise ValueError(
            "Expected instantiated "
            "IncarcerationPeriodNormalizationManager. Found None."
        )

    if not sp_normalization_manager:
        raise ValueError(
            "Expected instantiated "
            "SupervisionPeriodNormalizationManager. Found None."
        )

    # TODO(#10727): Move collapsing of transfers to later
    normalized_ips = (
        ip_normalization_manager.normalized_incarceration_period_index_for_calculations(
            collapse_transfers=False, overwrite_facility_information_in_transfers=False
        ).incarceration_periods
    )

    normalized_sps = (
        sp_normalization_manager.normalized_supervision_period_index_for_calculations().supervision_periods
    )

    return {
        StateIncarcerationPeriod.__name__: normalized_ips,
        StateSupervisionPeriod.__name__: normalized_sps,
        StateSupervisionViolationResponse.__name__: normalized_violation_responses,
        StateProgramAssignment.__name__: normalize_program_assignments,
    }


def entity_normalization_managers_for_periods(
    ip_normalization_delegate: StateSpecificIncarcerationNormalizationDelegate,
    sp_normalization_delegate: StateSpecificSupervisionNormalizationDelegate,
    incarceration_delegate: StateSpecificIncarcerationDelegate,
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
    if supervision_periods is None:
        if ip_normalization_delegate.normalization_relies_on_supervision_periods():
            raise ValueError(
                "IP normalization for this state relies on "
                "StateSupervisionPeriod entities. This pipeline must provide "
                "supervision_periods to be run on this state."
            )

    if normalized_violation_responses is None:
        if ip_normalization_delegate.normalization_relies_on_violation_responses():
            raise ValueError(
                "IP normalization for this state relies on "
                "StateSupervisionViolationResponse entities. This pipeline must "
                "provide violation_responses to be run on this state."
            )

    if supervision_periods is not None and (
        incarceration_sentences is None or supervision_sentences is None
    ):
        if sp_normalization_delegate.normalization_relies_on_sentences():
            raise ValueError(
                "SP normalization for this state relies on "
                "StateIncarcerationSentence and StateSupevisionSentence entities."
                "This pipeline must provide sentences to be run on this state."
            )

    if supervision_periods is not None and incarceration_periods is None:
        if sp_normalization_delegate.normalization_relies_on_incarceration_periods():
            raise ValueError(
                "SP normalization for this state relies on StateIncarcerationPeriod entities."
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
            delegate=sp_normalization_delegate,
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
            normalization_delegate=ip_normalization_delegate,
            incarceration_delegate=incarceration_delegate,
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
    violation_response_normalization_delegate: StateSpecificViolationResponseNormalizationDelegate,
    violation_responses: List[StateSupervisionViolationResponse],
) -> List[StateSupervisionViolationResponse]:
    """Instantiates the violation response manager and its appropriate delegate. Then
    returns normalized violation responses."""
    violation_response_manager = ViolationResponseNormalizationManager(
        violation_responses,
        violation_response_normalization_delegate,
    )
    return violation_response_manager.normalized_violation_responses_for_calculations()


def normalized_program_assignments_for_calculations(
    program_assignment_normalization_delegate: StateSpecificProgramAssignmentNormalizationDelegate,
    program_assignments: List[StateProgramAssignment],
) -> List[StateProgramAssignment]:
    """Instantiates the program assignment manager and its appropriate delegate. Then
    returns normalized program assignments."""
    program_assignment_manager = ProgramAssignmentNormalizationManager(
        program_assignments,
        program_assignment_normalization_delegate,
    )
    return program_assignment_manager.normalized_program_assignments_for_calculations()
