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
"""Utils for pre-processing state entities for calculations."""
import datetime
from typing import List, Optional, Tuple, Union

from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    IncarcerationPreProcessingManager,
)
from recidiviz.calculator.pipeline.utils.period_utils import (
    find_earliest_date_of_period_ending_in_death,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_incarceration_period_pre_processing_delegate,
)
from recidiviz.calculator.pipeline.utils.supervision_period_pre_processing_manager import (
    SupervisionPreProcessingManager,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
    StateSupervisionViolationResponse,
)


def pre_processing_managers_for_calculations(
    state_code: str,
    incarceration_periods: Optional[List[StateIncarcerationPeriod]],
    supervision_periods: Optional[List[StateSupervisionPeriod]],
    violation_responses: Optional[List[StateSupervisionViolationResponse]],
) -> Tuple[
    Optional[IncarcerationPreProcessingManager],
    Optional[SupervisionPreProcessingManager],
]:
    """Helper for returning the IncarcerationPreProcessingManager and
    SupervisionPreProcessingManager needed to access pre-processed incarceration and
    supervision periods for calculations.

    DISCLAIMER: IP pre-processing may rely on pre-processed StateSupervisionPeriod
    entities for some states. Tread carefully if you are implementing any changes to
    SP pre-processing that may create circular dependencies between these processes.
    """
    state_ip_pre_processing_manager_delegate = (
        get_state_specific_incarceration_period_pre_processing_delegate(state_code)
    )

    if supervision_periods is None:
        if (
            state_ip_pre_processing_manager_delegate.pre_processing_relies_on_supervision_periods()
        ):
            raise ValueError(
                f"IP pre-processing for {state_code} relies on "
                "StateSupervisionPeriod entities. This pipeline must provide "
                "supervision_periods to be run on this state."
            )

    if violation_responses is None:
        if (
            state_ip_pre_processing_manager_delegate.pre_processing_relies_on_violation_responses()
        ):
            raise ValueError(
                f"IP pre-processing for {state_code} relies on "
                "StateSupervisionViolationResponse entities. This pipeline must "
                "provide violation_responses to be run on this state."
            )

    all_periods: List[Union[StateIncarcerationPeriod, StateSupervisionPeriod]] = []
    all_periods.extend(supervision_periods or [])
    all_periods.extend(incarceration_periods or [])

    # The pre-processing functions need to know if this person has any periods that
    # ended because of death to handle any open periods or periods that extend past
    # their death date accordingly.
    earliest_death_date: Optional[
        datetime.date
    ] = find_earliest_date_of_period_ending_in_death(periods=all_periods)

    sp_pre_processing_manager: Optional[SupervisionPreProcessingManager] = None
    supervision_period_index: Optional[PreProcessedSupervisionPeriodIndex] = None

    if supervision_periods is not None:
        sp_pre_processing_manager = SupervisionPreProcessingManager(
            supervision_periods=supervision_periods,
            earliest_death_date=earliest_death_date,
        )

        supervision_period_index = (
            sp_pre_processing_manager.pre_processed_supervision_period_index_for_calculations()
        )

    ip_pre_processing_manager = (
        IncarcerationPreProcessingManager(
            incarceration_periods=incarceration_periods,
            delegate=state_ip_pre_processing_manager_delegate,
            pre_processed_supervision_period_index=supervision_period_index,
            violation_responses=violation_responses,
            earliest_death_date=earliest_death_date,
        )
        if incarceration_periods is not None
        else None
    )

    return ip_pre_processing_manager, sp_pre_processing_manager
