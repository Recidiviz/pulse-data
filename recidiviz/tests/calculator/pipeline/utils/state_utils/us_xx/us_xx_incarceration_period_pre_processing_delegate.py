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
"""Contains generic implementation of the StateSpecificIncarcerationPreProcessingDelegate
 for used in state-agnostic tests."""
from typing import List, Optional, Set

from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    PurposeForIncarcerationInfo,
    StateSpecificIncarcerationPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionViolationResponse,
)


class UsXxIncarcerationPreProcessingDelegate(
    StateSpecificIncarcerationPreProcessingDelegate
):
    """Test version of StateSpecificIncarcerationPreProcessingDelegate that uses the
    default behavior for all functions."""

    # Functions with state-specific overrides
    # No deviations from default logic for US_XX

    # Functions using default behavior
    def admission_reasons_to_filter(
        self,
    ) -> Set[StateIncarcerationPeriodAdmissionReason]:
        return self._default_admission_reasons_to_filter()

    def incarceration_types_to_filter(self) -> Set[StateIncarcerationType]:
        return self._default_incarceration_types_to_filter()

    def get_pfi_info_for_period_if_commitment_from_supervision(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        violation_responses: Optional[List[StateSupervisionViolationResponse]],
    ) -> PurposeForIncarcerationInfo:
        return self._default_get_pfi_info_for_period_if_commitment_from_supervision(
            sorted_incarceration_periods[incarceration_period_list_index]
        )

    def normalize_period_if_commitment_from_supervision(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: Optional[PreProcessedSupervisionPeriodIndex],
    ) -> StateIncarcerationPeriod:
        return self._default_normalize_period_if_commitment_from_supervision(
            sorted_incarceration_periods[incarceration_period_list_index]
        )

    def period_is_parole_board_hold(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
    ) -> bool:
        return self._default_period_is_parole_board_hold(
            sorted_incarceration_periods[incarceration_period_list_index]
        )

    def period_is_non_board_hold_temporary_custody(
        self, incarceration_period: StateIncarcerationPeriod
    ) -> bool:
        return self._default_period_is_non_board_hold_temporary_custody(
            incarceration_period
        )

    def pre_processing_incarceration_period_admission_reason_mapper(
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> Optional[StateIncarcerationPeriodAdmissionReason]:
        return (
            self._default_pre_processing_incarceration_period_admission_reason_mapper(
                incarceration_period
            )
        )

    def pre_processing_relies_on_supervision_periods(self) -> bool:
        return self._default_pre_processing_relies_on_supervision_periods()

    def pre_processing_relies_on_violation_responses(self) -> bool:
        return self._default_pre_processing_relies_on_violation_responses()
