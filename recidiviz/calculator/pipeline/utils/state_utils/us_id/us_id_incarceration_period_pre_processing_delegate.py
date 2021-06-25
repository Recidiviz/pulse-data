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
"""Contains state-specific logic for certain aspects of pre-processing US_ID
StateIncarcerationPeriod entities so that they are ready to be used in pipeline
calculations."""
from typing import Optional, Set

from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    StateSpecificIncarcerationPreProcessingDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class UsIdIncarcerationPreProcessingDelegate(
    StateSpecificIncarcerationPreProcessingDelegate
):
    """US_ID implementation of the StateSpecificIncarcerationPreProcessingDelegate."""

    # Functions with state-specific overrides
    # No deviations from default logic for US_ID

    # Functions using default behavior
    def admission_reasons_to_filter(
        self,
    ) -> Set[StateIncarcerationPeriodAdmissionReason]:
        return self._default_admission_reasons_to_filter()

    def normalize_period_if_commitment_from_supervision(
        self, incarceration_period: StateIncarcerationPeriod
    ) -> StateIncarcerationPeriod:
        # TODO(#7441): Implement overrides for US_ID
        return self._default_normalize_period_if_commitment_from_supervision(
            incarceration_period
        )

    def incarceration_types_to_filter(self) -> Set[StateIncarcerationType]:
        return self._default_incarceration_types_to_filter()

    def period_is_parole_board_hold(
        self, incarceration_period: StateIncarcerationPeriod
    ) -> bool:
        return self._default_period_is_parole_board_hold(incarceration_period)

    def period_is_non_board_hold_temporary_custody(
        self, incarceration_period: StateIncarcerationPeriod
    ) -> bool:
        return self._default_period_is_non_board_hold_temporary_custody(
            incarceration_period
        )

    def pre_processing_incarceration_period_admission_reason_map(
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> Optional[StateIncarcerationPeriodAdmissionReason]:
        return (
            self._default_pre_processing_incarceration_period_admission_reason_mapper(
                incarceration_period
            )
        )

    def pre_processing_relies_on_supervision_periods(self) -> bool:
        # TODO(#7441): Return True once we implement the US_ID IP pre-processing that
        #  relies on supervision periods
        return self._default_pre_processing_relies_on_supervision_periods()
