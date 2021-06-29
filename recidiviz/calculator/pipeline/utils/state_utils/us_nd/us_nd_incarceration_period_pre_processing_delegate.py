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
"""Contains state-specific logic for certain aspects of pre-processing US_ND
StateIncarcerationPeriod entities so that they are ready to be used in pipeline
calculations."""
from typing import List, Optional, Set

from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    StateSpecificIncarcerationPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class UsNdIncarcerationPreProcessingDelegate(
    StateSpecificIncarcerationPreProcessingDelegate
):
    """US_ND implementation of the StateSpecificIncarcerationPreProcessingDelegate."""

    # Functions with state-specific overrides
    def admission_reasons_to_filter(
        self,
    ) -> Set[StateIncarcerationPeriodAdmissionReason]:
        """US_ND drops all admissions to temporary custody periods from the
        calculations."""
        return {StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY}

    def period_is_parole_board_hold(
        self, incarceration_period: StateIncarcerationPeriod
    ) -> bool:
        """There are no parole board hold incarceration periods in US_ND."""
        if (
            incarceration_period.specialized_purpose_for_incarceration
            == StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
        ):
            raise ValueError(
                "Unexpected "
                "StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD "
                "value in US_ND. We do not expect any parole board hold "
                "periods for this state."
            )
        return False

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
        # TODO(#7441): Return True once we implement the US_ND IP pre-processing that
        #  relies on supervision periods
        return self._default_pre_processing_relies_on_supervision_periods()

    # Functions using default behavior
    def normalize_period_if_commitment_from_supervision(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: Optional[PreProcessedSupervisionPeriodIndex],
    ) -> StateIncarcerationPeriod:
        # TODO(#7441): Implement overrides for US_ND
        return self._default_normalize_period_if_commitment_from_supervision(
            sorted_incarceration_periods[incarceration_period_list_index]
        )

    def incarceration_types_to_filter(self) -> Set[StateIncarcerationType]:
        return self._default_incarceration_types_to_filter()

    def period_is_non_board_hold_temporary_custody(
        self, incarceration_period: StateIncarcerationPeriod
    ) -> bool:
        return self._default_period_is_non_board_hold_temporary_custody(
            incarceration_period
        )
