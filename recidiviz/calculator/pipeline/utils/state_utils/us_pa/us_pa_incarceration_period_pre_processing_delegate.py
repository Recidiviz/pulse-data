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
"""Contains state-specific logic for certain aspects of pre-processing US_PA
StateIncarcerationPeriod entities so that they are ready to be used in pipeline
calculations."""
from typing import Set

from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    StateSpecificIncarcerationPreProcessingDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)


class UsPaIncarcerationPreProcessingDelegate(
    StateSpecificIncarcerationPreProcessingDelegate
):
    # Functions with state-specific overrides
    # No deviations from default logic for US_PA

    # Functions using default behavior
    def should_collapse_transfers_different_purposes_for_incarceration(self) -> bool:
        return (
            self._default_should_collapse_transfers_different_purposes_for_incarceration()
        )

    def incarceration_types_to_filter(self) -> Set[StateIncarcerationType]:
        return self._default_incarceration_types_to_filter()

    def admission_reasons_to_filter(
        self,
    ) -> Set[StateIncarcerationPeriodAdmissionReason]:
        return self._default_admission_reasons_to_filter()
