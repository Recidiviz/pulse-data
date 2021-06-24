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
from typing import Optional, Set

from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    StateSpecificIncarcerationPreProcessingDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.str_field_utils import normalize
from recidiviz.ingest.direct.regions.us_pa import us_pa_enum_helpers
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class UsPaIncarcerationPreProcessingDelegate(
    StateSpecificIncarcerationPreProcessingDelegate
):
    """US_PA implementation of the StateSpecificIncarcerationPreProcessingDelegate."""

    # Functions with state-specific overrides
    # TODO(#7222): Use default behavior once we've done an ingest re-run for US_PA
    def pre_processing_incarceration_period_admission_reason_map(
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> Optional[StateIncarcerationPeriodAdmissionReason]:
        """We have updated our StateIncarcerationPeriodAdmissionReason
        enum-mappings for US_PA, and the changes require a re-run (are beyond the scope
        of a database migration). Until that re-run happens, we will be re-ingesting
        raw admission_reason_raw_text values and using the following logic to provide
        updated admission reason values."""
        if not incarceration_period.admission_reason_raw_text:
            return incarceration_period.admission_reason
        return us_pa_enum_helpers.incarceration_period_admission_reason_mapper(
            normalize(
                incarceration_period.admission_reason_raw_text, remove_punctuation=True
            )
        )

    # Functions using default behavior
    def incarceration_types_to_filter(self) -> Set[StateIncarcerationType]:
        return self._default_incarceration_types_to_filter()

    def admission_reasons_to_filter(
        self,
    ) -> Set[StateIncarcerationPeriodAdmissionReason]:
        return self._default_admission_reasons_to_filter()

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

    def pre_processing_relies_on_supervision_periods(self) -> bool:
        # TODO(#7441): Return True once we implement the US_PA IP pre-processing that
        #  relies on supervision periods
        return self._default_pre_processing_relies_on_supervision_periods()
