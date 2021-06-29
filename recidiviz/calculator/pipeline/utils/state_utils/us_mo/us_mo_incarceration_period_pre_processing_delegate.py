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
"""Contains state-specific logic for certain aspects of pre-processing US_MO
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
from recidiviz.common.str_field_utils import normalize
from recidiviz.ingest.direct.regions.us_mo import us_mo_enum_helpers
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class UsMoIncarcerationPreProcessingDelegate(
    StateSpecificIncarcerationPreProcessingDelegate
):
    """US_MO implementation of the StateSpecificIncarcerationPreProcessingDelegate."""

    # Functions with state-specific overrides
    def incarceration_types_to_filter(self) -> Set[StateIncarcerationType]:
        """US_MO drops all incarceration periods that aren't in a STATE_PRISON from
        calculations."""
        return {
            t
            for t in StateIncarcerationType
            if t != StateIncarcerationType.STATE_PRISON
        }

    def period_is_parole_board_hold(
        self, incarceration_period: StateIncarcerationPeriod
    ) -> bool:
        """In US_MO, we can infer that an incarceration period with an admission_reason
        of TEMPORARY_CUSTODY is a parole board hold if the period has a
        specialized_purpose_for_incarceration value of GENERAL.
        """
        return (
            incarceration_period.admission_reason
            == StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY
            and incarceration_period.specialized_purpose_for_incarceration
            == StateSpecializedPurposeForIncarceration.GENERAL
        )

    def period_is_non_board_hold_temporary_custody(
        self, incarceration_period: StateIncarcerationPeriod
    ) -> bool:
        """The only periods of temporary custody in US_MO are parole board holds."""
        return False

    # TODO(#7965): Use default behavior once we've done an ingest re-run for US_MO
    def pre_processing_incarceration_period_admission_reason_map(
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> Optional[StateIncarcerationPeriodAdmissionReason]:
        """We have updated our StateIncarcerationPeriodAdmissionReason
        enum-mappings for US_MO, and the changes require a re-run (are beyond the scope
        of a database migration). Until that re-run happens, we will be re-ingesting
        raw admission_reason_raw_text values and using the following logic to provide
        updated admission reason values."""
        if not incarceration_period.admission_reason_raw_text:
            return incarceration_period.admission_reason

        current_admission_reason = incarceration_period.admission_reason
        re_mapped_admission_reason = (
            us_mo_enum_helpers.incarceration_period_admission_reason_mapper(
                normalize(
                    incarceration_period.admission_reason_raw_text,
                    remove_punctuation=True,
                )
            )
        )

        # TODO(#7442): Handle double revocation admissions when normalizing commitment
        #  from supervision admissions in IP pre-processing
        if (
            current_admission_reason
            == StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY
            and re_mapped_admission_reason
            in (
                StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION,
                StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
                StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            )
        ):
            # This admission was previously classified as a board hold, but is now
            # getting cast as a revocation admission. We're not actually sure what's
            # going on here, so let's return INTERNAL_UNKNOWN to be safe.
            return StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN

        return re_mapped_admission_reason

    def pre_processing_relies_on_supervision_periods(self) -> bool:
        """IP pre-processing for US_MO does not rely on StateSupervisionPeriod
        entities."""
        return False

    # Functions using default behavior
    def admission_reasons_to_filter(
        self,
    ) -> Set[StateIncarcerationPeriodAdmissionReason]:
        return self._default_admission_reasons_to_filter()

    def normalize_period_if_commitment_from_supervision(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: Optional[PreProcessedSupervisionPeriodIndex],
    ) -> StateIncarcerationPeriod:
        return self._default_normalize_period_if_commitment_from_supervision(
            sorted_incarceration_periods[incarceration_period_list_index]
        )
