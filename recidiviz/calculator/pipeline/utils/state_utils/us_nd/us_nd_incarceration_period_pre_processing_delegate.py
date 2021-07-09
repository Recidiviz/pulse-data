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

import attr

from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    PurposeForIncarcerationInfo,
    StateSpecificIncarcerationPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.incarceration_period_utils import (
    incarceration_periods_with_admissions_between_dates,
)
from recidiviz.calculator.pipeline.utils.period_utils import (
    find_last_terminated_period_before_date,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
    StateSupervisionViolationResponse,
)

# The number of months for the window of time prior to a new admission return to search
# for a previous probation supervision period that ended due to revocation to which we
# may attribute this commitment from supervision
_NEW_ADMISSION_PROBATION_COMMITMENT_LOOKBACK_MONTHS = 24


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

    def normalize_period_if_commitment_from_supervision(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: Optional[PreProcessedSupervisionPeriodIndex],
    ) -> StateIncarcerationPeriod:
        return _us_nd_normalize_period_if_commitment_from_supervision(
            incarceration_period_list_index=incarceration_period_list_index,
            sorted_incarceration_periods=sorted_incarceration_periods,
            supervision_period_index=supervision_period_index,
        )

    def period_is_parole_board_hold(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
    ) -> bool:
        """There are no parole board hold incarceration periods in US_ND."""
        if self._default_period_is_parole_board_hold(
            incarceration_period_list_index, sorted_incarceration_periods
        ):
            raise ValueError(
                "Unexpected "
                "StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD "
                "value in US_ND. We do not expect any parole board hold "
                "periods for this state."
            )
        return False

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
        """The apply_commitment_from_supervision_period_overrides function for US_ND
        relies on supervision period entities."""
        return True

    # Functions using default behavior
    def get_pfi_info_for_period_if_commitment_from_supervision(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        violation_responses: Optional[List[StateSupervisionViolationResponse]],
    ) -> PurposeForIncarcerationInfo:
        return self._default_get_pfi_info_for_period_if_commitment_from_supervision(
            incarceration_period_list_index,
            sorted_incarceration_periods,
            violation_responses,
        )

    def incarceration_types_to_filter(self) -> Set[StateIncarcerationType]:
        return self._default_incarceration_types_to_filter()

    def period_is_non_board_hold_temporary_custody(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
    ) -> bool:
        return self._default_period_is_non_board_hold_temporary_custody(
            incarceration_period_list_index,
            sorted_incarceration_periods,
        )

    def pre_processing_relies_on_violation_responses(self) -> bool:
        return self._default_pre_processing_relies_on_violation_responses()


def _us_nd_normalize_period_if_commitment_from_supervision(
    incarceration_period_list_index: int,
    sorted_incarceration_periods: List[StateIncarcerationPeriod],
    supervision_period_index: Optional[PreProcessedSupervisionPeriodIndex],
) -> StateIncarcerationPeriod:
    """Returns an updated version of the specified incarceration period if it is a
    commitment from supervision admission.

    Updates the admission_reason to be a PROBATION_REVOCATION or a PAROLE_REVOCATION
    for the following scenarios:
        - The person was admitted to a state prison due to a NEW_ADMISSION (not a period
      of TEMPORARY_CUSTODY in a county jail) and was previously in a PAROLE or
          PROBATION supervision period that terminated due to a REVOCATION.

    Also note that in this case, we also ensure that there was not an intermediate
    period of incarceration in a state prison between the supervision REVOCATION and
    this incarceration period under examination, to make sure we do not mistakenly
    re-classify what is truly a NEW_ADMISSION as a REVOCATION.
    """
    if supervision_period_index is None:
        raise ValueError(
            "IP pre-processing relies on supervision periods for US_ND. "
            "Expected non-null supervision_period_index."
        )

    incarceration_period = sorted_incarceration_periods[incarceration_period_list_index]

    admission_date = incarceration_period.admission_date

    if not admission_date:
        raise ValueError(f"Admission date for null for {incarceration_period}")

    admission_reason = incarceration_period.admission_reason
    if admission_reason == StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION:

        most_recent_supervision_period = find_last_terminated_period_before_date(
            upper_bound_date=admission_date,
            periods=supervision_period_index.supervision_periods,
            maximum_months_proximity=_NEW_ADMISSION_PROBATION_COMMITMENT_LOOKBACK_MONTHS,
        )

        if (
            most_recent_supervision_period
            and most_recent_supervision_period.termination_reason
            == StateSupervisionPeriodTerminationReason.REVOCATION
            and most_recent_supervision_period.supervision_type
            in (StateSupervisionType.PAROLE, StateSupervisionType.PROBATION)
        ):
            was_intermediate_state_prison_admission = (
                _intermediate_state_prison_admission(
                    most_recent_supervision_period,
                    incarceration_period,
                    sorted_incarceration_periods,
                )
            )

            # If there was a separate admission to a state prison after the revocation
            # and before this admission, then it is not accurate to describe *this*
            # admission as being due to a revocation
            if was_intermediate_state_prison_admission:
                return incarceration_period

            return attr.evolve(
                incarceration_period,
                admission_reason=(
                    StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION
                    if most_recent_supervision_period.supervision_type
                    == StateSupervisionType.PROBATION
                    else StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION
                ),
            )

    # This period does not require any updated values
    return incarceration_period


def _intermediate_state_prison_admission(
    most_recent_supervision_period: StateSupervisionPeriod,
    incarceration_period: StateIncarcerationPeriod,
    incarceration_periods: List[StateIncarcerationPeriod],
) -> bool:
    """Returns whether or not there was an admission to a state prison after the most
    recent supervision period ended and before the given incarceration period started,
    inclusive of the supervision period termination date and exclusive of the
    incarceration period admission date."""
    start_date = most_recent_supervision_period.termination_date
    end_date = incarceration_period.admission_date

    if start_date is None or end_date is None:
        raise ValueError(
            "Expected a supervision period termination date and an incarceration period admission date at this point. "
            f"Termination date: [{start_date}]. Admission date: [{end_date}]."
        )

    intermediate_incarceration_periods = (
        incarceration_periods_with_admissions_between_dates(
            incarceration_periods, start_date, end_date
        )
    )

    return any(
        ip.incarceration_type == StateIncarcerationType.STATE_PRISON
        for ip in intermediate_incarceration_periods
    )
