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
"""Utils for state-specific logic related to identifying commitments from
supervision in US_ND."""
import datetime
from typing import List, Optional, Set, Tuple

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.commitment_from_supervision_utils import (
    StateSpecificCommitmentFromSupervisionDelegate,
    get_commitment_from_supervision_supervision_period,
)
from recidiviz.calculator.pipeline.utils.period_utils import (
    find_last_terminated_period_before_date,
)
from recidiviz.calculator.pipeline.utils.pre_processed_incarceration_period_index import (
    PreProcessedIncarcerationPeriodIndex,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    is_commitment_from_supervision,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.date import DateRange
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
    StateSupervisionViolationResponse,
)

# The number of months for the window of time prior to a new admission return to search
# for a previous probation supervision period that ended due to revocation to which we
# may attribute this commitment from supervision
_NEW_ADMISSION_PROBATION_COMMITMENT_LOOKBACK_MONTHS = 24


class UsNdCommitmentFromSupervisionDelegate(
    StateSpecificCommitmentFromSupervisionDelegate
):
    """US_ND implementation of the StateSpecificCommitmentFromSupervisionDelegate."""

    def should_filter_to_matching_supervision_types_in_pre_commitment_sp_search(
        self,
    ) -> bool:
        """The US_ND schema allows for overlapping supervision periods, and it's
        possible for a person to have a supervision period of one supervision type end
        while a supervision period of another type stays active during their time in
        prison. A commitment from supervision admission should always be attributed
        to a supervision period with a supervision type matching the
        supervision type encoded in the admission reason.

        For parole revocations we want to look for supervision periods that are
        of type PAROLE, and for probation revocations we want to look for supervision
        periods that are of type PROBATION.
        """
        return True

    def admission_reasons_that_should_prioritize_overlaps_in_pre_commitment_sp_search(
        self,
    ) -> Set[StateIncarcerationPeriodAdmissionReason]:
        """In US_ND there are different expectations for when a supervision period
        will be terminated relative to the date of a commitment from
        supervision admission based on the |admission_reason| on the commitment.
        """
        # We prioritize periods that are overlapping with the |admission_date|,
        # of PAROLE periods since we don't expect parole periods to be terminated at the
        # time of a parole revocation admission

        # However, for PROBATION, we prioritize periods that have terminated before the
        # |admission_date|, since we expect probation periods to be terminated at the
        # time of a probation revocation admission
        return {StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION}

    def violation_history_window_pre_commitment_from_supervision(
        self,
        admission_date: datetime.date,
        sorted_and_filtered_violation_responses: List[
            StateSupervisionViolationResponse
        ],
        default_violation_history_window_months: int,
    ) -> DateRange:
        """For US_ND we look for violation responses with a response_date within 90 days
        of a commitment from supervision admission to incarceration. 90 days is an
        arbitrary buffer for which we accept discrepancies between the
        SupervisionViolationResponse response_date and the StateIncarcerationPeriod's
        admission_date.
        """

        violation_window_lower_bound_inclusive = admission_date - relativedelta(days=90)
        violation_window_upper_bound_exclusive = admission_date + relativedelta(days=90)
        return DateRange(
            lower_bound_inclusive_date=violation_window_lower_bound_inclusive,
            upper_bound_exclusive_date=violation_window_upper_bound_exclusive,
        )


def us_nd_pre_commitment_supervision_period_if_commitment(
    incarceration_period: StateIncarcerationPeriod,
    supervision_periods: List[StateSupervisionPeriod],
    incarceration_period_index: PreProcessedIncarcerationPeriodIndex,
) -> Tuple[bool, Optional[StateSupervisionPeriod]]:
    """Determines whether the incarceration_period started because of a commitment from
    supervision. If a commitment from supervision did occur, this finds the supervision
    period from which the person was committed.

    For US_ND, commitments from supervision occur in the following circumstances:
        - The person was admitted to a state prison due to some kind of REVOCATION
          admission type.
        - The person was admitted to a state prison due to a NEW_ADMISSION (not a period
          of TEMPORARY_CUSTODY in a county jail) and was previously in a PROBATION
          supervision period that terminated due to a REVOCATION.

    Note that the case above that refers specifically to PROBATION does not include
    PAROLE supervision periods because 1) we think that PAROLE followed by NEW ADMISSION
    is not actually reasonably interpretable as a parole revocation based on how
    probation and parole are administered on the ground, i.e. probation is administered
    differently and might habitually result in these particular data consistencies to
    be dealt with here, and 2) we don’t have mass examples of this happening in the data
    for PAROLE like we do for PROBATION.

    Also note that in this PROBATION case, we also ensure that there was not an
    intermediate period of incarceration in a state prison between the PROBATION
    REVOCATION and this incarceration period under examination, to make sure we do not
    mistakenly re-classify what is truly a NEW_ADMISSION as a PROBATION_REVOCATION.

    Args:
        - incarceration_period: The StateIncarcerationPeriod being evaluated for an
          instance of commitment from supervision.
        - supervision_periods: A list of the person's StateSupervisionPeriods
          that may be relevant as a commitment.
        - incarceration_period_index: The index of StateIncarcerationPeriods for this
          person, to check if previous incarceration periods impact this commitment.
    Returns:
        A tuple in the format [bool, Optional[StateSupervisionPeriod]] representing
        whether or not a commitment from supervision occurred and, if so, the
        supervision period from which the commitment occurred if it can be identified.
        It is not possible for this function to return True, None.
    """
    admission_date = incarceration_period.admission_date
    admission_reason = incarceration_period.admission_reason

    if not admission_date:
        raise ValueError(f"Admission date for null for {incarceration_period}")

    if not admission_reason:
        raise ValueError(f"Admission reason for null for {incarceration_period}")

    admission_is_commitment = is_commitment_from_supervision(admission_reason)

    if admission_is_commitment:
        pre_commitment_supervision_period = get_commitment_from_supervision_supervision_period(
            incarceration_period=incarceration_period,
            supervision_periods=supervision_periods,
            commitment_from_supervision_delegate=UsNdCommitmentFromSupervisionDelegate(),
            incarceration_period_index=incarceration_period_index,
        )

        return admission_is_commitment, pre_commitment_supervision_period

    if (
        incarceration_period.admission_reason
        == StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION
    ):
        most_recent_supervision_period = find_last_terminated_period_before_date(
            upper_bound_date=admission_date,
            periods=supervision_periods,
            maximum_months_proximity=_NEW_ADMISSION_PROBATION_COMMITMENT_LOOKBACK_MONTHS,
        )

        if (
            most_recent_supervision_period
            and most_recent_supervision_period.termination_reason
            == StateSupervisionPeriodTerminationReason.REVOCATION
            and most_recent_supervision_period.supervision_type
            == StateSupervisionType.PROBATION
        ):
            was_intermediate_state_prison_admission = (
                _intermediate_state_prison_admission(
                    most_recent_supervision_period,
                    incarceration_period,
                    incarceration_period_index,
                )
            )

            # If there was a separate admission to a state prison after the revocation
            # and before this admission, then it is not accurate to describe *this*
            # admission as being due to a revocation
            if was_intermediate_state_prison_admission:
                return False, None

            return True, most_recent_supervision_period

    return False, None


def _intermediate_state_prison_admission(
    most_recent_supervision_period: StateSupervisionPeriod,
    incarceration_period: StateIncarcerationPeriod,
    incarceration_period_index: PreProcessedIncarcerationPeriodIndex,
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
        incarceration_period_index.incarceration_periods_with_admissions_between_dates(
            start_date, end_date
        )
    )

    return any(
        ip.incarceration_type == StateIncarcerationType.STATE_PRISON
        for ip in intermediate_incarceration_periods
    )
