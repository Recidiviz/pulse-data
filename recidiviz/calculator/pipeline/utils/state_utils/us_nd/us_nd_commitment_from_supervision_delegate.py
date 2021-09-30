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
from typing import List, Optional, Set

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.state_utils.state_specific_commitment_from_supervision_delegate import (
    StateSpecificCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.supervision_type_identification import (
    get_pre_incarceration_supervision_type_from_ip_admission_reason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.date import DateRange
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolationResponse,
)


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

    def get_commitment_from_supervision_supervision_type(
        self,
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_period: StateIncarcerationPeriod,
        previous_supervision_period: Optional[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        """Determines the supervision type for the given supervision period that
        preceded the given incarceration period that represents a commitment from
        supervision.

        As noted in UsNdIncarcerationPreProcessingDelegate one of the ways in which a
        commitment from supervision can occur in US_ND is when a NEW_ADMISSION
        incarceration period directly follows a PROBATION supervision period
        that ended due to a REVOCATION. As such, we handle that case here by specifying
        a supervision type of PROBATION in this case.

        Note that this refers specifically to PROBATION and does not include PAROLE
        because 1) we think that PAROLE followed by NEW ADMISSION is not actually
        reasonably interpretable as a parole revocation based on how probation and
        parole are administered on the ground, and 2) we don’t have mass examples of
        NEW_ADMISSION incarceration directly following PAROLE in the data like we do
        for PROBATION.
        """
        if not incarceration_period.admission_reason:
            raise ValueError(
                "Unexpected missing admission_reason on incarceration period: "
                f"[{incarceration_period}]"
            )

        default_supervision_type = (
            get_pre_incarceration_supervision_type_from_ip_admission_reason(
                incarceration_period.admission_reason
            )
        )

        if default_supervision_type:
            return default_supervision_type

        if (
            previous_supervision_period
            and previous_supervision_period.supervision_period_supervision_type
            == StateSupervisionPeriodSupervisionType.PROBATION
        ):
            return StateSupervisionPeriodSupervisionType.PROBATION

        return None
