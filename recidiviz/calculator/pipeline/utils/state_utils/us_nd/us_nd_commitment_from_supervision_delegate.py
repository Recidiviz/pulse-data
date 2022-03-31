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

from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_commitment_from_supervision_delegate import (
    StateSpecificCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_incarceration_period_normalization_delegate import (
    PAROLE_REVOCATION_NORMALIZED_PREFIX,
    PREVIOUS_SUPERVISION_TYPE_TO_INCARCERATION_ADMISSION_REASON_RAW_TEXT,
    PROBATION_REVOCATION_NORMALIZED_PREFIX,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    is_commitment_from_supervision,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.date import DateRange


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

    def admission_reason_raw_texts_that_should_prioritize_overlaps_in_pre_commitment_sp_search(
        self,
    ) -> Set[str]:
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

        # Filter dictionary by keeping admission reason raw texts whose associated supervision types are PAROLE.
        filtered_admission_raw_texts: Set[str] = {
            key
            for (
                key,
                value,
            ) in PREVIOUS_SUPERVISION_TYPE_TO_INCARCERATION_ADMISSION_REASON_RAW_TEXT.items()
            if value == StateSupervisionPeriodSupervisionType.PAROLE
        }

        return filtered_admission_raw_texts

    def violation_history_window_pre_commitment_from_supervision(
        self,
        admission_date: datetime.date,
        sorted_and_filtered_violation_responses: List[
            NormalizedStateSupervisionViolationResponse
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
        incarceration_period: NormalizedStateIncarcerationPeriod,
        previous_supervision_period: Optional[NormalizedStateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        """Determines the supervision type for the given supervision period that
        preceded the given incarceration period that represents a commitment from
        supervision.
        """
        if not incarceration_period.admission_reason:
            raise ValueError(
                "Unexpected missing admission_reason on incarceration period: "
                f"[{incarceration_period}]"
            )

        default_supervision_type = (
            self.get_pre_incarceration_supervision_type_from_ip_admission_reason(
                incarceration_period.admission_reason,
                incarceration_period.admission_reason_raw_text,
            )
        )

        return default_supervision_type

    def get_pre_incarceration_supervision_type_from_ip_admission_reason(
        self,
        admission_reason: StateIncarcerationPeriodAdmissionReason,
        admission_reason_raw_text: Optional[str],
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        if (
            not is_commitment_from_supervision(admission_reason)
            or admission_reason_raw_text is None
        ):
            # All incarceration periods at by this point must be commitments from supervision.
            raise ValueError(
                f"Enum case not handled for admission reason raw text: {admission_reason_raw_text} and admission"
                f" reason {admission_reason}"
            )

        # If a key in PREVIOUS_SUPERVISION_TYPE_TO_INCARCERATION_ADMISSION_REASON_RAW_TEXT is present within the
        # admission_reason_raw_text.
        supervision_type_matched_with_raw_text = [
            val
            for key, val in PREVIOUS_SUPERVISION_TYPE_TO_INCARCERATION_ADMISSION_REASON_RAW_TEXT.items()
            if key == admission_reason_raw_text
            or (
                key
                in (
                    PAROLE_REVOCATION_NORMALIZED_PREFIX,
                    PROBATION_REVOCATION_NORMALIZED_PREFIX,
                )
                and admission_reason_raw_text.startswith(key)
            )
        ]

        # If there is exactly one match with the admission reason raw text, return the supervision type associated.
        if len(supervision_type_matched_with_raw_text) == 1:
            return supervision_type_matched_with_raw_text[0]

        # If there are too many or too few supervision type matches for the admission reason raw text, raise an error.
        if len(supervision_type_matched_with_raw_text) > 1:
            raise ValueError(
                f"Admission reason raw text: {admission_reason_raw_text} matched with multiple"
                f" supervision types: {str(supervision_type_matched_with_raw_text)}"
            )
        raise ValueError(
            f"Enum case not handled for admission reason raw text: {admission_reason_raw_text} and admission"
            f" reason {admission_reason}"
        )
