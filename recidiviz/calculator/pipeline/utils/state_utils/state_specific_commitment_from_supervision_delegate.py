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
"""Contains the StateSpecificCommitmentFromSupervisionDelegate, the interface
for state-specific decisions involved in categorizing various attributes of
commitment from supervision admissions."""
import abc
import datetime
from typing import List, Optional, Set

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.calculator.pipeline.utils.violation_response_utils import (
    violation_responses_in_window,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.date import DateRange


class StateSpecificCommitmentFromSupervisionDelegate(abc.ABC, StateSpecificDelegate):
    """Interface for state-specific decisions involved in categorizing various
    attributes of commitment from supervision admissions."""

    def should_filter_to_matching_supervision_types_in_pre_commitment_sp_search(
        self,
    ) -> bool:
        """Whether or not we should only look at supervision periods where the
        supervision type matches the type of supervision that ended due to the
        commitment admission as indicated by the admission_reason.

        Default behavior is look at any supervision period, regardless of type.
        Should be overridden by state-specific implementations if necessary.
        """
        return False

    def admission_reason_raw_texts_that_should_prioritize_overlaps_in_pre_commitment_sp_search(
        self,
    ) -> Set[str]:
        """Returns the set of commitment from supervision admission reason raw texts for which
        we should prioritize periods that *overlap* with the date of admission to
        incarceration, as opposed to prioritizing periods that have already terminated
        by the date of admission.

        Default behavior is always prioritizing periods that have terminated prior to
        the admission. Should be overridden by state-specific implementations if
        necessary.

        A state may want to override this if supervision periods are habitually
        terminated after commitment periods begin.
        """

        return set()

    def violation_history_window_pre_commitment_from_supervision(
        self,
        admission_date: datetime.date,
        sorted_and_filtered_violation_responses: List[
            NormalizedStateSupervisionViolationResponse
        ],
        default_violation_history_window_months: int,
    ) -> DateRange:
        """Returns the window of time before a commitment from supervision in which we
        should consider violations for the violation history prior to the admission.

        Default behavior is to use the date of the last violation response recorded
        prior to the |admission_date| as the upper bound of the window, with a lower
        bound that is |default_violation_history_window_months| before that date.

        Should be overridden by state-specific implementations if necessary.
        """
        # We will use the date of the last response prior to the admission as the
        # window cutoff.
        responses_before_admission = violation_responses_in_window(
            violation_responses=sorted_and_filtered_violation_responses,
            upper_bound_exclusive=admission_date + relativedelta(days=1),
            lower_bound_inclusive=None,
        )

        violation_history_end_date = admission_date

        if responses_before_admission:
            # If there were violation responses leading up to the incarceration
            # admission, then we want the violation history leading up to the last
            # response_date instead of the admission_date on the
            # incarceration_period
            last_response = responses_before_admission[-1]

            if not last_response.response_date:
                # This should never happen, but is here to silence mypy warnings
                # about empty response_dates.
                raise ValueError(
                    "Not effectively filtering out responses without valid"
                    " response_dates."
                )
            violation_history_end_date = last_response.response_date

        violation_window_lower_bound_inclusive = (
            violation_history_end_date
            - relativedelta(months=default_violation_history_window_months)
        )
        violation_window_upper_bound_exclusive = (
            violation_history_end_date + relativedelta(days=1)
        )

        return DateRange(
            lower_bound_inclusive_date=violation_window_lower_bound_inclusive,
            upper_bound_exclusive_date=violation_window_upper_bound_exclusive,
        )

    # pylint: disable=unused-argument
    def get_commitment_from_supervision_supervision_type(
        self,
        incarceration_period: NormalizedStateIncarcerationPeriod,
        previous_supervision_period: Optional[NormalizedStateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        """Returns the supervision type the person was on before they were committed to
        incarceration from supervision.

        Default behavior is to return the supervision_type on the
        |previous_supervision_period|, if provided, else infers the supervision type
        from the admission_reason on the |incarceration_period|.

        Should be overridden by state-specific implementations if necessary.
        """
        if previous_supervision_period and previous_supervision_period.supervision_type:
            return previous_supervision_period.supervision_type
        return None

    def get_pre_incarceration_supervision_type_from_ip_admission_reason(
        self,
        admission_reason: StateIncarcerationPeriodAdmissionReason,
        admission_reason_raw_text: Optional[str],
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        """Derives the supervision type the person was serving prior to being
        admitted to incarceration with the given |admission_reason| and |admission_reason_raw_text|.

        This method should only be called if should_filter_to_matching_supervision_types_in_pre_commitment_sp_search
        is True.

        A state specific override must be implemented.
        """

        raise ValueError(
            "This method should only be called if "
            "`should_filter_to_matching_supervision_types_in_pre_commitment_sp_search` returns True. State"
            "specific override must be implemented."
        )
