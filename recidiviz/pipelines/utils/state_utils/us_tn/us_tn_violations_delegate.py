# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Contains US_TN implementation of the StateSpecificViolationDelegate."""
import datetime
from typing import List

from dateutil.relativedelta import relativedelta

from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.common.date import DateRange
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)


class UsTnViolationDelegate(StateSpecificViolationDelegate):
    """US_TN implementation of the StateSpecificViolationDelegate."""

    def violation_history_window_relevant_to_critical_date(
        self,
        critical_date: datetime.date,
        sorted_and_filtered_violation_responses: List[
            NormalizedStateSupervisionViolationResponse
        ],
        default_violation_history_window_months: int,
    ) -> DateRange:
        """For US_TN we look for violation responses with a response_date within 1 month after
        the incarceration period admission date (critical date). We set the lower bound to 24 months so we
        only attach violations that have happened within 24 months since the incarceration period admission date.
        """

        violation_window_lower_bound_inclusive = critical_date - relativedelta(
            months=24
        )
        violation_window_upper_bound_exclusive = critical_date + relativedelta(months=1)
        return DateRange(
            lower_bound_inclusive_date=violation_window_lower_bound_inclusive,
            upper_bound_exclusive_date=violation_window_upper_bound_exclusive,
        )

    def should_include_response_in_violation_history(
        self,
        response: NormalizedStateSupervisionViolationResponse,
        include_follow_up_responses: bool = False,
    ) -> bool:
        """For US_TN, we include all responses of type CITATION, VIOLATION_REPORT and PERMANENT_DECISION responses to
        be included in the violation history.
        """
        return response.response_type in (
            StateSupervisionViolationResponseType.VIOLATION_REPORT,
            StateSupervisionViolationResponseType.CITATION,
            StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )
