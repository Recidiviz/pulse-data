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
"""Utils for state-specific logic related to identifying violations in US_ND."""
import datetime
from typing import List, Optional

from dateutil.relativedelta import relativedelta

from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.common.date import DateRange
from recidiviz.persistence.entity.activity.normalized_entities import (
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)


class UsNdViolationDelegate(StateSpecificViolationDelegate):
    """US_ND implementation of the StateSpecificViolationsDelegate."""

    def should_include_response_in_violation_history(
        self,
        response: NormalizedStateSupervisionViolationResponse,
        include_follow_up_responses: bool = False,
    ) -> bool:
        """For US_ND we only include responses of type PERMANENT_DECISION."""
        return (
            response.response_type
            == StateSupervisionViolationResponseType.PERMANENT_DECISION
        )

    def violation_history_window_relevant_to_critical_date(
        self,
        critical_date: datetime.date,
        sorted_and_filtered_violation_responses: List[
            NormalizedStateSupervisionViolationResponse
        ],
        default_violation_history_window_months: int,
    ) -> DateRange:
        """For US_ND we look for violation responses with a response_date within 90 days
        of a critical date. 90 days is an arbitrary buffer for which we accept discrepancies between the
        SupervisionViolationResponse response_date and the StateIncarcerationPeriod's
        admission_date.
        """

        violation_window_lower_bound_inclusive = critical_date - relativedelta(days=90)
        violation_window_upper_bound_exclusive = critical_date + relativedelta(days=90)
        return DateRange(
            lower_bound_inclusive_date=violation_window_lower_bound_inclusive,
            upper_bound_exclusive_date=violation_window_upper_bound_exclusive,
        )

    def additional_violation_history_windows_relevant_to_critical_date(
        self,
        critical_date: datetime.date,
        sorted_and_filtered_violation_responses: List[
            NormalizedStateSupervisionViolationResponse
        ],
        default_violation_history_window_months: int,
        termination_date_of_preceding_supervision_period: Optional[datetime.date],
    ) -> List[DateRange]:
        """For US_ND we also look for violation responses within 90 days of the end
        of the supervision period that preceded the admission, if there was one. This
        catches violations that were recorded around the time a supervision case
        closed even when the person was not readmitted to incarceration until much
        later (e.g. an absconsion violation recorded at case closure, followed by a
        new-crime incarceration admission long afterward).
        """
        if termination_date_of_preceding_supervision_period is None:
            return []

        return [
            self.violation_history_window_relevant_to_critical_date(
                critical_date=termination_date_of_preceding_supervision_period,
                sorted_and_filtered_violation_responses=sorted_and_filtered_violation_responses,
                default_violation_history_window_months=default_violation_history_window_months,
            )
        ]
