# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Contains US_IX implementation of the StateSpecificViolationDelegate."""

import datetime
from typing import List

from dateutil.relativedelta import relativedelta

from recidiviz.common.date import DateRange
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)


class UsIxViolationDelegate(StateSpecificViolationDelegate):
    """US_IX implementation of the StateSpecificViolationDelegate."""

    def violation_history_window_relevant_to_critical_date(
        self,
        critical_date: datetime.date,
        sorted_and_filtered_violation_responses: List[
            NormalizedStateSupervisionViolationResponse
        ],
        default_violation_history_window_months: int,
    ) -> DateRange:
        """For US_IX we look for violation responses up to 14 days after and 24 months before the admission_date for the
        incarceration period (critical date). We set the lower bound to 24 months so we only attach violations
        that have happened within 24 months since the incarceration period admission date.
        """

        violation_window_lower_bound_inclusive = critical_date - relativedelta(
            months=24
        )
        violation_window_upper_bound_exclusive = critical_date + relativedelta(days=14)
        return DateRange(
            lower_bound_inclusive_date=violation_window_lower_bound_inclusive,
            upper_bound_exclusive_date=violation_window_upper_bound_exclusive,
        )
