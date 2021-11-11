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
"""US_ID implementation of the supervision pre-processing delegate"""
from typing import List, Optional

from recidiviz.calculator.pipeline.utils.period_utils import (
    find_last_terminated_period_before_date,
)
from recidiviz.calculator.pipeline.utils.supervision_period_pre_processing_manager import (
    StateSpecificSupervisionPreProcessingDelegate,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod

# We expect transition dates from supervision to incarceration to be fairly close
# together for the data in US_ID. We limit the search for a pre or post-incarceration
# supervision type to 1 month prior to admission or 1 month following release.
SUPERVISION_TYPE_LOOKBACK_MONTH_LIMIT = 1


class UsIdSupervisionPreProcessingDelegate(
    StateSpecificSupervisionPreProcessingDelegate
):
    """US_ID implementation of the supervision pre-processing delegate"""

    def supervision_admission_reason_override(
        self,
        supervision_period: StateSupervisionPeriod,
        supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriodAdmissionReason]:
        """Looks at the provided |supervision_period| and all supervision periods for
        this person and returns the (potentially updated) admission reason for this
        |supervision period|. This is necessary because ID periods that occur after
        investigation should be counted as newly court sentenced, as opposed to
        transfers.
        In order to determine if an override should occur, this method looks at the
        list of supervision periods to find the most recently
        non-null supervision period supervision type preceding the given
        |supervision_period|.
        """
        if not supervision_period.start_date:
            raise ValueError(
                "Found null supervision_period.start_date while getting admission "
                "reason override."
            )
        # Get the most recent, non-null previous supervision type.
        periods_with_set_supervision_types = [
            period
            for period in supervision_periods
            if period.supervision_type is not None
        ]

        most_recent_previous_period = find_last_terminated_period_before_date(
            upper_bound_date=supervision_period.start_date,
            periods=periods_with_set_supervision_types,
            maximum_months_proximity=SUPERVISION_TYPE_LOOKBACK_MONTH_LIMIT,
        )

        previous_supervision_type = (
            most_recent_previous_period.supervision_type
            if most_recent_previous_period
            else None
        )

        if (
            previous_supervision_type
            == StateSupervisionPeriodSupervisionType.INVESTIGATION
            and supervision_period.admission_reason
            == StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE
        ):
            return StateSupervisionPeriodAdmissionReason.COURT_SENTENCE
        return supervision_period.admission_reason
