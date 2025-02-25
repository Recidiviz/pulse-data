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
"""US_TN implementation of the StateSpecificSupervisionNormalizationDelegate."""
import datetime
from typing import List, Optional

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)


class UsTnSupervisionNormalizationDelegate(
    StateSpecificSupervisionNormalizationDelegate
):
    """US_TN implementation of the StateSpecificSupervisionNormalizationDelegate."""

    def supervision_level_override(
        self,
        supervision_period_list_index: int,
        sorted_supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionLevel]:
        """US_TN specific logic for determining supervision level from previous supervision level when EXTERNAL_UNKNOWN is present
        and termination date is less than 31 days after start date. In TN, POs can "backdate" or "frontend" supervision level changes
        i.e. if someone's level changes on March 10, they could enter that as changing end of Feb or end of March.
        I believe this is why we're seeing lots of people who were released in March, but whose supervision level ended at the end of Feb.
        Because of this behavior, there is sometimes a gap between when someone's supervision level information ends and
        then their periods are actually closed out. This override makes sure that when EXTERNAL_UNKNOWN levels are written into those periods,
        we override the level to carry the previous level through so that the final termination at discharge matches the level someone was
        when they were discharged."""

        sp = sorted_supervision_periods[supervision_period_list_index]

        if supervision_period_list_index == 0 or sp.start_date is None:
            return sp.supervision_level

        previous_period = sorted_supervision_periods[supervision_period_list_index - 1]
        date_difference = abs(
            (sp.termination_date or datetime.date.today()) - sp.start_date
        ).days

        if (
            sp.supervision_level == StateSupervisionLevel.EXTERNAL_UNKNOWN
            and date_difference < 31
        ):
            return previous_period.supervision_level

        return sp.supervision_level

    def close_incorrectly_open_supervision_periods(
        self,
        # superivision_period_list_index: int,
        sorted_supervision_periods: List[StateSupervisionPeriod],
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        # incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        # incarceration_period: StateIncarcerationPeriod,
        # supervision_period: StateSupervisionPeriod,
    ) -> List[StateSupervisionPeriod]:
        """OffenderMovement is the source of truth logic for movements in TN but is not used in Supervision Periods
        View so there are a number of open Supervision Periods that need to be closed. We use Incarceration Periods
        that end with SENTENCE_SERVED and overlap with Supervision Periods to close these out as expected.
        """
        # if open supervision period, close

        if (
            len(sorted_supervision_periods) >= 1
            and len(sorted_incarceration_periods) >= 1
        ):
            last_supervision_period = sorted_supervision_periods[-1]
            last_incarceration_period = sorted_incarceration_periods[-1]

            if (
                last_incarceration_period.release_date is not None
                and last_supervision_period.start_date is not None
            ):
                if (
                    last_supervision_period.termination_date is None
                    and last_incarceration_period.release_reason
                    == StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED
                    and last_incarceration_period.release_date
                    > last_supervision_period.start_date
                ):
                    # Make last supervision period termination_date equal to last incarceration
                    # period end date
                    last_supervision_period.termination_date = (
                        last_incarceration_period.release_date
                    )

                    # Make TerminationReason discharge to close out period
                    last_supervision_period.termination_reason = (
                        StateSupervisionPeriodTerminationReason.DISCHARGE
                    )

                    # Make termination_reason_raw_text the release_reason_raw_text from the IP to understand why period is closed
                    last_supervision_period.termination_reason_raw_text = (
                        last_incarceration_period.release_reason_raw_text
                    )

                    sorted_supervision_periods[-1] = last_supervision_period

        return sorted_supervision_periods
