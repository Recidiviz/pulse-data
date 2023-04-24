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

from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod


class UsTnSupervisionNormalizationDelegate(
    StateSpecificSupervisionNormalizationDelegate
):
    """US_TN implementation of the StateSpecificSupervisionNormalizationDelegate."""

    # TODO(#12028): Delete this when TN ingest rerun has eliminated the bad
    #  periods with dates of 9999-12-31.
    def drop_bad_unmodified_periods(
        self, supervision_periods: List[StateSupervisionPeriod]
    ) -> List[StateSupervisionPeriod]:
        return [
            sp
            for sp in supervision_periods
            if sp.termination_date != datetime.date(9999, 12, 31)
        ]

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
