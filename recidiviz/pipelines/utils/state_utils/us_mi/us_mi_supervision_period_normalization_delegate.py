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
"""US_MI implementation of the StateSpecificSupervisionNormalizationDelegate."""
from typing import List, Optional

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.supervision_period_utils import SUCCESSFUL_TERMINATIONS


class UsMiSupervisionNormalizationDelegate(
    StateSpecificSupervisionNormalizationDelegate
):
    """US_MI implementation of the StateSpecificSupervisionNormalizationDelegate."""

    # TODO(#30367): Revisit and see if we can omit investigation periods in ingest directly
    def drop_bad_periods(
        self, sorted_supervision_periods: List[StateSupervisionPeriod]
    ) -> List[StateSupervisionPeriod]:
        periods_to_keep = []

        for sp in sorted_supervision_periods:
            # If supervision type = INVESTIGATION, let's drop
            if (
                sp.supervision_type
                == StateSupervisionPeriodSupervisionType.INVESTIGATION
            ):
                continue

            periods_to_keep.append(sp)

        return periods_to_keep

    # TODO(#30399) Revisit to see if we can do this in ingest directly
    def supervision_level_override(
        self,
        supervision_period_list_index: int,
        sorted_supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionLevel]:
        """
        For any supervision period of time where supervision level is missing, the period doesn't end in discharge,
        override the supervision level and set it to IN_CUSTODY.  We've validated with trusted testers that when
        we see a supervision period with a missing supervision level, it is usually because the person is in custody.
        """

        sp = sorted_supervision_periods[supervision_period_list_index]

        if (
            sp.supervision_level_raw_text is None
            and sp.supervision_level is None
            and sp.termination_reason not in SUCCESSFUL_TERMINATIONS
        ):
            return StateSupervisionLevel.IN_CUSTODY

        return sp.supervision_level
