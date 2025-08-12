# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""US_NE implementation of the StateSpecificSupervisionNormalizationDelegate."""
from typing import List

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)


class UsNeSupervisionNormalizationDelegate(
    StateSpecificSupervisionNormalizationDelegate
):
    """US_NE implementation of the StateSpecificSupervisionNormalizationDelegate."""

    def normalize_subsequent_absconsion_periods(
        self,
        sorted_supervision_periods: List[StateSupervisionPeriod],
    ) -> List[StateSupervisionPeriod]:
        """People can have many case officer changes after absconding. This logic is
        to carry down the supervision_type of ABSCONSION to all subsequent
        supervision periods until there is any type of movement that is not transfer
        within state, since all other end movements are return from absconsion or end to supervision.
        """

        updated_supervision_periods: List[StateSupervisionPeriod] = []
        in_absconsion = False

        for sp in sorted_supervision_periods:
            # Enter absconsion mode if this period is ABSCONSION
            if sp.supervision_type == StateSupervisionPeriodSupervisionType.ABSCONSION:
                in_absconsion = True
                updated_supervision_periods.append(sp)
                continue

            # While in absconsion, overwrite supervision_type
            if in_absconsion:
                sp.supervision_type = StateSupervisionPeriodSupervisionType.ABSCONSION

                if (
                    sp.termination_reason
                    != StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE
                ):
                    in_absconsion = False

            updated_supervision_periods.append(sp)

        return updated_supervision_periods
