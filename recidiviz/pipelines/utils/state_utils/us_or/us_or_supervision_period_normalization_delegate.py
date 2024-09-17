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
"""US_OR implementation of the StateSpecificSupervisionNormalizationDelegate."""
from typing import List, Optional

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)


class UsOrSupervisionNormalizationDelegate(
    StateSpecificSupervisionNormalizationDelegate
):
    """US_OR implementation of the StateSpecificSupervisionNormalizationDelegate."""

    def supervision_type_override(
        self,
        supervision_period_list_index: int,
        sorted_supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        """US_OR specific logic for determining supervision type of ABSCONSION from start_reasons of ABSCONSION,
        making this change only for open periods of absconsion since previous ones are recorded erroneously in OR ."""

        sp = sorted_supervision_periods[supervision_period_list_index]

        if supervision_period_list_index == 0 or sp.start_date is None:
            return sp.supervision_type

        if sp.admission_reason == StateSupervisionPeriodAdmissionReason.ABSCONSION:
            sp.supervision_type = StateSupervisionPeriodSupervisionType.ABSCONSION

        return sp.supervision_type
