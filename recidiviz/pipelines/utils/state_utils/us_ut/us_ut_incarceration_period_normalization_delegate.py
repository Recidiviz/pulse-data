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
"""Contains US_UT implementation of the StateSpecificIncarcerationNormalizationDelegate."""
from typing import List

from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.pipelines.utils.incarceration_period_utils import (
    infer_incarceration_periods_from_in_custody_sps,
)


class UsUtIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_UT implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    def infer_additional_periods(
        self,
        person_id: int,
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: NormalizedSupervisionPeriodIndex,
    ) -> List[StateIncarcerationPeriod]:
        """
        In UT, we'll infer additional incarceration periods when a person is on supervision
        but has a supervision level that denotes they are in custody.
        """

        # Infer an incarceration period for any period of time where
        # supervision level = IN_CUSTODY and there's not already an incarceration period during that time
        new_incarceration_periods = infer_incarceration_periods_from_in_custody_sps(
            person_id=person_id,
            state_code=StateCode.US_UT,
            incarceration_periods=incarceration_periods,
            supervision_period_index=supervision_period_index,
            temp_custody_custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
        )

        return new_incarceration_periods
