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
"""Contains state-specific logic for certain aspects of pre-processing US_ME
StateIncarcerationPeriod entities so that they are ready to be used in pipeline
calculations."""
from typing import List

from recidiviz.persistence.entity.activity.entities import StateIncarcerationPeriod
from recidiviz.pipelines.ingest.activity.normalization.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.incarceration_period_utils import (
    legacy_standardize_purpose_for_incarceration_values,
)

INCARCERATION_SENTENCE_PERIOD_LOOKBACK = 7


class UsMeIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_ME implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    def standardize_purpose_for_incarceration_values(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """Standardizing PFI using _standardize_purpose_for_incarceration_values
        for US_ME since this was previously the default normalization behavior
        and there hasn't been a use case for skipping this inferrence yet"""

        return legacy_standardize_purpose_for_incarceration_values(
            incarceration_periods
        )
