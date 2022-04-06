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
from typing import List

from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
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
