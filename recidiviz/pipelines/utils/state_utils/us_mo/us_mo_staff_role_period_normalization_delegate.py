# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Contains US_MO implementation of the StateSpecificStaffRolePeriodNormalizationDelegate."""
import datetime
from typing import List, Optional

from recidiviz.persistence.entity.entity_utils import deep_entity_update
from recidiviz.persistence.entity.state.entities import StateStaffRolePeriod
from recidiviz.pipelines.normalization.utils.normalization_managers.staff_role_period_normalization_manager import (
    StateSpecificStaffRolePeriodNormalizationDelegate,
)


class UsMoStaffRolePeriodNormalizationDelegate(
    StateSpecificStaffRolePeriodNormalizationDelegate
):
    """US_MO implementation of the StateSpecificStaffRolePeriodNormalizationDelegate."""

    def normalize_role_periods(
        self, role_periods: List[StateStaffRolePeriod]
    ) -> List[StateStaffRolePeriod]:
        return [
            deep_entity_update(
                rp,
                start_date=rp.start_date
                if self._is_date_valid(rp.start_date)
                and self._period_bounds_valid(rp.start_date, rp.end_date)
                else None,
                end_date=rp.end_date
                if self._is_date_valid(rp.end_date)
                and self._period_bounds_valid(rp.start_date, rp.end_date)
                else None,
            )
            for rp in role_periods
        ]

    @staticmethod
    def _period_bounds_valid(
        start_date: Optional[datetime.date], end_date: Optional[datetime.date]
    ) -> bool:
        """For MO, there are numerous cases where a role period is recorded with an
        end date earlier than the start date. Since at least one of the dates must be
        erroneous, we invalidate them both."""
        if start_date and end_date:
            return end_date >= start_date
        return True

    @staticmethod
    def _is_date_valid(date_bound: Optional[datetime.date]) -> bool:
        """For MO, there are numerous cases where the role period start/end date appears
        to be entered incorrectly, but nonetheless can be parsed into a date in the future
        or the distant past. Any dates before 1900 are treated as invalid, and dates in
        the future are removed as well to indicate open periods."""
        return not date_bound or (
            date_bound.year >= 1900 and date_bound <= datetime.date.today()
        )
