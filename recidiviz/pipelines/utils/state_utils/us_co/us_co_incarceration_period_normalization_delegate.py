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
"""Contains US_CO implementation of the StateSpecificIncarcerationNormalizationDelegate."""
import datetime
from typing import Optional

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)


class UsCoIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_CO implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    def incarceration_facility_override(
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> Optional[str]:
        """If there is an incarceration period starting with an escape code and the return date is 72 hours after the
        start date, or there is no end date, then we assume the period's facility was fugitive inmate.
        """

        if incarceration_period.admission_date is None:
            return incarceration_period.facility

        delta = incarceration_period.duration.timedelta()

        if (
            incarceration_period.admission_reason
            is StateIncarcerationPeriodAdmissionReason.ESCAPE
        ) and (delta >= datetime.timedelta(days=3)):
            incarceration_period.facility = "FUG-INMATE"

        return incarceration_period.facility
