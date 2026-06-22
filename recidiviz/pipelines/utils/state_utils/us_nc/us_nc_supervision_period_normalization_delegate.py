# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""US_NC implementation of the StateSpecificSupervisionNormalizationDelegate."""
from typing import List, Optional

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.activity.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)
from recidiviz.pipelines.ingest.activity.normalization.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)

# NC provides snapshot data with no termination reason. If an incarceration
# period starts within this many days of a supervision period ending, we infer
# the supervision was revoked rather than discharged.
_REVOCATION_LOOKFORWARD_DAYS = 7


class UsNcSupervisionNormalizationDelegate(
    StateSpecificSupervisionNormalizationDelegate
):
    """US_NC implementation of the StateSpecificSupervisionNormalizationDelegate."""

    def __init__(self, incarceration_periods: List[StateIncarcerationPeriod]) -> None:
        self._incarceration_periods = incarceration_periods

    def supervision_termination_reason_override(
        self, supervision_period: StateSupervisionPeriod
    ) -> Optional[StateSupervisionPeriodTerminationReason]:
        """NC provides snapshot data with no termination reason. For closed periods,
        infer REVOCATION if an incarceration period starts during the supervision
        period or within 7 days after it ends; otherwise default to DISCHARGE."""
        if supervision_period.end_date_exclusive is None:
            return None

        for ip in self._incarceration_periods:
            if ip.admission_date is None:
                continue
            days_after = (
                ip.admission_date - supervision_period.end_date_exclusive
            ).days
            if (
                ip.admission_date >= supervision_period.start_date
                and days_after <= _REVOCATION_LOOKFORWARD_DAYS
            ):
                return StateSupervisionPeriodTerminationReason.REVOCATION

        return StateSupervisionPeriodTerminationReason.DISCHARGE
