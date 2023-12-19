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
from typing import List

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)


class UsMiSupervisionNormalizationDelegate(
    StateSpecificSupervisionNormalizationDelegate
):
    """US_MI implementation of the StateSpecificSupervisionNormalizationDelegate."""

    # TODO(#23256): Delete this once ingest handles this deletion better
    def drop_bad_periods(
        self, sorted_supervision_periods: List[StateSupervisionPeriod]
    ) -> List[StateSupervisionPeriod]:
        periods_to_keep = []

        for idx, sp in enumerate(sorted_supervision_periods):
            # If supervision type = INVESTIGATION, let's drop
            if (
                sp.supervision_type
                == StateSupervisionPeriodSupervisionType.INVESTIGATION
            ):
                continue

            # Only consider droppping if ingest considered this the last supervision period for this person at some point
            # and there are more than one periods
            # (to ensure we are only going to drop periods likely due to entity deletion issues)
            # (Note: supervision periods external ids are 1-indexed which is why we're comparing against length directly)
            if (
                int((sp.external_id).split("-")[1]) == len(sorted_supervision_periods)
                and len(sorted_supervision_periods) > 1
            ):
                previous_sp = sorted_supervision_periods[idx - 1]

                if (
                    # if the previous supervision period ended with a discharge
                    previous_sp.termination_reason
                    in (
                        StateSupervisionPeriodTerminationReason.DISCHARGE,
                        StateSupervisionPeriodTerminationReason.EXPIRATION,
                    )
                    # and this was a null period that started on the discharge date
                    and sp.termination_date is None
                    and previous_sp.termination_date == sp.start_date
                    # that wasn't started because of a movement
                    and sp.admission_reason_raw_text is None
                    and sp.admission_reason
                    == StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE
                ):
                    # don't keep this period because it's a non-sensical and likely due to entity deletion issues
                    continue

            periods_to_keep.append(sp)

        return periods_to_keep
