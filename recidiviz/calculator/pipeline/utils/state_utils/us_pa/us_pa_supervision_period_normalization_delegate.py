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
"""US_PA implementation of the supervision normalization delegate"""
from datetime import date
from typing import List, Optional

from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities_utils import (
    update_normalized_entity_with_globally_unique_id,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)


class UsPaSupervisionNormalizationDelegate(
    StateSpecificSupervisionNormalizationDelegate
):
    """US_PA implementation of the supervision normalization delegate"""

    def normalization_relies_on_incarceration_periods(self) -> bool:
        """In US_PA, inference of additional periods requires incarceration periods."""
        return True

    def infer_additional_periods(
        self,
        person_id: int,
        supervision_periods: List[StateSupervisionPeriod],
        incarceration_periods: Optional[List[StateIncarcerationPeriod]],
    ) -> List[StateSupervisionPeriod]:
        """In US_PA, we need to infer additional periods that represent periods of
        active absconsions. This assumes that the supervision periods are already sorted
        and have missing dates inferred.

        If a supervision period ending in absconsion has a subsequent incarceration period,
        this new inferred period will have a start date of when the supervision period
        ended and a termination date of when the incarceration period began. Otherwise,
        if there is no incarceration period afterwards, then the termination date is not
        supplied and this new supervision period is implied to be an open period, indicating
        active absconsion.
        """

        # TODO(#10084): Consider combining functions of IP / SP normalization to
        # guarantee better end dates for inferred absconsion supervision periods
        if incarceration_periods is None:
            raise ValueError(
                "Incarceration periods are required for inferring additional supervision periods."
            )

        admission_dates = sorted(
            [ip.admission_date for ip in incarceration_periods if ip.admission_date]
        )

        new_supervision_periods: List[StateSupervisionPeriod] = []
        for index, supervision_period in enumerate(supervision_periods):
            new_supervision_periods.append(supervision_period)
            if (
                supervision_period.termination_reason
                == StateSupervisionPeriodTerminationReason.ABSCONSION
            ):
                if supervision_period.termination_date is None:
                    raise ValueError(
                        "Unexpected null termination date for supervision period with "
                        f"termination reason: {supervision_period.supervision_period_id}"
                    )
                next_incarceration_admission_date = next(
                    (
                        admission_date
                        for admission_date in admission_dates
                        if admission_date > supervision_period.termination_date
                    ),
                    None,
                )
                next_supervision_start_date = (
                    supervision_periods[index + 1].start_date
                    if index < len(supervision_periods) - 1
                    else None
                )
                termination_date: Optional[date] = None
                if next_supervision_start_date and next_incarceration_admission_date:
                    termination_date = min(
                        next_supervision_start_date,
                        next_incarceration_admission_date,
                    )
                elif next_supervision_start_date:
                    termination_date = next_supervision_start_date
                else:
                    termination_date = next_incarceration_admission_date
                new_supervision_period = StateSupervisionPeriod(
                    state_code=StateCode.US_PA.value,
                    start_date=supervision_period.termination_date,
                    termination_date=termination_date,
                    supervision_type=supervision_period.supervision_type,
                    admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
                    termination_reason=StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION
                    if termination_date
                    else None,
                    supervision_site=None,
                    supervision_level=supervision_period.supervision_level,
                    supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                )
                # Add a unique id to the new SP
                update_normalized_entity_with_globally_unique_id(
                    person_id=person_id, entity=new_supervision_period
                )
                new_supervision_periods.append(new_supervision_period)
        return new_supervision_periods
