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
"""Contains US_MI implementation of the StateSpecificIncarcerationNormalizationDelegate."""
from typing import List, Optional

from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mi.us_mi_incarceration_period import (
    UsMiIncarcerationPeriod,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
)


class UsMiIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_MI implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    def incarceration_admission_reason_override(
        self,
        incarceration_period: StateIncarcerationPeriod,
        incarceration_sentences: Optional[List[StateIncarcerationSentence]],
    ) -> Optional[StateIncarcerationPeriodAdmissionReason]:
        if (
            incarceration_period.admission_reason
            == StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
        ):
            return StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION
        return incarceration_period.admission_reason

    def period_is_parole_board_hold(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
    ) -> bool:
        """A period is considered a parole board hold if the person is being housed in
        housing unit 6 of the Macomb Correctional Facility. These people are alleged
        parole violators and are awaiting possible new commitments back to prison."""
        incarceration_period = sorted_incarceration_periods[
            incarceration_period_list_index
        ]
        if not isinstance(incarceration_period, UsMiIncarcerationPeriod):
            raise ValueError(
                f"Unexpected type for incarceration period: {type(incarceration_period)}."
                f"Found incarceration period: {incarceration_period}."
            )

        return (
            (
                incarceration_period.reporting_station_name == "HU#6"
                and incarceration_period.facility == "MRF"
            )
            or incarceration_period.specialized_purpose_for_incarceration
            == StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
        )
