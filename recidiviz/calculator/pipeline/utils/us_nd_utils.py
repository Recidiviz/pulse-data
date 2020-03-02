# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Utils for state-specific calculations for North Dakota."""
from typing import List

from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodReleaseReason, \
    StateIncarcerationPeriodAdmissionReason
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


def set_missing_admission_data(incarceration_periods: List[StateIncarcerationPeriod]) -> List[StateIncarcerationPeriod]:
    """Sets missing admission data on incarceration period inputs according to logic specific to North Dakota data.

    In the ND data, there are over 1,000 incarceration periods with empty admission dates and reasons that follow a
    release for a transfer. These incarceration periods have valid release data, so we know that this individual was
    transferred back into a facility at some point. For every incarceration period where this is the case, this
    function sets the admission date to the date of the transfer out, and the admission reason to be a transfer back in.
    """
    filtered_incarceration_periods: List[StateIncarcerationPeriod] = []

    # Remove placeholder incarceration periods and any without an external_id
    for incarceration_period in incarceration_periods:
        if not is_placeholder(incarceration_period) and incarceration_period.external_id is not None:
            filtered_incarceration_periods.append(incarceration_period)

    updated_incarceration_periods: List[StateIncarcerationPeriod] = []

    if filtered_incarceration_periods:
        filtered_incarceration_periods.sort(key=lambda b: b.external_id)

        for index, incarceration_period in enumerate(filtered_incarceration_periods):
            if not incarceration_period.admission_date and not incarceration_period.admission_reason and index > 0:
                previous_incarceration_period = filtered_incarceration_periods[index - 1]
                if previous_incarceration_period.release_reason == StateIncarcerationPeriodReleaseReason.TRANSFER \
                        and previous_incarceration_period.release_date is not None:
                    incarceration_period.admission_reason = StateIncarcerationPeriodAdmissionReason.TRANSFER
                    incarceration_period.admission_date = previous_incarceration_period.release_date

            updated_incarceration_periods.append(incarceration_period)

    return updated_incarceration_periods
