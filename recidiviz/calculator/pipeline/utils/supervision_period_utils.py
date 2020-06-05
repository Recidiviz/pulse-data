# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utils for validating and manipulating supervision periods for use in calculations."""
import logging
from datetime import date
from typing import List, Optional

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import \
    investigation_periods_in_supervision_population, non_state_custodial_authority_in_supervision_population
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod


# The number of months for the window of time prior to a revocation return in which we look for a terminated supervision
# period to attribute the revocation to
SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT = 24

# The suffix appended to the state_code that represents the custodial authority of that state's Department of
# Corrections
STATE_DOC_CUSTODIAL_AUTHORITY_SUFFIX = '_DOC'


def prepare_supervision_periods_for_calculations(supervision_periods: List[StateSupervisionPeriod]) -> \
        List[StateSupervisionPeriod]:

    if not supervision_periods:
        return supervision_periods

    state_code = supervision_periods[0].state_code

    if not investigation_periods_in_supervision_population(state_code):
        supervision_periods = _drop_investigation_supervision_periods(supervision_periods)
    if not non_state_custodial_authority_in_supervision_population(state_code):
        supervision_periods = _drop_non_state_custodial_authority_periods(supervision_periods)

    return supervision_periods


def _drop_investigation_supervision_periods(supervision_periods: List[StateSupervisionPeriod]) -> \
        List[StateSupervisionPeriod]:
    """Drops all supervision periods with a supervision_period_supervision_type of INVESTIGATION."""
    return [
        period for period in supervision_periods
        if period.supervision_period_supervision_type != StateSupervisionPeriodSupervisionType.INVESTIGATION
    ]


def _drop_non_state_custodial_authority_periods(supervision_periods: List[StateSupervisionPeriod]) -> \
        List[StateSupervisionPeriod]:
    """Drops all supervision periods where the custodial_authority is not the state DOC of the state_code on the
    supervision_period."""
    return [
        period for period in supervision_periods
        if period.custodial_authority == (period.state_code + STATE_DOC_CUSTODIAL_AUTHORITY_SUFFIX)
    ]


def _get_relevant_supervision_periods_before_admission_date(
        admission_date: Optional[date], supervision_periods: List[StateSupervisionPeriod]
) -> List[StateSupervisionPeriod]:
    """Returns the relevant supervision periods preceding an admission to prison that can be used to determine
    supervision type prior to admission.
    """

    if not admission_date:
        logging.warning('Unexpectedly no admission date provided')
        return []

    relevant_periods = _supervision_periods_overlapping_with_date(admission_date, supervision_periods)

    if not relevant_periods:
        # If there are no overlapping supervision periods, but they had a prison admission, then they
        # may have been on supervision at some time before this admission. If present within the
        # |SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT|, find the most recently terminated supervision period.
        most_recent_supervision_period = \
            _find_last_supervision_period_terminated_before_date(
                admission_date, supervision_periods, SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT)

        if most_recent_supervision_period:
            relevant_periods.append(most_recent_supervision_period)

    return relevant_periods


def _find_last_supervision_period_terminated_before_date(
        upper_bound_date: date, supervision_periods: List[StateSupervisionPeriod],
        maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT) -> Optional[StateSupervisionPeriod]:
    """Looks for the supervision period that ended most recently before the upper_bound_date, within the
    month window defined by |maximum_months_proximity|.

    If no terminated supervision period is found before the upper_bound_date, returns None.
    """
    termination_date_cutoff = upper_bound_date - relativedelta(months=maximum_months_proximity)

    previous_periods = [
        supervision_period for supervision_period in supervision_periods
        if supervision_period.start_date is not None and supervision_period.termination_date is not None
        and supervision_period.termination_date >= termination_date_cutoff
        and supervision_period.start_date <= supervision_period.termination_date <= upper_bound_date
    ]

    if previous_periods:
        previous_periods.sort(key=lambda b: b.termination_date)
        return previous_periods[-1]

    return None


def _supervision_periods_overlapping_with_date(
        intersection_date: date, supervision_periods: List[StateSupervisionPeriod]
) -> List[StateSupervisionPeriod]:
    """Returns the supervision periods that overlap with the intersection_date."""
    overlapping_periods = [
        supervision_period for supervision_period in supervision_periods
        if supervision_period.start_date is not None and supervision_period.start_date <= intersection_date and
        (supervision_period.termination_date is None or intersection_date <= supervision_period.termination_date)
    ]

    return overlapping_periods
