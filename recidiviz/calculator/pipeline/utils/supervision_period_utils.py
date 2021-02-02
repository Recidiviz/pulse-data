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

from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod

# The number of months for the window of time prior to a revocation return in which we look for a terminated supervision
# period to attribute the revocation to
SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT = 24


def prepare_supervision_periods_for_calculations(supervision_periods: List[StateSupervisionPeriod],
                                                 drop_federal_and_other_country_supervision_periods: bool) -> \
        List[StateSupervisionPeriod]:
    supervision_periods = _drop_placeholder_periods(supervision_periods)

    if drop_federal_and_other_country_supervision_periods:
        supervision_periods = _drop_other_country_and_federal_supervision_periods(supervision_periods)

    return supervision_periods


def _drop_placeholder_periods(supervision_periods: List[StateSupervisionPeriod]) -> \
        List[StateSupervisionPeriod]:
    """Drops all placeholder supervision periods."""
    return [
        period for period in supervision_periods
        if not is_placeholder(period)
    ]


def _set_unset_start_dates(supervision_periods: List[StateSupervisionPeriod]) -> \
        List[StateSupervisionPeriod]:
    """Sets start_dates on periods where start_date is unset."""
    updated_periods: List[StateSupervisionPeriod] = []

    for supervision_period in supervision_periods:
        if not supervision_period.start_date:
            if not supervision_period.termination_date:
                # Drop periods without start or termination dates
                continue
            supervision_period.start_date = supervision_period.termination_date
        updated_periods.append(supervision_period)

    return updated_periods


def _drop_other_country_and_federal_supervision_periods(supervision_periods: List[StateSupervisionPeriod]) -> \
        List[StateSupervisionPeriod]:
    """Drop all supervision periods whose custodial authority excludes it from the state's supervision metrics."""
    return [
        period for period in supervision_periods
        if period.custodial_authority not in (StateCustodialAuthority.FEDERAL, StateCustodialAuthority.OTHER_COUNTRY)
    ]


def get_relevant_supervision_periods_before_admission_date(
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
            find_last_supervision_period_terminated_before_date(
                admission_date, supervision_periods, SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT)

        if most_recent_supervision_period:
            relevant_periods.append(most_recent_supervision_period)

    return relevant_periods


def find_last_supervision_period_terminated_before_date(
        upper_bound_date: date, supervision_periods: List[StateSupervisionPeriod],
        maximum_months_proximity: int = SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT) -> Optional[StateSupervisionPeriod]:
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
        return max(previous_periods, key=lambda p: p.termination_date or date.min)

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
