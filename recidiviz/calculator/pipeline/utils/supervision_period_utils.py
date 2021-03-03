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

from recidiviz.calculator.pipeline.utils.period_utils import (
    sort_periods_by_set_dates_and_statuses,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodAdmissionReason as AdmissionReason,
    StateSupervisionPeriodTerminationReason as TerminationReason,
)
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod

# The number of months for the window of time prior to a revocation return in which we look for a terminated supervision
# period to attribute the revocation to
SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT = 24


def prepare_supervision_periods_for_calculations(
    supervision_periods: List[StateSupervisionPeriod],
    drop_federal_and_other_country_supervision_periods: bool,
) -> List[StateSupervisionPeriod]:
    supervision_periods = _drop_placeholder_periods(supervision_periods)

    if drop_federal_and_other_country_supervision_periods:
        supervision_periods = _drop_other_country_and_federal_supervision_periods(
            supervision_periods
        )

    supervision_periods = _infer_missing_dates_and_statuses(supervision_periods)

    return supervision_periods


def _drop_placeholder_periods(
    supervision_periods: List[StateSupervisionPeriod],
) -> List[StateSupervisionPeriod]:
    """Drops all placeholder supervision periods."""
    return [period for period in supervision_periods if not is_placeholder(period)]


def _is_active_period(period: StateSupervisionPeriod) -> bool:
    return period.status == StateSupervisionPeriodStatus.UNDER_SUPERVISION


def _infer_missing_dates_and_statuses(
    supervision_periods: List[StateSupervisionPeriod],
) -> List[StateSupervisionPeriod]:
    """First, sorts the supervision_periods in chronological order of the start and termination dates. Then, for any
    periods missing dates and statuses, infers this information given the other supervision periods.
    """
    sort_periods_by_set_dates_and_statuses(supervision_periods, _is_active_period)

    updated_periods: List[StateSupervisionPeriod] = []

    for sp in supervision_periods:
        if sp.termination_date is None:
            if sp.status != StateSupervisionPeriodStatus.UNDER_SUPERVISION:
                # If the person is not under supervision on this period, set the termination date to the start date.
                sp.termination_date = sp.start_date
                sp.termination_reason = TerminationReason.INTERNAL_UNKNOWN
            elif sp.termination_reason or sp.termination_reason_raw_text:
                # There is no termination date on this period, but the set termination_reason indicates that the person
                # is no longer in custody. Set the termination date to the start date.
                sp.termination_date = sp.start_date
                sp.status = StateSupervisionPeriodStatus.TERMINATED

                logging.warning(
                    "No termination_date for supervision period (%d) with nonnull termination_reason (%s) "
                    "or termination_reason_raw_text (%s)",
                    sp.supervision_period_id,
                    sp.termination_reason,
                    sp.termination_reason_raw_text,
                )

        elif sp.termination_date > date.today():
            # This is an erroneous termination_date in the future. For the purpose of calculations, clear the
            # termination_date and the termination_reason.
            sp.termination_date = None
            sp.termination_reason = None
            sp.status = StateSupervisionPeriodStatus.UNDER_SUPERVISION

        if sp.start_date is None:
            logging.info("Dropping supervision period without start_date: [%s]", sp)
            continue
        if sp.start_date > date.today():
            logging.info(
                "Dropping supervision period with start_date in the future: [%s]", sp
            )
            continue

        if sp.admission_reason is None:
            # We have no idea what this admission reason was. Set as INTERNAL_UNKNOWN.
            sp.admission_reason = AdmissionReason.INTERNAL_UNKNOWN
        if sp.termination_date is not None and sp.termination_reason is None:
            # We have no idea what this termination reason was. Set as INTERNAL_UNKNOWN.
            sp.termination_reason = TerminationReason.INTERNAL_UNKNOWN

        if sp.start_date and sp.termination_date:
            if sp.termination_date < sp.start_date:
                logging.info(
                    "Dropping supervision period with termination before admission: [%s]",
                    sp,
                )
                continue

        updated_periods.append(sp)

    return updated_periods


def _drop_other_country_and_federal_supervision_periods(
    supervision_periods: List[StateSupervisionPeriod],
) -> List[StateSupervisionPeriod]:
    """Drop all supervision periods whose custodial authority excludes it from the state's supervision metrics."""
    return [
        period
        for period in supervision_periods
        if period.custodial_authority
        not in (StateCustodialAuthority.FEDERAL, StateCustodialAuthority.OTHER_COUNTRY)
    ]


def get_relevant_supervision_periods_before_admission_date(
    admission_date: Optional[date], supervision_periods: List[StateSupervisionPeriod]
) -> List[StateSupervisionPeriod]:
    """Returns the relevant supervision periods preceding an admission to prison that can be used to determine
    supervision type prior to admission.
    """

    if not admission_date:
        logging.warning("Unexpectedly no admission date provided")
        return []

    relevant_periods = _supervision_periods_overlapping_with_date(
        admission_date, supervision_periods
    )

    if not relevant_periods:
        # If there are no overlapping supervision periods, but they had a prison admission, then they
        # may have been on supervision at some time before this admission. If present within the
        # |SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT|, find the most recently terminated supervision period.
        most_recent_supervision_period = (
            find_last_supervision_period_terminated_before_date(
                admission_date,
                supervision_periods,
                SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
            )
        )

        if most_recent_supervision_period:
            relevant_periods.append(most_recent_supervision_period)

    return relevant_periods


def find_last_supervision_period_terminated_before_date(
    upper_bound_date: date,
    supervision_periods: List[StateSupervisionPeriod],
    maximum_months_proximity: int = SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
) -> Optional[StateSupervisionPeriod]:
    """Looks for the supervision period that ended most recently before the upper_bound_date, within the
    month window defined by |maximum_months_proximity|.

    If no terminated supervision period is found before the upper_bound_date, returns None.
    """
    termination_date_cutoff = upper_bound_date - relativedelta(
        months=maximum_months_proximity
    )

    previous_periods = [
        supervision_period
        for supervision_period in supervision_periods
        if supervision_period.start_date is not None
        and supervision_period.termination_date is not None
        and supervision_period.termination_date >= termination_date_cutoff
        and supervision_period.start_date
        <= supervision_period.termination_date
        <= upper_bound_date
    ]

    if previous_periods:
        return max(previous_periods, key=lambda p: p.termination_date or date.min)

    return None


def _supervision_periods_overlapping_with_date(
    intersection_date: date, supervision_periods: List[StateSupervisionPeriod]
) -> List[StateSupervisionPeriod]:
    """Returns the supervision periods that overlap with the intersection_date."""
    overlapping_periods = [
        supervision_period
        for supervision_period in supervision_periods
        if supervision_period.start_date is not None
        and supervision_period.start_date <= intersection_date
        and (
            supervision_period.termination_date is None
            or intersection_date <= supervision_period.termination_date
        )
    ]

    return overlapping_periods
