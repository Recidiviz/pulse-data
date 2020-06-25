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
"""A class for caching information about a set of supervision periods for use in the calculation pipelines."""

from collections import defaultdict
from datetime import date
from typing import List, Dict

import attr

from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodAdmissionReason, \
    StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod

# A supervision period that has one of these admission reasons indicates the official start of supervision
OFFICIAL_SUPERVISION_ADMISSION_REASONS = [StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                                          StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE]

# If a person is transferred from one of these types of supervision to a new type of supervision, this counts as an
# official admission to new supervision
SUPERVISION_TYPE_TRANSFER_OFFICIAL_ADMISSIONS = [
    StateSupervisionPeriodSupervisionType.INVESTIGATION,
    StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION
]


def _supervision_periods_converter(supervision_periods: List[StateSupervisionPeriod]):
    supervision_periods.sort(key=lambda b: (b.start_date,
                                            b.termination_date or date.max,
                                            # Official admissions should sort before other admissions on the
                                            # same day
                                            b.admission_reason not in OFFICIAL_SUPERVISION_ADMISSION_REASONS))
    return supervision_periods


@attr.s
class SupervisionPeriodIndex:
    """A class for caching information about a set of supervision periods for use in the calculation pipelines."""

    supervision_periods: List[StateSupervisionPeriod] = attr.ib(converter=_supervision_periods_converter)

    # A dictionary mapping supervision_period_id values to the date on which the person started serving the
    # supervision represented by that period. For parole, this is the date the person started parole after being
    # released from prison. For probation, this is the date the person started probation after being sentenced to
    # probation by the court. Regardless of admission_reason, supervision starts with the first supervision period
    # recorded for the person. Supervision periods stop and start all of the time because of transfers between officers,
    # periods of absconsion, etc. For every period, this maps to a date on which the supervision officially started.
    supervision_start_dates_by_period_id: Dict[int, date] = attr.ib()

    @supervision_start_dates_by_period_id.default
    def _supervision_start_dates_by_period_id(self) -> Dict[int, date]:
        """Determines when the person began serving this supervision for each supervision period."""
        supervision_start_dates_by_period_id: Dict[int, date] = defaultdict()

        most_recent_official_start = None
        most_recent_nonnull_supervision_type = None

        for index, supervision_period in enumerate(self.supervision_periods):
            supervision_period_id = supervision_period.supervision_period_id

            if not supervision_period_id:
                raise ValueError("Unexpected supervision period without a supervision_period_id.")

            if not supervision_period.start_date:
                raise ValueError("Supervision period pre-processing is not setting missing start_dates correctly.")

            if index == 0 or supervision_period.admission_reason in OFFICIAL_SUPERVISION_ADMISSION_REASONS:
                # These indicate that supervision is "officially" starting to serve their supervision.
                most_recent_official_start = supervision_period.start_date
            elif (supervision_period.admission_reason == StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE
                  and most_recent_nonnull_supervision_type in SUPERVISION_TYPE_TRANSFER_OFFICIAL_ADMISSIONS
                  and supervision_period.supervision_period_supervision_type is not None
                  and supervision_period.supervision_period_supervision_type != most_recent_nonnull_supervision_type):
                # This transfer to a new kind of supervision type indicates the person is officially starting a new
                # kind of supervision
                most_recent_official_start = supervision_period.start_date

            if not most_recent_official_start:
                supervision_start_dates_by_period_id[supervision_period_id] = supervision_period.start_date
            else:
                supervision_start_dates_by_period_id[supervision_period_id] = most_recent_official_start

            if supervision_period.supervision_period_supervision_type is not None:
                most_recent_nonnull_supervision_type = supervision_period.supervision_period_supervision_type

        return supervision_start_dates_by_period_id

    # A dictionary mapping years and months of terminations of supervision to the StateSupervisionPeriods that ended in
    # that month.
    supervision_periods_by_termination_month: Dict[int, Dict[int, List[StateSupervisionPeriod]]] = attr.ib()

    @supervision_periods_by_termination_month.default
    def _supervision_periods_by_termination_month(self) -> \
            Dict[int, Dict[int, List[StateSupervisionPeriod]]]:
        """Organizes the list of StateSupervisionPeriods by the year and month of the termination_date on the period."""
        supervision_periods_by_termination_month: Dict[int, Dict[int, List[StateSupervisionPeriod]]] = defaultdict()

        for supervision_period in self.supervision_periods:
            if supervision_period.termination_date:
                year = supervision_period.termination_date.year
                month = supervision_period.termination_date.month

                if year not in supervision_periods_by_termination_month.keys():
                    supervision_periods_by_termination_month[year] = {
                        month: [supervision_period]
                    }
                elif month not in supervision_periods_by_termination_month[year].keys():
                    supervision_periods_by_termination_month[year][month] = [supervision_period]
                else:
                    supervision_periods_by_termination_month[year][month].append(supervision_period)

        return supervision_periods_by_termination_month
