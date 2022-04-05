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
from typing import List, Dict, Optional

import attr

from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodAdmissionReason, \
    StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod


def _supervision_periods_converter(supervision_periods: List[StateSupervisionPeriod]):
    supervision_periods.sort(key=lambda b: (b.start_date,
                                            b.termination_date or date.max,
                                            # Official admissions should sort before other admissions on the same day
                                            not _is_official_supervision_admission(b.admission_reason)))
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

            if index == 0 or _is_official_supervision_admission(supervision_period.admission_reason):
                # These indicate that supervision is "officially" starting to serve their supervision.
                most_recent_official_start = supervision_period.start_date
            elif (supervision_period.admission_reason == StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE
                  and _transfer_from_supervision_type_is_official_admission(most_recent_nonnull_supervision_type)
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


def _is_official_supervision_admission(admission_reason: Optional[StateSupervisionPeriodAdmissionReason]) -> bool:
    """Returns whether or not the |admission_reason| is considered an official start of supervision."""
    if not admission_reason:
        return False

    # A supervision period that has one of these admission reasons indicates the official start of supervision
    official_admissions = [
        StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
        StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE
    ]

    non_official_admissions = [
        StateSupervisionPeriodAdmissionReason.ABSCONSION,
        StateSupervisionPeriodAdmissionReason.EXTERNAL_UNKNOWN,
        StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
        StateSupervisionPeriodAdmissionReason.INVESTIGATION,
        StateSupervisionPeriodAdmissionReason.TRANSFER_OUT_OF_STATE,
        StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
        StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION
    ]

    if admission_reason in official_admissions:
        return True
    if admission_reason in non_official_admissions:
        return False

    raise ValueError(f"Unsupported StateSupervisionPeriodAdmissionReason value: {admission_reason}")


def _transfer_from_supervision_type_is_official_admission(
        supervision_period_supervision_type: Optional[StateSupervisionPeriodSupervisionType]) -> bool:
    """Returns whether being transferred from a supervision period with the given |supervision_period_supervision_type|
    is considered an official start of supervision."""
    if not supervision_period_supervision_type:
        return False

    # If a person is transferred from one of these types of supervision to a new type of supervision, this counts as an
    # official admission to new supervision
    official_start_supervision_types = [
        StateSupervisionPeriodSupervisionType.INVESTIGATION,
        StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION
    ]

    non_official_start_supervision_types = [
        StateSupervisionPeriodSupervisionType.DUAL,
        StateSupervisionPeriodSupervisionType.EXTERNAL_UNKNOWN,
        StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
        StateSupervisionPeriodSupervisionType.PAROLE,
        StateSupervisionPeriodSupervisionType.PROBATION
    ]

    if supervision_period_supervision_type in official_start_supervision_types:
        return True
    if supervision_period_supervision_type in non_official_start_supervision_types:
        return False

    raise ValueError(f"Unsupported StateSupervisionPeriodSupervisionType value: {supervision_period_supervision_type}")
