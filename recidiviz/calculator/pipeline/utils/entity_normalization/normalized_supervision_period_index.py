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
"""A class for caching information about a set of supervision periods for use in the metric calculation pipelines."""

from collections import defaultdict
from datetime import date
from typing import Dict, List, Optional

import attr

from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateSupervisionPeriod,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    sort_normalized_entities_by_sequence_num,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    is_official_supervision_admission,
)
from recidiviz.common.date import DateRange, DateRangeDiff


def _supervision_periods_sorter(
    supervision_periods: List[NormalizedStateSupervisionPeriod],
) -> List[NormalizedStateSupervisionPeriod]:
    """Sorts the NormalizedStateSupervisionPeriods by the sequence_num."""
    return sort_normalized_entities_by_sequence_num(supervision_periods)


@attr.s
class NormalizedSupervisionPeriodIndex:
    """A class for caching information about a set of normalized supervision periods
    for use in the metric calculation pipelines.
    """

    sorted_supervision_periods: List[NormalizedStateSupervisionPeriod] = attr.ib(
        converter=_supervision_periods_sorter
    )

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

        for index, supervision_period in enumerate(self.sorted_supervision_periods):
            supervision_period_id = supervision_period.supervision_period_id

            if not supervision_period_id:
                raise ValueError(
                    "Unexpected supervision period without a supervision_period_id."
                )

            if not supervision_period.start_date:
                raise ValueError(
                    "Supervision period normalization is not setting missing start_dates correctly."
                )

            if index == 0 or is_official_supervision_admission(
                supervision_period.admission_reason
            ):
                # These indicate that supervision is "officially" starting to serve their supervision.
                most_recent_official_start = supervision_period.start_date
            elif (
                supervision_period.admission_reason
                == StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE
                and _transfer_from_supervision_type_is_official_admission(
                    most_recent_nonnull_supervision_type
                )
                and supervision_period.supervision_type is not None
                and supervision_period.supervision_type
                != most_recent_nonnull_supervision_type
            ):
                # This transfer to a new kind of supervision type indicates the person is officially starting a new
                # kind of supervision
                most_recent_official_start = supervision_period.start_date

            if not most_recent_official_start:
                supervision_start_dates_by_period_id[
                    supervision_period_id
                ] = supervision_period.start_date
            else:
                supervision_start_dates_by_period_id[
                    supervision_period_id
                ] = most_recent_official_start

            if supervision_period.supervision_type is not None:
                most_recent_nonnull_supervision_type = (
                    supervision_period.supervision_type
                )

        return supervision_start_dates_by_period_id

    # A dictionary mapping years and months of terminations of supervision to the StateSupervisionPeriods that ended in
    # that month.
    supervision_periods_by_termination_month: Dict[
        int, Dict[int, List[NormalizedStateSupervisionPeriod]]
    ] = attr.ib()

    @supervision_periods_by_termination_month.default
    def _supervision_periods_by_termination_month(
        self,
    ) -> Dict[int, Dict[int, List[NormalizedStateSupervisionPeriod]]]:
        """Organizes the list of StateSupervisionPeriods by the year and month of the termination_date on the period."""
        supervision_periods_by_termination_month: Dict[
            int, Dict[int, List[NormalizedStateSupervisionPeriod]]
        ] = defaultdict()

        for supervision_period in self.sorted_supervision_periods:
            if supervision_period.termination_date:
                year = supervision_period.termination_date.year
                month = supervision_period.termination_date.month

                if year not in supervision_periods_by_termination_month.keys():
                    supervision_periods_by_termination_month[year] = {
                        month: [supervision_period]
                    }
                elif month not in supervision_periods_by_termination_month[year].keys():
                    supervision_periods_by_termination_month[year][month] = [
                        supervision_period
                    ]
                else:
                    supervision_periods_by_termination_month[year][month].append(
                        supervision_period
                    )

        return supervision_periods_by_termination_month

    def get_most_recent_previous_supervision_period(
        self, current_supervision_period: NormalizedStateSupervisionPeriod
    ) -> Optional[NormalizedStateSupervisionPeriod]:
        """Given a current supervision period, return the most recent previous supervision period, if present."""
        if not current_supervision_period.supervision_period_id:
            raise ValueError(
                "Current supervision period must have a supervision period id."
            )

        current_supervision_period_index: Optional[
            int
        ] = self.sorted_supervision_periods.index(current_supervision_period)

        if current_supervision_period_index is None:
            raise ValueError(
                "Current supervision period was not located in supervision period index."
            )

        # `sorted_supervision_periods` sorts supervision periods from least recent to most recent, so if the current
        # supervision period is first in the list, that means there is no most recent previous supervision
        # period.
        if current_supervision_period_index == 0:
            return None

        most_recent_previous_supervision_period = self.sorted_supervision_periods[
            current_supervision_period_index - 1
        ]

        return most_recent_previous_supervision_period

    def get_supervision_period_overlapping_with_date_range(
        self, date_range: DateRange
    ) -> Optional[NormalizedStateSupervisionPeriod]:
        """Returns the first supervision period that overlaps with the given date range."""
        for supervision_period in self.sorted_supervision_periods:
            if DateRangeDiff(supervision_period.duration, date_range).overlapping_range:
                return supervision_period
        return None


def _transfer_from_supervision_type_is_official_admission(
    supervision_type: Optional[StateSupervisionPeriodSupervisionType],
) -> bool:
    """Returns whether being transferred from a supervision period with the given
    |supervision_type| is considered an official start of
    supervision."""
    if not supervision_type:
        return False

    # If a person is transferred from one of these types of supervision to a new type
    # of supervision, this counts as an official admission to new supervision
    official_start_supervision_types = [
        StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT,
        StateSupervisionPeriodSupervisionType.INVESTIGATION,
        StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION,
    ]

    non_official_start_supervision_types = [
        StateSupervisionPeriodSupervisionType.DUAL,
        StateSupervisionPeriodSupervisionType.EXTERNAL_UNKNOWN,
        StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
        StateSupervisionPeriodSupervisionType.PAROLE,
        StateSupervisionPeriodSupervisionType.PROBATION,
        StateSupervisionPeriodSupervisionType.BENCH_WARRANT,
        StateSupervisionPeriodSupervisionType.ABSCONSION,
    ]

    if supervision_type in official_start_supervision_types:
        return True
    if supervision_type in non_official_start_supervision_types:
        return False

    raise ValueError(
        f"Unsupported StateSupervisionPeriodSupervisionType value: {supervision_type}"
    )
