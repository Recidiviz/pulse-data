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
"""US_MO implementation of the supervision delegate"""
from datetime import date
from typing import Any, Dict, List, Optional

from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_utils import (
    us_mo_get_most_recent_supervision_type_before_upper_bound_day,
)
from recidiviz.common.date import DateRange, DateRangeDiff
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationSentence,
    StateSupervisionPeriod,
    StateSupervisionSentence,
)


class UsMoSupervisionDelegate(StateSpecificSupervisionDelegate):
    """US_MO implementation of the supervision delegate"""

    def supervision_types_mutually_exclusive(self) -> bool:
        """In US_MO, people on DUAL supervision are tracked as mutually exclusive from groups of people
        on PAROLE or PROBATION."""
        return True

    def supervision_period_in_supervision_population_in_non_excluded_date_range(
        self,
        date_range: DateRange,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_period: StateSupervisionPeriod,
        supervising_officer_external_id: Optional[str],
    ) -> bool:
        """In US_MO, first finds the overlapping date range between the supervision period
        and the date range given. If no overlap, then it does not count towards the population.

        In addition, a supervision period should have an active PO to also be included to the
        supervision population.

        Otherwise finds the most recent nonnull supervision period, supervision type associated to
        the person with these sentences to indicate that the person should count towards the population.
        """
        overlapping_range = DateRangeDiff(
            range_1=date_range, range_2=supervision_period.duration
        ).overlapping_range

        if not overlapping_range:
            return False

        if (
            supervising_officer_external_id is None
            and supervision_period.supervising_officer is None
        ):
            return False

        return (
            us_mo_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=overlapping_range.upper_bound_exclusive_date,
                lower_bound_inclusive_date=overlapping_range.lower_bound_inclusive_date,
                incarceration_sentences=incarceration_sentences,
                supervision_sentences=supervision_sentences,
            )
            is not None
        )

    def should_produce_supervision_event_for_period(
        self,
        supervision_period: StateSupervisionPeriod,
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
    ) -> bool:
        """For US_MO, we retrieve the most recent nonnull supervision period supervision type associated with these
        sentences for the person in order to determine if an event should be produced. If no days of the supervision_period
        count towards any metrics, the period can be dropped entirely.
        """
        sp_range = supervision_period.duration

        return (
            us_mo_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=sp_range.upper_bound_exclusive_date,
                lower_bound_inclusive_date=sp_range.lower_bound_inclusive_date,
                incarceration_sentences=incarceration_sentences,
                supervision_sentences=supervision_sentences,
            )
            is not None
        )

    def get_supervising_officer_external_id_for_supervision_period(
        self,
        supervision_period: StateSupervisionPeriod,
        supervision_period_to_agent_associations: Dict[int, Dict[str, Any]],
    ) -> Optional[str]:
        """In US_MO, because supervision periods may not necessarily have persisted
        supervision period ids, we do a date-range based mapping back to original
        ingested periods."""
        for agent_dict in supervision_period_to_agent_associations.values():
            start_date: Optional[date] = agent_dict["agent_start_date"]
            end_date: Optional[date] = agent_dict["agent_end_date"]

            if not start_date:
                continue

            if DateRangeDiff(
                supervision_period.duration,
                DateRange.from_maybe_open_range(start_date, end_date),
            ).overlapping_range:
                return agent_dict["agent_external_id"]

        return None
