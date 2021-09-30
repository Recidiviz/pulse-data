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
"""Contains the StateSpecificSupervisionDelegate, the interface
for state-specific decisions involved in categorizing various attributes of
supervision."""
# pylint: disable=unused-argument
import abc
from typing import List, Optional, Tuple

from recidiviz.calculator.pipeline.supervision.events import SupervisionPopulationEvent
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.date import DateRange
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationSentence,
    StateSupervisionPeriod,
    StateSupervisionSentence,
)


class StateSpecificSupervisionDelegate(abc.ABC):
    """Interface for state-specific decisions involved in categorizing various attributes of supervision."""

    def supervision_types_mutually_exclusive(self) -> bool:
        """For some states, we want to track people on DUAL supervision as mutually exclusive from the groups of people on
        either PAROLE and PROBATION. For others, a person can be on multiple types of supervision simultaneously
        and contribute to counts for both types.

        Default behavior is that supervision types are *not* mutually exclusive, meaning a person can be on multiple types of supervision simultaneously.

        Returns whether our calculations should consider supervision types as distinct for the given state_code.
        """
        return False

    def supervision_location_from_supervision_site(
        self, supervision_site: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """Retrieves level 1 and level 2 location information from a supervision site.
        By default, returns the |supervision_site| as the level 1 location, and None as the level 2 location.
        """
        # TODO(#3829): Remove this helper once we've once we've built level 1/level 2 supervision
        #  location distinction directly into our schema.
        level_1_supervision_location = supervision_site
        level_2_supervision_location = None

        return (
            level_1_supervision_location or None,
            level_2_supervision_location or None,
        )

    def is_supervision_location_out_of_state(
        self, supervision_population_event: SupervisionPopulationEvent
    ) -> bool:
        """Returns whether the location on the supervision_population_event indicates supervision
        served out-of-state."""
        return False

    def supervision_period_in_supervision_population_in_non_excluded_date_range(
        self,
        date_range: DateRange,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_period: StateSupervisionPeriod,
    ) -> bool:
        """Returns False if there is state-specific information to indicate that the supervision period should not count
        towards the supervision population in the date range.

        This function should only be called for a date range where the person does not have any overlapping incarceration
        that prevents the person from being counted simultaneously in the supervision population.

        By default returns True.
        """
        return True

    def should_produce_supervision_event_for_period(
        self,
        supervision_period: StateSupervisionPeriod,
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
    ) -> bool:
        """Whether or not any SupervisionEvents should be created using the
        supervision_period. In some cases, supervision period pre-processing will not drop
        periods entirely because we need them for context in some of the calculations,
        but we do not want to create metrics using the periods.

        If this returns True, it does not necessarily mean they should be counted towards
        the supervision population for any part of this period. It just means that a
        person was actively assigned to supervision at this time and various
        characteristics of this period may be relevant for generating metrics (such as the
        termination reason / date) even if we may not count this person towards the
        supervision population during the period time span (e.g. if they are incarcerated
        the whole time).

        Default behavior is to not produce any supervision events associated with investigation
        or pre-confinement supervision. Currenlty, there should be no supervision events associated
        with investigation or pre-confinement supervision.
        """
        if (
            supervision_period.supervision_period_supervision_type
            == StateSupervisionPeriodSupervisionType.INVESTIGATION
        ):
            return False

        return True

    def get_index_of_first_reliable_supervision_assessment(self) -> int:
        """Some states rely on the first-reassessment (the second assessment) instead of the first assessment when comparing
        terminating assessment scores to a score at the beginning of someone's supervision.

        Default behavior is to return 1, the index of the second assessment."""
        # TODO(#2782): Investigate whether to update this logic
        return 1
