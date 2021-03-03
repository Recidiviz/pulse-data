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
# ============================================================================
"""Specific entity matching utils for StateIncarcerationIncident entities."""

import datetime
from typing import List, Tuple, Optional

from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity_matching.state.state_matching_utils import (
    add_child_to_entity,
    remove_child_from_entity,
)


def move_incidents_onto_periods(merged_persons: List[schema.StatePerson]):
    """Moves all StateIncarcerationIncidents that have placeholder StateIncarcerationPeriod parents onto non-placeholder
    StateIncarcerationPeriods if appropriate.

    This will only move incidents between periods that are part of the same sentence group."""
    for person in merged_persons:
        for sentence_group in person.sentence_groups:
            (
                placeholder_periods,
                non_placeholder_periods,
            ) = _get_periods_in_sentence_group(sentence_group)
            _move_incidents_onto_periods_helper(
                placeholder_periods=placeholder_periods,
                non_placeholder_periods=non_placeholder_periods,
            )


def _move_incidents_onto_periods_helper(
    *,
    placeholder_periods: List[schema.StateIncarcerationPeriod],
    non_placeholder_periods: List[schema.StateIncarcerationPeriod]
):
    """Moves all StateIncarcerationIncidents on any of the provided |placeholder_periods| onto periods in
    |non_placeholder_periods|, if a matching non-placeholder period exists."""
    for placeholder_period in placeholder_periods:
        incidents_to_remove = []
        for incident in placeholder_period.incarceration_incidents:
            match = _find_matching_period(incident, non_placeholder_periods)
            if match:
                incidents_to_remove.append((match, incident))

        # Remove incidents from placeholder parent after looping through all
        # incidents.
        for match_period, incident in incidents_to_remove:
            add_child_to_entity(
                entity=match_period,
                child_field_name="incarceration_incidents",
                child_to_add=incident,
            )
            remove_child_from_entity(
                entity=placeholder_period,
                child_field_name="incarceration_incidents",
                child_to_remove=incident,
            )


def _get_periods_in_sentence_group(
    sentence_group: schema.StateSentenceGroup,
) -> Tuple[
    List[schema.StateIncarcerationPeriod], List[schema.StateIncarcerationPeriod]
]:
    """Finds all placeholder and non-placeholder StateIncarcerationPeriods in the provided |sentence_group|, and
    returns the two lists in a tuple."""
    placeholder_periods = []
    non_placeholder_periods = []

    for incarceration_sentence in sentence_group.incarceration_sentences:
        for incarceration_period in incarceration_sentence.incarceration_periods:
            if is_placeholder(incarceration_period):
                placeholder_periods.append(incarceration_period)
            else:
                non_placeholder_periods.append(incarceration_period)
    return placeholder_periods, non_placeholder_periods


def _find_matching_period(
    incident: schema.StateIncarcerationIncident,
    potential_periods: List[schema.StateIncarcerationPeriod],
) -> Optional[schema.StateIncarcerationPeriod]:
    """Given the |incident|, finds a matching StateIncarcerationPeriod from the provided |periods|, if one exists."""
    incident_date = incident.incident_date
    if not incident_date:
        return None

    for potential_period in potential_periods:
        admission_date = potential_period.admission_date
        release_date = potential_period.release_date

        # Only match to periods with admission_dates
        if not admission_date:
            continue

        # If no release date, we assume the person is still in custody.
        if not release_date:
            release_date = datetime.date.max

        if (
            admission_date <= incident_date <= release_date
            and incident.facility == potential_period.facility
        ):
            return potential_period
    return None
