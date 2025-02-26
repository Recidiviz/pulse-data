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
"""Utils for using StatePerson entities in calculations."""

from typing import List, Optional, Union, Tuple, Dict, Iterable, Any, cast

import attr
import apache_beam as beam
from more_itertools import one

from recidiviz.calculator.pipeline.utils.event_utils import IdentifierEvent
from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.person_characteristics import Race, Ethnicity
from recidiviz.persistence.entity.state.entities import StatePerson

# Race and ethnicity values that we do not track in the state_race_ethnicity_population_counts table
_NON_PRIORITIZED_RACES_OR_ETHNICITIES: List[Union[Race, Ethnicity]] = [
    Race.EXTERNAL_UNKNOWN,
    Ethnicity.EXTERNAL_UNKNOWN,
    Ethnicity.NOT_HISPANIC
]


@attr.s(frozen=True)
class StateRaceEthnicityPopulationCounts(BuildableAttr):
    """Stores counts of populations for races and ethnicities in a state, along with the priority for this race or
    ethnicity to be represented in metrics."""
    # The state described by the count
    state_code: str = attr.ib()  # non-nullable

    # The race or ethnicity group in the state
    race_or_ethnicity: str = attr.ib()  # non-nullable

    # The number of people in the state with this race or ethnicity
    population_count: int = attr.ib()  # non-nullable

    # The ranked representation priority for this race or ethnicity in this state
    representation_priority: int = attr.ib()  # non-nullable


@attr.s(frozen=True)
class PersonMetadata(BuildableAttr):
    """Stores information about the StatePerson that is necessary for the metrics."""

    # The race or ethnicity value of a person that is least represented in the state’s population
    prioritized_race_or_ethnicity: Optional[str] = attr.ib(default=None)


class BuildPersonMetadata(beam.DoFn):
    """Produces a PersonMetadata object storing information about the given StatePerson."""

    # pylint: disable=arguments-differ
    def process(self,
                person_id_person: Tuple[int, BuildableAttr],
                state_race_ethnicity_population_counts: List[Dict[str, Any]]) -> Iterable[Tuple[int, PersonMetadata]]:
        """Returns a tuple containing the person_id and the PersonMetadata containing information about the given
        StatePerson."""
        person_id, person = person_id_person

        person = cast(StatePerson, person)

        race_ethnicity_population_counts: List[StateRaceEthnicityPopulationCounts] = [
            cast(StateRaceEthnicityPopulationCounts,
                 StateRaceEthnicityPopulationCounts.build_from_dictionary(state_race_ethnicity_population_count))
            for state_race_ethnicity_population_count in state_race_ethnicity_population_counts
        ]

        person_metadata = build_person_metadata(person, race_ethnicity_population_counts)

        yield person_id, person_metadata

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


class ExtractPersonEventsMetadata(beam.DoFn):
    # pylint: disable=arguments-differ
    def process(self,
                person_event_element: Tuple[int, Dict[str, Iterable[Any]]]) -> \
            Iterable[Tuple[StatePerson, List[IdentifierEvent], PersonMetadata]]:
        """Extracts the StatePerson, PersonMetadata, and list of pipeline-specific events for use in the calculator
        step of the pipeline."""
        _, element_data = person_event_element

        person_events = element_data.get('person_events')
        person_metadata_group = element_data.get('person_metadata')

        # If there isn't a person associated with this person_id_person, continue
        if person_events and person_metadata_group:
            person, events = one(person_events)
            person_metadata = one(person_metadata_group)

            yield person, events, person_metadata

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


def build_person_metadata(
        person: StatePerson, state_race_ethnicity_population_counts: List[StateRaceEthnicityPopulationCounts]) -> \
        PersonMetadata:
    """Loads a PersonMetadata object with information about the StatePerson that is necessary for the calculations."""
    prioritized_race_or_ethnicity = determine_prioritized_race_or_ethnicity(person,
                                                                            state_race_ethnicity_population_counts)

    return PersonMetadata(prioritized_race_or_ethnicity=prioritized_race_or_ethnicity)


def determine_prioritized_race_or_ethnicity(
        person: StatePerson, state_race_ethnicity_population_counts: List[StateRaceEthnicityPopulationCounts]) -> \
        Optional[str]:
    """Identifies the person's racial or ethnic identity that is least represented in the state's population, if
    applicable. Raises an error if there are no race/ethnicity population values for the state_code of the
    StatePerson."""
    state_code = person.state_code

    state_specific_population_counts = [
        state_race_ethnicity_population_count
        for state_race_ethnicity_population_count in state_race_ethnicity_population_counts
        if state_race_ethnicity_population_count.state_code == state_code
    ]

    if not state_specific_population_counts:
        raise ValueError(f"Missing race and ethnicity population values for {state_code}.")

    race_ethnicity_values = [
        race.race.value for race in person.races
        if race.race and race.race not in _NON_PRIORITIZED_RACES_OR_ETHNICITIES
    ]

    race_ethnicity_values.extend([
        ethnicity.ethnicity.value for ethnicity in person.ethnicities
        if ethnicity.ethnicity and ethnicity.ethnicity not in _NON_PRIORITIZED_RACES_OR_ETHNICITIES
    ])

    if race_ethnicity_values:
        relevant_state_race_ethnicity_population_counts = [
            state_race_ethnicity_population_count
            for state_race_ethnicity_population_count in state_specific_population_counts
            if state_race_ethnicity_population_count.race_or_ethnicity in race_ethnicity_values
        ]

        if not relevant_state_race_ethnicity_population_counts:
            raise ValueError(f"Unsupported race/ethnicity values: {race_ethnicity_values}")

        # A representation_priority value of 1 means it is the least represented race/ethnicity in the state, resulting
        # in the greatest importance of being represented in metrics. For this reason, we sort in descending order of
        # representation_priority.
        relevant_state_race_ethnicity_population_counts.sort(key=lambda b: b.representation_priority)
        return str(relevant_state_race_ethnicity_population_counts[0].race_or_ethnicity)

    return None
