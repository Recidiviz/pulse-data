# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Extracts Person entities from BigQuery."""

from __future__ import absolute_import

from typing import Any

import apache_beam as beam
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.persistence.entity.state.entities import StatePerson


class ExtractPersons(beam.PTransform):
    """Extracts StatePerson entities.

    Queries BigQuery for the given persons and returns hydrated StatePerson
    entities.
    """

    # TODO: Add more query parameters
    def __init__(self, dataset):
        super(ExtractPersons, self).__init__()
        self._dataset = dataset

    def expand(self, input_or_inputs):
        # TODO(1784): Implement new queries with new schema
        person_query = f'''SELECT * FROM `{self._dataset}.person`
                                WHERE gender != 'gender' '''''

        # Read persons from BQ and hydrate StatePerson entities
        return (input_or_inputs
                | 'Read StatePersons' >>
                beam.io.Read(beam.io.BigQuerySource
                             (query=person_query,
                              use_standard_sql=True))
                | 'Hydrate StatePerson entities' >>
                beam.ParDo(HydratePersonEntity()))


@with_input_types(beam.typehints.Dict[Any, Any])
@with_output_types(beam.typehints.Tuple[int, StatePerson])
class HydratePersonEntity(beam.DoFn):
    """Hydrates a StatePerson entity."""

    def process(self, element, *args, **kwargs):
        """Builds a StatePerson entity from key-value pairs.

        Args:
            element: A dictionary containing StatePerson information.

        Yields:
            A tuple containing |person_ID| and the StatePerson entity.
        """

        # Build the person from the values in the element
        person = StatePerson.build_from_dictionary(element)

        # Cross-entity relationships to hydrate
        # TODO(1780): Implement external IDs
        # TODO(1780): Implement aliases
        # TODO(1781): Implement multiple races
        # TODO(1781): Implement multiple ethnicities
        # TODO(1782): Implement sentence_groups
        # TODO(1780): Implement assessments

        if not person.person_id:
            raise ValueError("No person_id on this person.")

        yield (person.person_id, person)

    def to_runner_api_parameter(self, unused_context):
        pass
