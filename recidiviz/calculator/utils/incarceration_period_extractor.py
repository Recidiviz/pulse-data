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
"""Extracts StateIncarcerationPeriod entities from BigQuery."""


from __future__ import absolute_import

from typing import Any

import apache_beam as beam
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class ExtractIncarcerationPeriods(beam.PTransform):
    """Extracts StateIncarcerationPeriod entities.

    Queries BigQuery for the given incarceration periods and returns hydrated
    StateIncarcerationPeriod entities.
    """

    # TODO: Add more query parameters
    def __init__(self, dataset):
        super(ExtractIncarcerationPeriods, self).__init__()
        self._dataset = dataset

    def expand(self, input_or_inputs):
        # TODO(1784): Implement new queries with new schema
        incarceration_periods_query = \
            f'''SELECT * FROM `{self._dataset}.booking`'''

        # Read incarceration periods from BQ and hydrate
        # StateIncarcerationPeriod entities
        return (input_or_inputs
                | 'Read StateIncarcerationPeriods' >>
                beam.io.Read(
                    beam.io.BigQuerySource(query=incarceration_periods_query,
                                           use_standard_sql=True))
                | 'Hydrate StateIncarcerationPeriod entities' >>
                beam.ParDo(HydrateIncarcerationPeriodEntity()))


@with_input_types(beam.typehints.Dict[Any, Any])
@with_output_types(beam.typehints.Tuple[int, StateIncarcerationPeriod])
class HydrateIncarcerationPeriodEntity(beam.DoFn):
    """Hydrates a StateIncarcerationPeriod entity."""

    def process(self, element, *args, **kwargs):
        """Builds a StateIncarcerationPeriod entity from key-value pairs.

        Args:
            element: Dictionary containing StateIncarcerationPeriod information.

        Yields:
            A tuple containing |person_id| and the StateIncarcerationPeriod
            entity.
        """
        # Build the incarceration_period from the values in the element

        incarceration_period = \
            StateIncarcerationPeriod.build_from_dictionary(element)

        # Who
        person_id = element.get('person_id')

        # TODO(1782): Implement incarceration_sentence_ids

        #  TODO(1780): Hydrate these cross-entity relationships as side inputs
        #   (state_person_id, incarceration_sentence_ids,
        #   supervision_sentence_ids, incarceration_incidents, parole_decisions,
        #   assessments, and source_supervision_violation_response)

        if not person_id:
            raise ValueError("No person_id associated with this "
                             "incarceration period.")

        yield (person_id, incarceration_period)

    def to_runner_api_parameter(self, unused_context):
        pass
