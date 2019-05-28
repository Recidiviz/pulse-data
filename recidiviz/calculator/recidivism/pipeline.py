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

"""Runs the recidivism calculation pipeline.

usage: pipeline.py --output=OUTPUT_LOCATION --project=PROJECT
                    --dataset=DATASET --methodology=METHODOLOGY
                    [--include_age] [--include_gender]
                    [--include_race] [--include_release_facility]
                    [--include_stay_length] [--release_count_min]
                    [--include_revocation_returns]

Example output to GCP storage bucket:
python -m recidiviz.calculator.recidivism.pipeline
        --project=recidiviz-project-name
        --dataset=recidiviz-project-name.dataset
        --output=gs://recidiviz-bucket/output_location
            --methodology=BOTH

Example output to local file:
python -m recidiviz.calculator.recidivism.pipeline
        --project=recidiviz-project-name
        --dataset=recidiviz-project-name.dataset
        --output=output_file --methodology=PERSON

Example output including race and gender dimensions:
python -m recidiviz.calculator.recidivism.pipeline
        --project=recidiviz-project-name
        --dataset=recidiviz-project-name.dataset
        --output=output_file --methodology=EVENT
            --include_race=True --include_gender=True

"""

from __future__ import absolute_import

import argparse
import logging

from datetime import date
from typing import Any, Dict, List

import apache_beam as beam
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.calculator.recidivism import identifier, calculator, \
    RecidivismEvent, RecidivismMetric
from recidiviz.calculator.recidivism.metrics import Methodology
from recidiviz.common.constants.person_characteristics import Gender, \
    ResidencyStatus
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, \
    StateIncarcerationFacilitySecurityLevel
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StateIncarcerationPeriod


@with_input_types(beam.typehints.Dict[Any, Any])
@with_output_types(beam.typehints.Tuple[int, StatePerson])
class ExtractPerson(beam.DoFn):
    """Extraction of person entities."""

    def process(self, element, *args, **kwargs):
        """ Builds a person entity from a collection of
        corresponding key-value pairs.

        Args:
            element: A dictionary containing StatePerson information.

        Yields:
            A tuple containing |person_ID| and the StatePerson entity.
        """

        person_builder = StatePerson.builder()

        person = person_builder

        # Build the person from the values in the element

        # External IDs
        # TODO(1780): Implement external IDs

        # Residency
        person.residency_status = ResidencyStatus. \
            parse_from_canonical_string(element.get('residency_status'))
        person.current_address = element.get('current_address')

        # Names
        person.full_name = element.get('full_name')
        # TODO(1780): Implement aliases

        # Birth Date
        person.birthdate = element.get('birthdate')
        person.birthdate_inferred_from_age = \
            element.get('birthdate_inferred_from_age')

        # Gender
        person.gender = \
            Gender.parse_from_canonical_string(element.get('gender'))

        # TODO(1781): Implement multiple races
        # TODO(1781): Implement multiple ethnicities

        # ID
        person.person_id = element.get('person_id')

        # TODO(1782): Implement sentence_groups

        # TODO(1780): Implement assessments

        if person.person_id:
            # Only return a StatePerson if we have a valid person_id for them
            yield (person.person_id, person.build())

    def to_runner_api_parameter(self, unused_context):
        pass


@with_input_types(beam.typehints.Dict[Any, Any])
@with_output_types(beam.typehints.Tuple[int, StateIncarcerationPeriod])
class ExtractIncarcerationPeriod(beam.DoFn):
    """Extraction of StateIncarcerationPeriod entity."""

    def process(self, element, *args, **kwargs):
        """Builds a StateIncarcerationPeriod entity from a collection of
        corresponding key-value pairs.

        Args:
            element: Dictionary containing StateIncarcerationPeriod information.

        Yields:
            A tuple containing |person_id| and the StateIncarcerationPeriod
            entity.
        """

        incarceration_period_builder = StateIncarcerationPeriod.builder()

        incarceration_period = incarceration_period_builder

        # Build the incarceration_period from the values in the element

        # IDs
        incarceration_period.external_id = element.get('external_id')
        incarceration_period.incarceration_period_id = \
            element.get('incarceration_period_id')

        # Status
        incarceration_period.status = \
            StateIncarcerationPeriodStatus.parse_from_canonical_string(
                element.get('status'))
        # TODO(1801): Don't hydrate raw text fields in calculate code
        incarceration_period.status_raw_text = element.get('status_raw_text')

        # Type
        incarceration_period.incarceration_type = \
            StateIncarcerationType.parse_from_canonical_string(
                element.get('incarceration_type'))
        incarceration_period.incarceration_type_raw_text = \
            element.get('incarceration_type_raw_text')

        # When
        incarceration_period.admission_date = element.get('admission_date')
        incarceration_period.release_date = element.get('release_date')

        # Where
        incarceration_period.state_code = element.get('state_code')
        incarceration_period.county_code = element.get('county_code')
        incarceration_period.facility = element.get('facility')
        incarceration_period.housing_unit = element.get('housing_unit')

        # What
        incarceration_period.facility_security_level = \
            StateIncarcerationFacilitySecurityLevel.parse_from_canonical_string(
                element.get('facility_security_level'))
        incarceration_period.facility_security_level_raw_text = \
            element.get('facility_security_level_raw_text')

        incarceration_period.admission_reason = \
            StateIncarcerationPeriodAdmissionReason.parse_from_canonical_string(
                element.get('admission_reason'))
        incarceration_period.admission_reason_raw_text = \
            element.get('admission_reason_raw_text')

        incarceration_period.projected_release_reason = \
            StateIncarcerationPeriodReleaseReason.parse_from_canonical_string(
                element.get('projected_release_reason'))
        incarceration_period.projected_release_reason_raw_text = \
            element.get('projected_release_reason_raw_text')

        incarceration_period.release_reason = \
            StateIncarcerationPeriodReleaseReason.parse_from_canonical_string(
                element.get('release_reason'))
        incarceration_period.release_reason_raw_text = \
            element.get('release_reason_raw_text')

        # Who
        incarceration_period.state_person_id = element.get('state_person_id')

        # TODO(1782): Implement incarceration_sentence_ids

        #  TODO(1780): Hydrate these cross-entity relationships as side inputs
        #   (state_person_id, incarceration_sentence_ids,
        #   supervision_sentence_ids, incarceration_incidents, parole_decisions,
        #   assessments, and source_supervision_violation_response)

        if incarceration_period.state_person_id:
            # Only return the StateIncarcerationPeriod if we have a valid
            # person_id to connect it to
            yield (incarceration_period.state_person_id,
                   incarceration_period.build())

    def to_runner_api_parameter(self, unused_context):
        pass


@with_input_types(beam.typehints.Tuple[int, Dict[str, Any]],
                  **{'include_revocation_returns': bool})
@with_output_types(beam.typehints.Tuple[
    int, Dict[int, List[RecidivismEvent]]])
class FindRecidivism(beam.DoFn):
    """Transform for finding recidivism and non-recidivism."""

    def process(self, element, *args, **kwargs):
        """Sends the identifier the sorted StateIncarcerationPeriods for
        identifying instances of recidivism and non-recidivism.

        Args:
            element: Tuple containing person_id and a dictionary with
                a StatePerson and a list of StateIncarcerationPeriods
            **kwargs: This should be a dictionary with values for the following
                keys:
                    - include_revocation_returns

        Yields:
            Tuple containing |person_id| and a collection
            of RecidivismEvents.
        """

        include_revocation_returns = kwargs['include_revocation_returns']

        person_id, person_incarceration_periods = element

        # Get the StateIncarcerationPeriods as a list
        incarceration_periods = \
            list(person_incarceration_periods['incarceration_periods'])

        # Sort StateIncarcerationPeriods by admission date
        incarceration_periods.sort(key=lambda b: b.admission_date)

        # Identify the RecidivismEvents from the StateIncarcerationPeriods
        recidivism_events = \
            identifier.find_recidivism(incarceration_periods,
                                       include_revocation_returns)

        yield (person_id, recidivism_events)

    def to_runner_api_parameter(self, unused_context):
        pass


@with_input_types(beam.typehints.Tuple[int, Dict[str, Any]])
@with_output_types(beam.typehints.Tuple[Dict[str, Any], Any])
class MapMetricCombinations(beam.DoFn):
    """Transform for calculating recidivism metric combinations
    for a StatePerson and their RecidivismEvents."""

    def process(self, element, *args, **kwargs):
        """Sends the calculator the person entity and corresponding
        RecidivismEvents for mapping all recidivism combinations.

        Args:
            element: Tuple containing a |person_id| and a dictionary storing
             a StatePerson and their RecidivismEvents

        Yields:
            Each recidivism metric combination.
        """

        _, person_events = element

        # Get the person if they exist
        person_list = list(person_events['person'])

        if not person_list or len(person_list) != 1:
            return

        person = person_list[0]

        # Get the recidivism events if they exist
        grouped_recidivism_events_list = \
            list(person_events['recidivism_events'])

        if not grouped_recidivism_events_list or \
                len(grouped_recidivism_events_list) != 1:
            return

        grouped_recidivism_events = grouped_recidivism_events_list[0]

        # Calculate recidivism metric combinations for this person and events
        metric_combinations = \
            calculator.map_recidivism_combinations(person,
                                                   grouped_recidivism_events)

        # Return each of the recidivism metric combinations
        for metric_combination in metric_combinations:
            yield metric_combination

    def to_runner_api_parameter(self, unused_context):
        pass


@with_input_types(beam.typehints.Tuple[
    Dict[str, str], List[int]])
@with_output_types(RecidivismMetric)
class ProduceRecidivismMetric(beam.DoFn):
    """Transform for producing RecidivismMetrics ."""

    def process(self, element, *args, **kwargs):
        """Populates a RecidivismMetric object with values from
        the metric_key.

        Args:
            element: A tuple containing a dictionary of the metric_key for a
            given recidivism metric, and the list of 1s and 0s representing
            the recidivism and non-recidivism events corresponding to this
            metric.

        Yields:
            The RecidivismMetric.
        """

        (metric_key, group) = element

        # Calculate number of releases and number resulting in recidivism
        total_releases = len(list(group))
        total_returns = sum(group)

        if total_releases == 0:
            return

        if not metric_key:
            return

        # Calculate recidivism rate
        recidivism_rate = ((total_returns + 0.0) / total_releases)

        # Build RecidivismMetric object from metric_key
        recidivism_metric = RecidivismMetric()
        # TODO(1789): Implement pipeline execution_id
        recidivism_metric.execution_id = 12345
        recidivism_metric.total_releases = total_releases
        recidivism_metric.total_returns = total_returns
        recidivism_metric.recidivism_rate = recidivism_rate

        recidivism_metric.release_cohort = metric_key['release_cohort']
        recidivism_metric.follow_up_period = metric_key['follow_up_period']
        recidivism_metric.methodology = metric_key['methodology']
        recidivism_metric.created_on = date.today()

        if 'age' in metric_key:
            recidivism_metric.age_bucket = metric_key['age']
        if 'race' in metric_key:
            recidivism_metric.race = metric_key['race']
        if 'gender' in metric_key:
            recidivism_metric.gender = metric_key['gender']
        if 'release_facility' in metric_key:
            recidivism_metric.release_facility = metric_key['release_facility']
        if 'stay_length' in metric_key:
            recidivism_metric.stay_length_bucket = metric_key['stay_length']
        if 'return_type' in metric_key:
            recidivism_metric.return_type = metric_key['return_type']

        yield recidivism_metric

    def to_runner_api_parameter(self, unused_context):
        pass


@with_input_types(RecidivismMetric,
                  **{'dimensions_to_filter_out': [str],
                     'methodology': Methodology,
                     'release_count_min': int})
@with_output_types(RecidivismMetric)
class FilterMetrics(beam.DoFn):
    """Filters out metrics that should not be included in the output."""

    def process(self, element, *args, **kwargs):
        """Returns the RecidivismMetric if it should be included in the output.

            Args:
                element: A RecidivismMetric object
                **kwargs: This should be a dictionary with values for the
                    following keys:
                        - dimensions_to_filter_out: List of dimensions to filter
                             from the output.
                        - methodology: Methodology to report.
                        - release_count_min: Minimum number of releases in the
                            metric to be included in the output.

            Yields:
                The RecidivismMetric.
        """

        dimensions_to_filter_out = kwargs['dimensions_to_filter_out']
        methodology = kwargs['methodology']
        release_count_min = kwargs['release_count_min']

        recidivism_metric = element

        metric_dict = recidivism_metric.__dict__

        # Filter out unwanted dimensions
        for dimension in dimensions_to_filter_out:
            value = metric_dict.get(dimension)

            if value is not None:
                return

        # Filter out unwanted methodologies
        if methodology != Methodology.BOTH:
            if recidivism_metric.methodology != methodology:
                return

        # Filter metrics that have less than release_count_min releases
        if recidivism_metric.total_releases < release_count_min:
            return

        yield recidivism_metric

    def to_runner_api_parameter(self, unused_context):
        pass


def parse_arguments(argv):
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser()

    # Parse arguments
    parser.add_argument('--dataset',
                        dest='dataset',
                        type=str,
                        help='BigQuery data set to query.',
                        required=True)

    parser.add_argument('--include_age',
                        dest='include_age',
                        type=bool,
                        help='Include metrics broken down by age.',
                        default=False)

    parser.add_argument('--include_gender',
                        dest='include_gender',
                        type=bool,
                        help='Include metrics broken down by gender.',
                        default=False)

    parser.add_argument('--include_race',
                        dest='include_race',
                        type=bool,
                        help='Include metrics broken down by race.',
                        default=False)

    parser.add_argument('--include_release_facility',
                        dest='include_release_facility',
                        type=bool,
                        help='Include metrics broken down by release facility.',
                        default=True)

    parser.add_argument('--include_revocation_returns',
                        dest='include_revocation_returns',
                        type=bool,
                        help='Whether or not to include incarceration returns '
                             'caused by supervision revocation in the '
                             'recidivism calculations.',
                        default=False)

    parser.add_argument('--include_stay_length',
                        dest='include_stay_length',
                        type=bool,
                        help='Include metrics broken down by stay length.',
                        default=False)

    parser.add_argument('--methodology',
                        dest='methodology',
                        type=str,
                        choices=['PERSON', 'EVENT', 'BOTH'],
                        help='PERSON, EVENT, or BOTH',
                        required=True)

    parser.add_argument('--release_count_min',
                        dest='release_count_min',
                        type=int,
                        help='Filter metrics with total release count less '
                             'than this.',
                        default=0)

    parser.add_argument('--output',
                        dest='output',
                        type=str,
                        help='Output location to write results to.',
                        required=True)

    return parser.parse_known_args(argv)


def dimensions_to_filter(known_args):
    """Identifies dimensions to filter from output.

        Args:
            known_args: Arguments identified by the argument parsers.

        Returns: List of dimensions to filter from output.
    """

    dimensions = []

    if not known_args.include_age:
        dimensions.append('age_bucket')

    if not known_args.include_gender:
        dimensions.append('gender')

    # TODO(1781): Fix to be races and include ethnicities
    if not known_args.include_race:
        dimensions.append('race')

    if not known_args.include_release_facility:
        dimensions.append('release_facility')

    if not known_args.include_stay_length:
        dimensions.append('stay_length_bucket')

    return dimensions


def run(argv=None):
    """Runs the recidivism calculation pipeline."""

    # Parse command-line arguments
    known_args, pipeline_args = parse_arguments(argv)

    # Extend pipeline arguments
    pipeline_args.extend([
        # TODO(1783): Check ability to specify Python version with
        #  DataflowRunner
        # Change this to DataflowRunner to
        # run the pipeline on the Google Cloud Dataflow Service.
        '--runner=DirectRunner',
        # '--runner=DataflowRunner',
    ])

    with beam.Pipeline(argv=pipeline_args) as p:
        # Query for all persons and incarceration periods in
        # recidiviz-123.recidiviz_state_scratch_space

        # TODO(1784): Implement new queries with new schema
        person_query = f'''SELECT * FROM `{known_args.dataset}.person`
                                WHERE gender != 'gender' '''''

        incarceration_periods_query = \
            f'''SELECT * FROM `{known_args.dataset}.booking`'''

        # Read persons from BQ and load them into StatePerson entities
        persons = (p
                   | 'Read Persons' >> beam.io.Read(beam.io.BigQuerySource
                                                    (query=person_query,
                                                     use_standard_sql=True))
                   | 'Load StatePerson entities' >> beam.ParDo(ExtractPerson()))

        # Read incarceration periods from BQ and load them into
        # StateIncarcerationPeriod entities
        incarceration_periods = (p
                                 | 'Read StateIncarceration Periods' >>
                                 beam.io.Read(
                                     beam.io.BigQuerySource
                                     (query=incarceration_periods_query,
                                      use_standard_sql=True))
                                 | 'Load StateIncarcerationPeriod entities' >>
                                 beam.ParDo(ExtractIncarcerationPeriod()))

        # Group each StatePerson with their StateIncarcerationPeriods
        person_and_incarceration_periods = \
            ({'person': persons, 'incarceration_periods': incarceration_periods}
             | 'Group StatePerson to StateIncarcerationPeriods' >>
             beam.CoGroupByKey()
             )

        find_recidivism_kwargs = {'include_revocation_returns':
                                  known_args.include_revocation_returns}

        # Find recidivism events from the StatePerson's
        # StateIncarcerationPeriods
        recidivism_events = (person_and_incarceration_periods
                             | 'Find Recidivism Events' >>
                             beam.ParDo(FindRecidivism(),
                                        **find_recidivism_kwargs))

        # Group each StatePerson with their RecidivismEvents
        person_events = ({'person': persons,
                          'recidivism_events': recidivism_events}
                         | 'Group StatePerson to RecidivismEvents' >>
                         beam.CoGroupByKey())

        # Map each person and their events to the combinations of metrics
        # Note: We are preventing fusion here by having a GroupByKey in between
        # this high fan-out ParDo and the metric ParDo
        mapped_metric_combinations = (person_events
                                      | 'Map to metric combinations' >>
                                      beam.ParDo(MapMetricCombinations())
                                      )

        # Group metrics that have the same metric_key
        grouped_metric_combinations = (mapped_metric_combinations
                                       | 'Group metrics by metric key' >>
                                       beam.GroupByKey())

        # Build RecidivismMetric objects
        metrics = (grouped_metric_combinations
                   | 'Produce recidivism metrics' >>
                   beam.ParDo(ProduceRecidivismMetric()))

        # Get dimensions to filter out
        dimensions = dimensions_to_filter(known_args)

        filter_metrics_kwargs = {
            'dimensions_to_filter_out': dimensions,
            'methodology': Methodology[known_args.methodology],
            'release_count_min': known_args.release_count_min}

        # Filter out unneeded metrics
        filtered_metrics = (metrics
                            | 'Filter out unwanted metrics' >>
                            beam.ParDo(FilterMetrics(),
                                       **filter_metrics_kwargs))

        # Write results to the output sink
        _ = (filtered_metrics
             | 'Write Results' >> beam.io.WriteToText(known_args.output))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
