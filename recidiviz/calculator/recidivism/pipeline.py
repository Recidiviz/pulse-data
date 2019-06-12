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

from typing import Any, Dict, List, Tuple
from more_itertools import one

import apache_beam as beam
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.calculator.recidivism import identifier, calculator, \
    ReincarcerationRecidivismMetric
from recidiviz.calculator.recidivism.release_event import ReleaseEvent
from recidiviz.calculator.recidivism.metrics import RecidivismMethodologyType
from recidiviz.calculator.utils import person_extractor, \
    incarceration_period_extractor
from recidiviz.persistence.entity.state.entities import StatePerson


@with_input_types(beam.typehints.Tuple[int, Dict[str, Any]])
@with_output_types(beam.typehints.Tuple[StatePerson, List[ReleaseEvent]])
class GetReleaseEvents(beam.PTransform):
    """Transforms a StatePerson and their IncarcerationPeriods into
    ReleaseEvents."""

    def __init__(self):
        super(GetReleaseEvents, self).__init__()

    def expand(self, input_or_inputs):
        return (input_or_inputs
                | beam.ParDo(ClassifyReleaseEvents()))


@with_input_types(beam.typehints.Tuple[StatePerson, List[ReleaseEvent]])
@with_output_types(ReincarcerationRecidivismMetric)
class GetRecidivismMetrics(beam.PTransform):
    """Transforms a StatePerson and ReleaseEvents into RecidivismMetrics."""

    def __init__(self):
        super(GetRecidivismMetrics, self).__init__()

    def expand(self, input_or_inputs):
        # Calculate recidivism metric combinations from a StatePerson and their
        # ReleaseEvents
        recidivism_metric_combinations = (
            input_or_inputs
            | 'Map to metric combinations' >>
            beam.ParDo(CalculateRecidivismMetricCombinations()))

        # Group metrics that have the same metric_key
        # Note: We are preventing fusion here by having a GroupByKey in between
        # this high fan-out ParDo and the metric ParDo
        grouped_metric_combinations = (recidivism_metric_combinations
                                       | 'Group metrics by metric key' >>
                                       beam.GroupByKey())

        # Return ReincarcerationRecidivismMetric objects built from grouped
        # metric_keys
        return (grouped_metric_combinations
                | 'Produce recidivism metrics' >>
                beam.ParDo(ProduceReincarcerationRecidivismMetric()))


@with_input_types(beam.typehints.Tuple[int, Dict[str, Any]])
@with_output_types(beam.typehints.Tuple[StatePerson,
                                        Dict[int, List[ReleaseEvent]]])
class ClassifyReleaseEvents(beam.DoFn):
    """Classifies releases as either recidivism or non-recidivism events."""

    def process(self, element, *args, **kwargs):
        """ Identifies instances of recidivism and non-recidivism.

        Sends the identifier the StateIncarcerationPeriods for a given
        StatePerson, which returns a list of ReleaseEvents for each year the
        individual was released from incarceration.

        Args:
            element: Tuple containing person_id and a dictionary with
                a StatePerson and a list of StateIncarcerationPeriods

        Yields:
            Tuple containing the StatePerson and a collection
            of ReleaseEvents.
        """

        _, person_incarceration_periods = element

        # Get the StateIncarcerationPeriods as a list
        incarceration_periods = \
            list(person_incarceration_periods['incarceration_periods'])

        # Get the StatePerson
        person = one(person_incarceration_periods['person'])

        # Find the ReleaseEvents from the StateIncarcerationPeriods
        release_events_by_cohort_year = \
            identifier.find_release_events_by_cohort_year(
                incarceration_periods)

        yield (person, release_events_by_cohort_year)

    def to_runner_api_parameter(self, unused_context):
        pass


@with_input_types(beam.typehints.Tuple[StatePerson,
                                       Dict[int, List[ReleaseEvent]]])
@with_output_types(beam.typehints.Tuple[Dict[str, Any], Any])
class CalculateRecidivismMetricCombinations(beam.DoFn):
    """Calculates recidivism metric combinations."""

    def process(self, element, *args, **kwargs):
        """Produces various recidivism metric combinations.

        Sends the calculator the StatePerson entity and their corresponding
        ReleaseEvents for mapping all recidivism combinations.

        Args:
            element: Tuple containing a StatePerson and their ReleaseEvents

        Yields:
            Each recidivism metric combination.
        """

        person, release_events = element

        # Calculate recidivism metric combinations for this person and events
        metric_combinations = \
            calculator.map_recidivism_combinations(person,
                                                   release_events)

        # Return each of the recidivism metric combinations
        for metric_combination in metric_combinations:
            yield metric_combination

    def to_runner_api_parameter(self, unused_context):
        pass


@with_input_types(beam.typehints.Tuple[
    Dict[str, str], List[int]])
@with_output_types(ReincarcerationRecidivismMetric)
class ProduceReincarcerationRecidivismMetric(beam.DoFn):
    """Produces ReincarcerationRecidivismMetrics."""

    def process(self, element, *args, **kwargs):
        """Converts a recidivism metric key into a
        ReincarcerationRecidivismMetric.

        Args:
            element: A tuple containing a dictionary of the metric_key for a
            given recidivism metric, and a list where each element corresponds
            to a ReleaseEvent, with 1s representing RecidivismReleaseEvents, and
            0s representing NonRecidivismReleaseEvents.

        Yields:
            The ReincarcerationRecidivismMetric.
        """

        (metric_key, release_group) = element

        recidivism_metric = \
            ReincarcerationRecidivismMetric.build_from_metric_key_release_group(
                metric_key,
                release_group)

        if recidivism_metric:
            yield recidivism_metric

    def to_runner_api_parameter(self, unused_context):
        pass


@with_input_types(ReincarcerationRecidivismMetric,
                  **{'dimensions_to_filter_out': [str],
                     'methodologies': List[RecidivismMethodologyType],
                     'release_count_min': int})
@with_output_types(ReincarcerationRecidivismMetric)
class FilterMetrics(beam.DoFn):
    """Filters out metrics that should not be included in the output."""

    def process(self, element, *args, **kwargs):
        """Returns the ReincarcerationRecidivismMetric if it should be included
         in the output.

            Args:
                element: A ReincarcerationRecidivismMetric object
                **kwargs: This should be a dictionary with values for the
                    following keys:
                        - dimensions_to_filter_out: List of dimensions to filter
                            from the output.
                        - methodologies: The RecidivismMethodologyTypes to
                            report.
                        - release_count_min: Minimum number of releases in the
                            metric to be included in the output.

            Yields:
                The ReincarcerationRecidivismMetric.
        """

        dimensions_to_filter_out = kwargs['dimensions_to_filter_out']
        methodologies = kwargs['methodologies']
        release_count_min = kwargs['release_count_min']

        recidivism_metric = element

        metric_dict = recidivism_metric.__dict__

        # Filter out unwanted dimensions
        for dimension in dimensions_to_filter_out:
            value = metric_dict.get(dimension)

            if value is not None:
                return

        # Filter out unwanted methodologies
        if recidivism_metric.methodology not in methodologies:
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


def dimensions_and_methodologies(known_args) -> \
        Tuple[List[str], List[RecidivismMethodologyType]]:
    """Identifies dimensions to filter from output, and the methodologies of
    counting recidivism to use.

        Args:
            known_args: Arguments identified by the argument parsers.

        Returns: A tuple containing the list of dimensions to filter from
            the output, and a list of methodologies to use.
    """

    dimensions = []

    # TODO(1781): Fix race to be races and include ethnicities
    filterable_dimensions_map = {
        'include_age': 'age_bucket',
        'include_gender': 'gender',
        'include_race': 'race',
        'include_release_facility': 'release_facility',
        'include_stay_length': 'stay_length_bucket'}

    known_args_dict = vars(known_args)

    for dimension_key in filterable_dimensions_map:
        if not known_args_dict[dimension_key]:
            dimensions.append(filterable_dimensions_map[dimension_key])

    methodologies = []

    if known_args.methodology == 'BOTH':
        methodologies.append(RecidivismMethodologyType.EVENT)
        methodologies.append(RecidivismMethodologyType.PERSON)
    else:
        methodologies.append(RecidivismMethodologyType[known_args.methodology])

    return dimensions, methodologies


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
        # '--runner=DirectRunner',
        '--runner=DataflowRunner'
    ])

    with beam.Pipeline(argv=pipeline_args) as p:
        # Get StatePersons
        persons = (p
                   | 'Get hydrated StatePersons' >>
                   person_extractor.ExtractPersons(dataset=known_args.dataset))

        # Get StateIncarcerationPeriods
        incarceration_periods = (
            p
            | 'Get hydrated IncarcerationPeriods' >>
            incarceration_period_extractor.ExtractIncarcerationPeriods(
                dataset=known_args.dataset))

        # Group each StatePerson with their StateIncarcerationPeriods
        person_and_incarceration_periods = (
            {'person': persons,
             'incarceration_periods': incarceration_periods}
            | 'Group StatePerson to StateIncarcerationPeriods' >>
            beam.CoGroupByKey()
        )
        # Identify ReleaseEvents events from the StatePerson's

        # StateIncarcerationPeriods
        person_events = (
            person_and_incarceration_periods |
            'Get Recidivism Events' >>
            GetReleaseEvents())

        recidivism_metrics = (person_events
                              | 'Get Recidivism Metrics' >>
                              GetRecidivismMetrics())

        # Get dimensions to filter out and methodologies to use
        dimensions, methodologies = dimensions_and_methodologies(known_args)

        filter_metrics_kwargs = {
            'dimensions_to_filter_out': dimensions,
            'methodologies': methodologies,
            'release_count_min': known_args.release_count_min}

        # Filter out unneeded metrics
        filtered_recidivism_metrics = (
            recidivism_metrics
            | 'Filter out unwanted metrics' >>
            beam.ParDo(FilterMetrics(), **filter_metrics_kwargs))

        # Write results to the output sink
        _ = (filtered_recidivism_metrics
             | 'Write Results' >> beam.io.WriteToText(known_args.output))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
