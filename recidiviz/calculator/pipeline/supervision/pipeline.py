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
"""Runs the supervision calculation pipeline.

usage: pipeline.py --output=OUTPUT_LOCATION --project=PROJECT
                    --dataset=DATASET --methodology=METHODOLOGY
                    [--include_age] [--include_gender]
                    [--include_race] [--include_ethnicity]

Example output to GCP storage bucket:
python -m recidiviz.calculator.supervision.pipeline
        --project=recidiviz-project-name
        --dataset=recidiviz-project-name.dataset
        --output=gs://recidiviz-bucket/output_location
            --methodology=BOTH

Example output to local file:
python -m recidiviz.calculator.supervision.pipeline
        --project=recidiviz-project-name
        --dataset=recidiviz-project-name.dataset
        --output=output_file --methodology=PERSON

Example output including race and gender dimensions:
python -m recidiviz.calculator.supervision.pipeline
        --project=recidiviz-project-name
        --dataset=recidiviz-project-name.dataset
        --output=output_file --methodology=EVENT
            --include_race=True --include_gender=True

"""
import argparse
import datetime
import json
import logging
from typing import Dict, Any, List, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.pvalue import AsDict
from apache_beam.typehints import with_input_types, with_output_types
from more_itertools import one

from recidiviz.calculator.pipeline.supervision import identifier, calculator
from recidiviz.calculator.pipeline.supervision.metrics import \
    SupervisionMetric, SupervisionPopulationMetric,\
    SupervisionRevocationMetric, SupervisionSuccessMetric
from recidiviz.calculator.pipeline.supervision.metrics import \
    SupervisionMetricType as MetricType
from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    SupervisionTimeBucket
from recidiviz.calculator.pipeline.utils.beam_utils import SumFn, \
    SupervisionSuccessFn
from recidiviz.calculator.pipeline.utils.entity_hydration_utils import \
    SetViolationResponseOnIncarcerationPeriod
from recidiviz.calculator.pipeline.utils.execution_utils import get_job_id
from recidiviz.calculator.pipeline.utils.extractor_utils import BuildRootEntity
from recidiviz.calculator.pipeline.utils.metric_utils import \
    json_serializable_metric_key, MetricMethodologyType
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state import entities
from recidiviz.utils import environment

# Cached job_id value
_job_id = None


def job_id(pipeline_options: Dict[str, str]) -> str:
    global _job_id
    if not _job_id:
        _job_id = get_job_id(pipeline_options)
    return _job_id


@environment.test_only
def clear_job_id():
    global _job_id
    _job_id = None


@with_input_types(beam.typehints.Dict[str, Any], str)
@with_output_types(beam.typehints.Tuple[Any, Dict[str, Any]])
class ConvertDictToKVTuple(beam.DoFn):
    """Converts a dictionary into a key value tuple by extracting a value from
     the dictionary and setting it as the key."""

    #pylint: disable=arguments-differ
    def process(self, element, key):
        key_value = element.get(key)

        if key_value:
            yield (key_value, element)

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


@with_input_types(beam.typehints.Tuple[entities.StatePerson,
                                       List[SupervisionTimeBucket]])
@with_output_types(SupervisionMetric)
class GetSupervisionMetrics(beam.PTransform):
    """Transforms a StatePerson and their SupervisionTimeBuckets into
    SupervisionMetrics."""

    def __init__(self, pipeline_options: Dict[str, str],
                 inclusions: Dict[str, bool]):
        super(GetSupervisionMetrics, self).__init__()
        self._pipeline_options = pipeline_options
        self.inclusions = inclusions

    def expand(self, input_or_inputs):
        # Calculate supervision metric combinations from a StatePerson and their
        # SupervisionTimeBuckets
        supervision_metric_combinations = (
            input_or_inputs
            | 'Map to metric combinations' >>
            beam.ParDo(CalculateSupervisionMetricCombinations(),
                       **self.inclusions).with_outputs('populations',
                                                       'revocations',
                                                       'successes'))

        # Calculate the supervision population values for the metrics combined
        # by key
        populations_with_sums = (supervision_metric_combinations.populations
                                 | 'Calculate supervision population values' >>
                                 beam.CombinePerKey(SumFn()))

        # Calculate the revocation count values for metrics combined by key
        revocations_with_sums = (supervision_metric_combinations.revocations
                                 | 'Calculate supervision revocation values' >>
                                 beam.CombinePerKey(SumFn()))

        successes_with_sums = (supervision_metric_combinations.successes
                               | 'Calculate the supervision success values' >>
                               beam.CombinePerKey(SupervisionSuccessFn()))

        # Produce the SupervisionPopulationMetrics
        population_metrics = (populations_with_sums
                              | 'Produce supervision population metrics' >>
                              beam.ParDo(
                                  ProduceSupervisionMetrics(),
                                  **self._pipeline_options))

        # Produce the SupervisionRevocationMetrics
        revocation_metrics = (revocations_with_sums
                              | 'Produce supervision revocation metrics' >>
                              beam.ParDo(
                                  ProduceSupervisionMetrics(),
                                  **self._pipeline_options))

        # Produce the SupervisionSuccessMetrics
        success_metrics = (successes_with_sums
                           | 'Produce supervision success metrics' >>
                           beam.ParDo(
                               ProduceSupervisionMetrics(),
                               **self._pipeline_options))

        # Merge the metric groups
        merged_metrics = ((population_metrics, revocation_metrics,
                           success_metrics)
                          | 'Merge population, revocation, and success'
                            ' metrics' >>
                          beam.Flatten())

        # Return SupervisionMetrics objects
        return merged_metrics


@with_input_types(beam.typehints.Tuple[int, Dict[str, Any]],
                  beam.typehints.Optional[Dict[Any,
                                               Tuple[Any, Dict[str, Any]]]],
                  beam.typehints.Optional[Dict[Any,
                                               Tuple[Any, Dict[str, Any]]]]
                  )
@with_output_types(beam.typehints.Tuple[entities.StatePerson,
                                        List[SupervisionTimeBucket]])
class ClassifySupervisionTimeBuckets(beam.DoFn):
    """Classifies time on supervision as years and months with or without
    revocation, and classifies months of projected completion as either
    successful or not."""

    #pylint: disable=arguments-differ
    def process(self, element, ssvr_agent_associations,
                supervision_period_to_agent_associations):
        """Identifies instances of revocation and non-revocation time buckets on
        supervision, and classifies months of projected completion as either
        successful or not.
        """
        _, person_periods = element

        # Get the StateSupervisionSentences as a list
        supervision_sentences = \
            list(person_periods['supervision_sentences'])

        # Get the StateIncarcerationPeriods as a list
        incarceration_periods = \
            list(person_periods['incarceration_periods'])

        # Get the StatePerson
        person = one(person_periods['person'])

        # Find the SupervisionTimeBuckets from the supervision and incarceration
        # periods
        supervision_time_buckets = \
            identifier.find_supervision_time_buckets(
                supervision_sentences,
                incarceration_periods,
                ssvr_agent_associations,
                supervision_period_to_agent_associations)

        if not supervision_time_buckets:
            logging.info("No valid supervision time buckets for person with"
                         "id: %d. Excluding them from the "
                         "calculations.", person.person_id)
        else:
            yield (person, supervision_time_buckets)

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


@with_input_types(beam.typehints.Tuple[entities.StatePerson,
                                       List[SupervisionTimeBucket]])
@with_output_types(beam.typehints.Tuple[str, Any])
class CalculateSupervisionMetricCombinations(beam.DoFn):
    """Calculates supervision metric combinations."""

    def process(self, element, *args, **kwargs):
        """Produces various supervision metric combinations.

        Sends the calculator the StatePerson entity and their corresponding
        SupervisionTimeBuckets for mapping all supervision combinations.

        Args:
            element: Tuple containing a StatePerson and their
                SupervisionTimeBuckets
            **kwargs: This should be a dictionary with values for the
                following keys:
                    - age_bucket
                    - gender
                    - race
                    - ethnicity
        Yields:
            Each supervision metric combination, tagged by metric type.
        """
        person, supervision_time_buckets = element

        # Calculate supervision metric combinations for this person and their
        # supervision time buckets
        metric_combinations = \
            calculator.map_supervision_combinations(person,
                                                    supervision_time_buckets,
                                                    kwargs)

        # Return each of the supervision metric combinations
        for metric_combination in metric_combinations:
            metric_key, value = metric_combination
            metric_type = metric_key.get('metric_type')

            # Converting the metric key to a JSON string so it is hashable
            serializable_dict = json_serializable_metric_key(metric_key)
            json_key = json.dumps(serializable_dict, sort_keys=True)

            if metric_type == MetricType.POPULATION.value:
                yield beam.pvalue.TaggedOutput('populations',
                                               (json_key, value))
            elif metric_type == MetricType.REVOCATION.value:
                yield beam.pvalue.TaggedOutput('revocations',
                                               (json_key, value))
            elif metric_type == MetricType.SUCCESS.value:
                yield beam.pvalue.TaggedOutput('successes',
                                               (json_key, value))

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


@with_input_types(beam.typehints.Tuple[str, int],
                  **{'runner': str,
                     'project': str,
                     'job_name': str,
                     'region': str,
                     'job_timestamp': str}
                  )
@with_output_types(SupervisionMetric)
class ProduceSupervisionMetrics(beam.DoFn):
    """Produces SupervisionPopulationMetrics
    and SupervisionRevocationMetrics ready for persistence."""

    def process(self, element, *args, **kwargs):
        pipeline_options = kwargs

        pipeline_job_id = job_id(pipeline_options)

        (metric_key, value) = element

        if value is None:
            # Due to how the pipeline arrives at this function, this should be
            # impossible.
            raise ValueError("No value associated with this metric key.")

        # Convert JSON string to dictionary
        dict_metric_key = json.loads(metric_key)
        metric_type = dict_metric_key.get('metric_type')

        if metric_type == MetricType.POPULATION.value:
            dict_metric_key['count'] = value

            supervision_metric = \
                SupervisionPopulationMetric.build_from_metric_key_group(
                    dict_metric_key, pipeline_job_id)
        elif metric_type == MetricType.REVOCATION.value:
            dict_metric_key['count'] = value

            supervision_metric = \
                SupervisionRevocationMetric.build_from_metric_key_group(
                    dict_metric_key, pipeline_job_id)
        elif metric_type == MetricType.SUCCESS.value:
            dict_metric_key['successful_completion_count'] = \
                value.get('successful_completion_count')
            dict_metric_key['projected_completion_count'] = \
                value.get('projected_completion_count')

            supervision_metric = \
                SupervisionSuccessMetric.build_from_metric_key_group(
                    dict_metric_key, pipeline_job_id)
        else:
            logging.error("Unexpected metric of type: %s",
                          dict_metric_key.get('metric_type'))
            return

        if supervision_metric:
            yield supervision_metric

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


@with_input_types(SupervisionMetric)
@with_output_types(beam.typehints.Dict[str, Any])
class SupervisionMetricWritableDict(beam.DoFn):
    """Builds a dictionary in the format necessary to write the output to
    BigQuery."""

    def process(self, element, *args, **kwargs):
        """The beam.io.WriteToBigQuery transform requires elements to be in
        dictionary form, where the values are in formats as required by BigQuery
        I/O connector.

        For a list of required formats, see the "Data types" section of:
            https://beam.apache.org/documentation/io/built-in/google-bigquery/

        Args:
            element: A SupervisionMetric

        Yields:
            A dictionary representation of the SupervisionMetric
                in the format Dict[str, Any] so that it can be written to
                BigQuery using beam.io.WriteToBigQuery.
        """
        element_dict = json_serializable_metric_key(element.__dict__)

        if isinstance(element, SupervisionPopulationMetric):
            yield beam.pvalue.TaggedOutput('populations', element_dict)
        elif isinstance(element, SupervisionRevocationMetric):
            yield beam.pvalue.TaggedOutput('revocations', element_dict)
        elif isinstance(element, SupervisionSuccessMetric):
            yield beam.pvalue.TaggedOutput('successes', element_dict)

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


def parse_arguments(argv):
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser()

    # Parse arguments
    parser.add_argument('--input',
                        dest='input',
                        type=str,
                        help='BigQuery dataset to query.',
                        required=True)

    # TODO: Generalize these arguments
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

    parser.add_argument('--include_ethnicity',
                        dest='include_ethnicity',
                        type=bool,
                        help='Include metrics broken down by ethnicity.',
                        default=False)

    parser.add_argument('--methodology',
                        dest='methodology',
                        type=str,
                        choices=['PERSON', 'EVENT', 'BOTH'],
                        help='PERSON, EVENT, or BOTH',
                        required=True)

    parser.add_argument('--output',
                        dest='output',
                        type=str,
                        help='Output dataset to write results to.',
                        required=True)

    return parser.parse_known_args(argv)


def dimensions_and_methodologies(known_args) -> \
        Tuple[Dict[str, bool], List[MetricMethodologyType]]:

    filterable_dimensions_map = {
        'include_age': 'age_bucket',
        'include_ethnicity': 'ethnicity',
        'include_gender': 'gender',
        'include_race': 'race',
    }

    known_args_dict = vars(known_args)

    dimensions: Dict[str, bool] = {}

    for dimension_key in filterable_dimensions_map:
        if not known_args_dict[dimension_key]:
            dimensions[filterable_dimensions_map[dimension_key]] = False
        else:
            dimensions[filterable_dimensions_map[dimension_key]] = True

    methodologies = []

    if known_args.methodology == 'BOTH':
        methodologies.append(MetricMethodologyType.EVENT)
        methodologies.append(MetricMethodologyType.PERSON)
    else:
        methodologies.append(MetricMethodologyType[known_args.methodology])

    return dimensions, methodologies


def run(argv=None):
    """Runs the supervision calculation pipeline."""

    # Workaround to load SQLAlchemy objects at start of pipeline. This is
    # necessary because the BuildRootEntity function tries to access attributes
    # of relationship properties on the SQLAlchemy room_schema_class before they
    # have been loaded. However, if *any* SQLAlchemy objects have been
    # instantiated, then the relationship properties are loaded and their
    # attributes can be successfully accessed.
    _ = schema.StatePerson()

    # Parse command-line arguments
    known_args, pipeline_args = parse_arguments(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Get pipeline job details
    all_pipeline_options = pipeline_options.get_all_options()

    query_dataset = all_pipeline_options['project'] + '.' + known_args.input

    with beam.Pipeline(argv=pipeline_args) as p:
        # Get StatePersons
        persons = (p
                   | 'Load Persons' >>
                   BuildRootEntity(dataset=query_dataset,
                                   data_dict=None,
                                   root_schema_class=schema.StatePerson,
                                   root_entity_class=entities.StatePerson,
                                   unifying_id_field='person_id',
                                   build_related_entities=True))

        # Get StateIncarcerationPeriods
        incarceration_periods = (p
                                 | 'Load IncarcerationPeriods' >>
                                 BuildRootEntity(
                                     dataset=query_dataset,
                                     data_dict=None,
                                     root_schema_class=
                                     schema.StateIncarcerationPeriod,
                                     root_entity_class=
                                     entities.StateIncarcerationPeriod,
                                     unifying_id_field='person_id',
                                     build_related_entities=True))

        # Get StateSupervisionViolationResponses
        supervision_violation_responses = \
            (p
             | 'Load SupervisionViolationResponses' >>
             BuildRootEntity(
                 dataset=query_dataset,
                 data_dict=None,
                 root_schema_class=schema.StateSupervisionViolationResponse,
                 root_entity_class=entities.StateSupervisionViolationResponse,
                 unifying_id_field='person_id',
                 build_related_entities=True
             ))

        # Get StateSupervisionSentences
        supervision_sentences = (p
                                 | 'Load SupervisionSentences' >>
                                 BuildRootEntity(
                                     dataset=query_dataset,
                                     data_dict=None,
                                     root_schema_class=
                                     schema.StateSupervisionSentence,
                                     root_entity_class=
                                     entities.StateSupervisionSentence,
                                     unifying_id_field='person_id',
                                     build_related_entities=True))

        # Bring in the table that associates StateSupervisionViolationResponses
        # to information about StateAgents
        ssvr_to_agent_association_query = \
            f"SELECT * FROM `{query_dataset}.ssvr_to_agent_association`"

        ssvr_to_agent_associations = (
            p
            | "Read SSVR to Agent table from BigQuery" >>
            beam.io.Read(beam.io.BigQuerySource
                         (query=ssvr_to_agent_association_query,
                          use_standard_sql=True)))

        # Convert the association table rows into key-value tuples with the
        # value for the supervision_violation_response_id column as the key
        ssvr_agent_associations_as_kv = (
            ssvr_to_agent_associations |
            'Convert SSVR to Agent table to KV tuples' >>
            beam.ParDo(ConvertDictToKVTuple(),
                       'supervision_violation_response_id')
        )

        supervision_period_to_agent_association_query = \
            f"SELECT * FROM `{query_dataset}." \
            f"supervision_period_to_agent_association`"

        supervision_period_to_agent_associations = (
            p
            | "Read Supervision Period to Agent table from BigQuery" >>
            beam.io.Read(beam.io.BigQuerySource
                         (query=supervision_period_to_agent_association_query,
                          use_standard_sql=True)))

        # Convert the association table rows into key-value tuples with the
        # value for the supervision_period_id column as the key
        supervision_period_to_agent_associations_as_kv = (
            supervision_period_to_agent_associations |
            'Convert Supervision Period to Agent table to KV tuples' >>
            beam.ParDo(ConvertDictToKVTuple(),
                       'supervision_period_id')
        )

        # Group StateIncarcerationPeriods and StateSupervisionViolationResponses
        # by person_id
        incarceration_periods_and_violation_responses = (
            {'incarceration_periods': incarceration_periods,
             'violation_responses': supervision_violation_responses}
            | 'Group StateIncarcerationPeriods to '
              'StateSupervisionViolationResponses' >>
            beam.CoGroupByKey()
        )

        # Set the fully hydrated StateSupervisionViolationResponse entities on
        # the corresponding StateIncarcerationPeriods
        incarceration_periods_with_source_violations = (
            incarceration_periods_and_violation_responses
            | 'Set hydrated StateSupervisionViolationResponses on '
            'the StateIncarcerationPeriods' >>
            beam.ParDo(SetViolationResponseOnIncarcerationPeriod()))

        # Group each StatePerson with their StateIncarcerationPeriods and
        # StateSupervisionSentences
        person_periods_and_sentences = (
            {'person': persons,
             'incarceration_periods':
                 incarceration_periods_with_source_violations,
             'supervision_sentences':
                 supervision_sentences
             }
            | 'Group StatePerson to StateIncarcerationPeriods and'
              ' StateSupervisionPeriods' >>
            beam.CoGroupByKey()
        )

        # Identify SupervisionTimeBuckets from the StatePerson's
        # StateSupervisionSentences and StateIncarcerationPeriods
        person_time_buckets = (
            person_periods_and_sentences
            | beam.ParDo(ClassifySupervisionTimeBuckets(),
                         AsDict(
                             ssvr_agent_associations_as_kv),
                         AsDict(
                             supervision_period_to_agent_associations_as_kv
                         )))

        # Get dimensions to include and methodologies to use
        inclusions, _ = dimensions_and_methodologies(known_args)

        # Get pipeline job details for accessing job_id
        all_pipeline_options = pipeline_options.get_all_options()

        # Add timestamp for local jobs
        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        # Get supervision metrics
        supervision_metrics = (person_time_buckets
                               | 'Get Supervision Metrics' >>
                               GetSupervisionMetrics(
                                   pipeline_options=all_pipeline_options,
                                   inclusions=inclusions))

        # Convert the metrics into a format that's writable to BQ
        writable_metrics = (supervision_metrics
                            | 'Convert to dict to be written to BQ' >>
                            beam.ParDo(
                                SupervisionMetricWritableDict()).with_outputs(
                                    'populations', 'revocations', 'successes'))

        # Write the metrics to the output tables in BigQuery
        populations_table = known_args.output + \
            '.supervision_population_metrics'

        revocations_table = known_args.output + \
            '.supervision_revocation_metrics'

        successes_table = known_args.output + \
            '.supervision_success_metrics'

        _ = (writable_metrics.populations
             | f"Write population metrics to BQ table: {populations_table}" >>
             beam.io.WriteToBigQuery(
                 table=populations_table,
                 create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
             ))

        _ = (writable_metrics.revocations
             | f"Write revocation metrics to BQ table: {revocations_table}" >>
             beam.io.WriteToBigQuery(
                 table=revocations_table,
                 create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
             ))

        _ = (writable_metrics.successes
             | f"Write success metrics to BQ table: {successes_table}" >>
             beam.io.WriteToBigQuery(
                 table=successes_table,
                 create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
             ))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
