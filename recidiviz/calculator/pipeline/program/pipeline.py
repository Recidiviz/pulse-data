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
"""Runs the program calculation pipeline.

usage: pipeline.py --input=INPUT_LOCATION --output=OUTPUT_LOCATION
                    --project=PROJECT --reference_input=REFERENCE_LOCATION
                    --methodology=METHODOLOGY
                    [--include_age] [--include_gender]
                    [--include_race] [--include_ethnicity]

Example output to GCP storage bucket:
python -m recidiviz.calculator.program.pipeline
        --project=recidiviz-project-name
        --input=recidiviz-project-name.dataset
        --reference_input=recidiviz-project-name.ref_dataset
        --output=gs://recidiviz-bucket/output_location
            --methodology=BOTH

Example output to local file:
python -m recidiviz.calculator.program.pipeline
        --project=recidiviz-project-name
        --input=recidiviz-project-name.dataset
        --reference_input=recidiviz-project-name.ref_dataset
        --output=output_file --methodology=PERSON

Example output including race and gender dimensions:
python -m recidiviz.calculator.program.pipeline
        --project=recidiviz-project-name
        --input=recidiviz-project-name.dataset
        --reference_input=recidiviz-project-name.ref_dataset
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

from recidiviz.calculator.pipeline.program import identifier, calculator
from recidiviz.calculator.pipeline.program.metrics import ProgramMetric, \
    ProgramReferralMetric
from recidiviz.calculator.pipeline.program.metrics import \
    ProgramMetricType as MetricType
from recidiviz.calculator.pipeline.program.program_event import ProgramEvent
from recidiviz.calculator.pipeline.utils.beam_utils import SumFn, \
    ConvertDictToKVTuple
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


@with_input_types(beam.typehints.Tuple[entities.StatePerson,
                                       List[ProgramEvent]])
@with_output_types(ProgramMetric)
class GetProgramMetrics(beam.PTransform):
    """Transforms a StatePerson and their ProgramEvents into ProgramMetrics."""

    def __init__(self, pipeline_options: Dict[str, str],
                 inclusions: Dict[str, bool]):
        super(GetProgramMetrics, self).__init__()
        self._pipeline_options = pipeline_options
        self.inclusions = inclusions

    def expand(self, input_or_inputs):
        # Calculate program metric combinations from a StatePerson and their
        # ProgramEvents
        program_metric_combinations = (
            input_or_inputs
            | 'Map to metric combinations' >>
            beam.ParDo(CalculateProgramMetricCombinations(),
                       **self.inclusions).with_outputs('referrals'))

        referrals_with_sums = (program_metric_combinations.referrals
                               | 'Calculate program referral values' >>
                               beam.CombinePerKey(SumFn()))

        referral_metrics = (referrals_with_sums
                            | 'Produce program referral metrics' >>
                            beam.ParDo(
                                ProduceProgramMetrics(),
                                **self._pipeline_options))

        return referral_metrics


@with_input_types(beam.typehints.Tuple[int, Dict[str, Any]],
                  beam.typehints.Optional[Dict[Any,
                                               Tuple[Any, Dict[str, Any]]]]
                  )
@with_output_types(beam.typehints.Tuple[entities.StatePerson,
                                        List[ProgramEvent]])
class ClassifyProgramAssignments(beam.DoFn):
    """Classifies program assignments as program events, such as referrals
    to a program."""

    # pylint: disable=arguments-differ
    def process(self, element, supervision_period_to_agent_associations):
        """Identifies instances of referrals to a program."""
        _, person_entities = element

        # Get the StateProgramAssignments as a list
        program_assignments = \
            list(person_entities['program_assignments'])

        # Get the StateAssessments as a list
        assessments = list(person_entities['assessments'])

        # Get the StateSupervisionPeriods as a list
        supervision_periods = list(person_entities['supervision_periods'])

        # Get the StatePerson
        person = one(person_entities['person'])

        # Find the ProgramEvents from the StateProgramAssignments
        program_events = \
            identifier.find_program_events(
                program_assignments,
                assessments,
                supervision_periods,
                supervision_period_to_agent_associations)

        if not program_events:
            logging.info(
                "No valid program events for person with"
                "id: %d. Excluding them from the "
                "calculations.", person.person_id)
        else:
            yield (person, program_events)

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


@with_input_types(beam.typehints.Tuple[entities.StatePerson,
                                       List[ProgramEvent]])
@with_output_types(beam.typehints.Tuple[str, Any])
class CalculateProgramMetricCombinations(beam.DoFn):
    """Calculates program metric combinations."""

    def process(self, element, *args, **kwargs):
        """Produces various program metric combinations.

        Sends the calculator the StatePerson entity and their corresponding
        ProgramEvents for mapping all program combinations.

        Args:
            element: Tuple containing a StatePerson and their
                ProgramEvents
            **kwargs: This should be a dictionary with values for the
                following keys:
                    - age_bucket
                    - gender
                    - race
                    - ethnicity
        Yields:
            Each program metric combination, tagged by metric type.
        """
        person, program_events = element

        # Calculate program metric combinations for this person and their
        # program events
        metric_combinations = \
            calculator.map_program_combinations(person,
                                                program_events,
                                                kwargs)

        # Return each of the program metric combinations
        for metric_combination in metric_combinations:
            metric_key, value = metric_combination
            metric_type = metric_key.get('metric_type')

            # Converting the metric key to a JSON string so it is hashable
            serializable_dict = json_serializable_metric_key(metric_key)
            json_key = json.dumps(serializable_dict, sort_keys=True)

            if metric_type == MetricType.REFERRAL.value:
                yield beam.pvalue.TaggedOutput('referrals',
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
@with_output_types(ProgramMetric)
class ProduceProgramMetrics(beam.DoFn):
    """Produces ProgramMetrics ready for persistence."""

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

        if metric_type == MetricType.REFERRAL.value:
            dict_metric_key['count'] = value

            program_metric = \
                ProgramReferralMetric.build_from_metric_key_group(
                    dict_metric_key, pipeline_job_id)
        else:
            logging.error("Unexpected metric of type: %s",
                          dict_metric_key.get('metric_type'))
            return

        if program_metric:
            yield program_metric

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


@with_input_types(ProgramMetric)
@with_output_types(beam.typehints.Dict[str, Any])
class ProgramMetricWritableDict(beam.DoFn):
    """Builds a dictionary in the format necessary to write the output to
    BigQuery."""

    def process(self, element, *args, **kwargs):
        """The beam.io.WriteToBigQuery transform requires elements to be in
        dictionary form, where the values are in formats as required by BigQuery
        I/O connector.

        For a list of required formats, see the "Data types" section of:
            https://beam.apache.org/documentation/io/built-in/google-bigquery/

        Args:
            element: A ProgramMetric

        Yields:
            A dictionary representation of the ProgramMetric
                in the format Dict[str, Any] so that it can be written to
                BigQuery using beam.io.WriteToBigQuery.
        """
        element_dict = json_serializable_metric_key(element.__dict__)

        if isinstance(element, ProgramReferralMetric):
            yield beam.pvalue.TaggedOutput('referrals', element_dict)

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

    parser.add_argument('--reference_input',
                        dest='reference_input',
                        type=str,
                        help='BigQuery reference dataset to query.',
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
    """Runs the program calculation pipeline."""

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

    input_dataset = all_pipeline_options['project'] + '.' + known_args.input
    reference_dataset = all_pipeline_options['project'] + '.' + \
        known_args.reference_input

    with beam.Pipeline(argv=pipeline_args) as p:
        # Get StatePersons
        persons = (p
                   | 'Load Persons' >>
                   BuildRootEntity(dataset=input_dataset,
                                   data_dict=None,
                                   root_schema_class=schema.StatePerson,
                                   root_entity_class=entities.StatePerson,
                                   unifying_id_field='person_id',
                                   build_related_entities=True))

        # Get StateProgramAssignments
        program_assignments = (p
                               | 'Load Program Assignments' >>
                               BuildRootEntity(dataset=input_dataset,
                                               data_dict=None,
                                               root_schema_class=
                                               schema.StateProgramAssignment,
                                               root_entity_class=entities.
                                               StateProgramAssignment,
                                               unifying_id_field='person_id',
                                               build_related_entities=True))

        # Get StateAssessments
        assessments = (p
                       | 'Load Assessments' >>
                       BuildRootEntity(dataset=input_dataset,
                                       data_dict=None,
                                       root_schema_class=
                                       schema.StateAssessment,
                                       root_entity_class=entities.
                                       StateAssessment,
                                       unifying_id_field='person_id',
                                       build_related_entities=False))

        # Get StateSupervisionPeriods
        supervision_periods = (p
                               | 'Load SupervisionPeriods' >>
                               BuildRootEntity(
                                   dataset=input_dataset,
                                   data_dict=None,
                                   root_schema_class=
                                   schema.StateSupervisionPeriod,
                                   root_entity_class=
                                   entities.StateSupervisionPeriod,
                                   unifying_id_field='person_id',
                                   build_related_entities=False))

        supervision_period_to_agent_association_query = \
            f"SELECT * FROM `{reference_dataset}." \
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

        # Group each StatePerson with their other entities
        persons_entities = (
            {'person': persons,
             'program_assignments': program_assignments,
             'assessments': assessments,
             'supervision_periods': supervision_periods
             }
            | 'Group StatePerson to StateProgramAssignments and' >>
            beam.CoGroupByKey()
        )

        # Identify ProgramEvents from the StatePerson's
        # StateProgramAssignments
        person_program_events = (
            persons_entities
            | beam.ParDo(ClassifyProgramAssignments(),
                         AsDict(
                             supervision_period_to_agent_associations_as_kv
                         ))
        )

        # Get dimensions to include and methodologies to use
        inclusions, _ = dimensions_and_methodologies(known_args)

        # Get pipeline job details for accessing job_id
        all_pipeline_options = pipeline_options.get_all_options()

        # Add timestamp for local jobs
        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        # Get program metrics
        program_metrics = (person_program_events
                           | 'Get Program Metrics' >>
                           GetProgramMetrics(
                               pipeline_options=all_pipeline_options,
                               inclusions=inclusions))

        # Convert the metrics into a format that's writable to BQ
        writable_metrics = (program_metrics
                            | 'Convert to dict to be written to BQ' >>
                            beam.ParDo(
                                ProgramMetricWritableDict()).with_outputs(
                                    'referrals'))

        # Write the metrics to the output tables in BigQuery
        referrals_table = known_args.output + \
            '.program_referral_metrics'

        _ = (writable_metrics.referrals
             | f"Write referral metrics to BQ table: {referrals_table}" >>
             beam.io.WriteToBigQuery(
                 table=referrals_table,
                 create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
             ))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
