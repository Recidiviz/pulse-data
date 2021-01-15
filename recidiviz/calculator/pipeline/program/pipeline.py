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
"""Runs the incarceration calculation pipeline. See recidiviz/tools/run_calculation_pipelines.py for details on how to
run.
"""
import argparse
import datetime
import logging
from typing import Dict, Any, List, Tuple, Set, Optional, cast

import apache_beam as beam
from apache_beam.options.pipeline_options import SetupOptions, PipelineOptions
from apache_beam.pvalue import AsList
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.calculator.dataflow_output_storage_config import DATAFLOW_METRICS_TO_TABLES
from recidiviz.calculator.pipeline.program import identifier, calculator
from recidiviz.calculator.pipeline.program.metrics import ProgramMetric, \
    ProgramReferralMetric, ProgramParticipationMetric
from recidiviz.calculator.pipeline.program.metrics import ProgramMetricType
from recidiviz.calculator.pipeline.program.program_event import ProgramEvent
from recidiviz.calculator.pipeline.utils.beam_utils import RecidivizMetricWritableDict, ImportTableAsKVTuples, \
    ImportTable
from recidiviz.calculator.pipeline.utils.event_utils import IdentifierEvent
from recidiviz.calculator.pipeline.utils.execution_utils import get_job_id, person_and_kwargs_for_identifier
from recidiviz.calculator.pipeline.utils.extractor_utils import BuildRootEntity, WriteAppendToBigQuery
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata, BuildPersonMetadata, \
    ExtractPersonEventsMetadata
from recidiviz.calculator.pipeline.utils.pipeline_args_utils import add_shared_pipeline_arguments
from recidiviz.calculator.query.state.views.reference.supervision_period_to_agent_association import \
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME
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


@with_input_types(beam.typehints.Tuple[entities.StatePerson, List[IdentifierEvent], PersonMetadata])
@with_output_types(ProgramMetric)
class GetProgramMetrics(beam.PTransform):
    """Transforms a StatePerson and their ProgramEvents into ProgramMetrics."""

    def __init__(self, pipeline_options: Dict[str, str],
                 metric_types: Set[str],
                 calculation_month_count: int,
                 calculation_end_month: Optional[str] = None):
        super().__init__()
        self._pipeline_options = pipeline_options
        self._calculation_end_month = calculation_end_month
        self._calculation_month_count = calculation_month_count

        month_count_string = str(calculation_month_count) if calculation_month_count != -1 else 'all'
        end_month_string = calculation_end_month if calculation_end_month else 'the current month'
        logging.info("Producing metric output for %s month(s) up to %s", month_count_string, end_month_string)

        self._metric_inclusions: Dict[ProgramMetricType, bool] = {}

        for metric_option in ProgramMetricType:
            if metric_option.value in metric_types or 'ALL' in metric_types:
                self._metric_inclusions[metric_option] = True
                logging.info("Producing %s metrics", metric_option.value)
            else:
                self._metric_inclusions[metric_option] = False

    def expand(self, input_or_inputs):
        # Calculate program metric combinations from a StatePerson and their ProgramEvents
        program_metric_combinations = (input_or_inputs | 'Map to metric combinations' >>
                                       beam.ParDo(
                                           CalculateProgramMetricCombinations(),
                                           self._calculation_end_month,
                                           self._calculation_month_count,
                                           self._metric_inclusions))

        # Produce ProgramMetrics
        program_metrics = (program_metric_combinations |
                           'Produce ProgramMetrics' >>
                           beam.ParDo(ProduceProgramMetrics(), **self._pipeline_options))

        return program_metrics


@with_input_types(beam.typehints.Tuple[int, Dict[str, Any]])
@with_output_types(beam.typehints.Tuple[int, Tuple[entities.StatePerson, List[ProgramEvent]]])
class ClassifyProgramAssignments(beam.DoFn):
    """Classifies program assignments as program events, such as referrals to a program."""

    # pylint: disable=arguments-differ
    def process(self, element):
        """Identifies instances of referrals to a program."""
        _, person_entities = element

        person, kwargs = person_and_kwargs_for_identifier(person_entities)

        # Find the ProgramEvents from the StateProgramAssignments
        program_events = identifier.find_program_events(**kwargs)

        if not program_events:
            logging.info(
                "No valid program events for person with id: %d. Excluding them from the "
                "calculations.", person.person_id)
        else:
            yield person.person_id, (person, program_events)

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


@with_input_types(beam.typehints.Tuple[entities.StatePerson, List[IdentifierEvent], PersonMetadata],
                  beam.typehints.Optional[str],
                  beam.typehints.Optional[int],
                  beam.typehints.Dict[ProgramMetricType, bool])
@with_output_types(beam.typehints.Tuple[Dict[str, Any], Any])
class CalculateProgramMetricCombinations(beam.DoFn):
    """Calculates program metric combinations."""

    #pylint: disable=arguments-differ
    def process(self, element, calculation_end_month, calculation_month_count, metric_inclusions):
        """Produces various program metric combinations.

        Sends the calculator the StatePerson entity and their corresponding ProgramEvents for mapping all program
        combinations.

        Args:
            element: Tuple containing a StatePerson and their ProgramEvents
            calculation_end_month: The year and month of the last month for which metrics should be calculated.
            calculation_month_count: The number of months to limit the monthly calculation output to.
            metric_inclusions: A dictionary where the keys are each ProgramMetricType, and the values are boolean
                flags for whether or not to include that metric type in the calculations
        Yields:
            Each program metric combination.
        """
        person, program_events, person_metadata = element

        # Assert all events are of type IncarcerationEvent
        program_events = cast(List[ProgramEvent], program_events)

        # Calculate program metric combinations for this person and their program events
        metric_combinations = \
            calculator.map_program_combinations(person=person,
                                                program_events=program_events,
                                                metric_inclusions=metric_inclusions,
                                                calculation_end_month=calculation_end_month,
                                                calculation_month_count=calculation_month_count,
                                                person_metadata=person_metadata)

        # Return each of the program metric combinations
        for metric_combination in metric_combinations:
            yield metric_combination

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


@with_input_types(beam.typehints.Tuple[Dict[str, Any], Any])
@with_output_types(ProgramMetric)
class ProduceProgramMetrics(beam.DoFn):
    """Produces ProgramMetrics."""

    def process(self, element, *args, **kwargs):
        """Converts a program metric key into a ProgramMetric.

        The pipeline options are sent in as the **kwargs so that the job_id(pipeline_options) function can be called to
        retrieve the job_id.

        Args:
            element: A tuple containing the dictionary for the program metric, and the value of that metric.
            **kwargs: This should be a dictionary with values for the following keys:
                    - runner: Either 'DirectRunner' or 'DataflowRunner'
                    - project: GCP project ID
                    - job_name: Name of the pipeline job
                    - region: Region where the pipeline job is running
                    - job_timestamp: Timestamp for the current job, to be used if the job is running locally.

        Yields:
            The ProgramMetric.
        """
        pipeline_options = kwargs

        pipeline_job_id = job_id(pipeline_options)

        (dict_metric_key, _) = element

        if not dict_metric_key:
            # Due to how the pipeline arrives at this function, this should be impossible.
            raise ValueError("Empty dict_metric_key.")

        metric_type = dict_metric_key.pop('metric_type')

        if metric_type == ProgramMetricType.PROGRAM_REFERRAL:
            program_metric = ProgramReferralMetric.build_from_metric_key_group(dict_metric_key, pipeline_job_id)
        elif metric_type == ProgramMetricType.PROGRAM_PARTICIPATION:
            program_metric = ProgramParticipationMetric.build_from_metric_key_group(dict_metric_key, pipeline_job_id)
        else:
            logging.error("Unexpected metric of type: %s", metric_type)
            return

        if program_metric:
            yield program_metric

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


def get_arg_parser() -> argparse.ArgumentParser:
    """Returns the parser for the command-line arguments for this pipeline."""
    parser = argparse.ArgumentParser()

    # Parse arguments
    add_shared_pipeline_arguments(parser, include_calculation_limit_args=True)

    metric_type_options: List[str] = [
        metric_type.value for metric_type in ProgramMetricType
    ]

    metric_type_options.append('ALL')

    parser.add_argument('--metric_types',
                        dest='metric_types',
                        type=str,
                        nargs='+',
                        choices=metric_type_options,
                        help='A list of the types of metric to calculate.',
                        default={'ALL'})

    return parser


def run(apache_beam_pipeline_options: PipelineOptions,
        data_input: str,
        reference_view_input: str,
        static_reference_input: str,
        output: str,
        calculation_month_count: int,
        metric_types: List[str],
        state_code: str,
        calculation_end_month: Optional[str],
        person_filter_ids: Optional[List[int]]):
    """Runs the program calculation pipeline."""

    # Workaround to load SQLAlchemy objects at start of pipeline. This is necessary because the BuildRootEntity
    # function tries to access attributes of relationship properties on the SQLAlchemy room_schema_class before they
    # have been loaded. However, if *any* SQLAlchemy objects have been instantiated, then the relationship properties
    # are loaded and their attributes can be successfully accessed.
    _ = schema.StatePerson()

    apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = True

    # Get pipeline job details
    all_pipeline_options = apache_beam_pipeline_options.get_all_options()
    project_id = all_pipeline_options['project']

    if project_id is None:
        raise ValueError(f'No project set in pipeline options: {all_pipeline_options}')

    if state_code is None:
        raise ValueError('No state_code set for pipeline')

    input_dataset = project_id + '.' + data_input
    reference_dataset = project_id + '.' + reference_view_input
    static_reference_dataset = project_id + '.' + static_reference_input

    person_id_filter_set = set(person_filter_ids) if person_filter_ids else None

    with beam.Pipeline(options=apache_beam_pipeline_options) as p:
        # Get StatePersons
        persons = (p | 'Load Persons' >>
                   BuildRootEntity(dataset=input_dataset, root_entity_class=entities.StatePerson,
                                   unifying_id_field=entities.StatePerson.get_class_id_name(),
                                   build_related_entities=True,
                                   unifying_id_field_filter_set=person_id_filter_set,
                                   state_code=state_code))

        # Get StateProgramAssignments
        program_assignments = (p | 'Load Program Assignments' >>
                               BuildRootEntity(dataset=input_dataset,
                                               root_entity_class=entities.
                                               StateProgramAssignment,
                                               unifying_id_field=entities.StatePerson.get_class_id_name(),
                                               build_related_entities=True,
                                               unifying_id_field_filter_set=person_id_filter_set,
                                               state_code=state_code))

        # Get StateAssessments
        assessments = (p | 'Load Assessments' >>
                       BuildRootEntity(dataset=input_dataset, root_entity_class=entities.StateAssessment,
                                       unifying_id_field=entities.StatePerson.get_class_id_name(),
                                       build_related_entities=False,
                                       unifying_id_field_filter_set=person_id_filter_set,
                                       state_code=state_code))

        # Get StateSupervisionPeriods
        supervision_periods = (p | 'Load SupervisionPeriods' >>
                               BuildRootEntity(dataset=input_dataset, root_entity_class=entities.StateSupervisionPeriod,
                                               unifying_id_field=entities.StatePerson.get_class_id_name(),
                                               build_related_entities=False,
                                               unifying_id_field_filter_set=person_id_filter_set,
                                               state_code=state_code))

        supervision_period_to_agent_associations_as_kv = (
                p | 'Load supervision_period_to_agent_associations_as_kv' >>
                ImportTableAsKVTuples(
                    dataset_id=reference_dataset,
                    table_id=SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
                    table_key='person_id',
                    state_code_filter=state_code,
                    person_id_filter_set=person_id_filter_set
                ))

        state_race_ethnicity_population_counts = (
                p | 'Load state_race_ethnicity_population_counts' >>
                ImportTable(
                    dataset_id=static_reference_dataset,
                    table_id='state_race_ethnicity_population_counts',
                    state_code_filter=state_code,
                    person_id_filter_set=None
                ))

        # Group each StatePerson with their other entities
        persons_entities = (
            {'person': persons,
             'program_assignments': program_assignments,
             'assessments': assessments,
             'supervision_periods': supervision_periods,
             'supervision_period_to_agent_association': supervision_period_to_agent_associations_as_kv,
             }
            | 'Group StatePerson to StateProgramAssignments and' >> beam.CoGroupByKey()
        )

        # Identify ProgramEvents from the StatePerson's StateProgramAssignments
        person_program_events = (
            persons_entities
            | beam.ParDo(ClassifyProgramAssignments())
        )

        person_metadata = (persons
                           | "Build the person_metadata dictionary" >>
                           beam.ParDo(BuildPersonMetadata(),
                                      AsList(state_race_ethnicity_population_counts)))

        person_program_events_with_metadata = (
            {
                'person_events': person_program_events,
                'person_metadata': person_metadata
            }
            | 'Group ProgramEvents with person-level metadata' >> beam.CoGroupByKey()
            | 'Organize StatePerson, PersonMetadata and ProgramEvents for calculations' >>
            beam.ParDo(ExtractPersonEventsMetadata())
        )

        # Get pipeline job details for accessing job_id
        all_pipeline_options = apache_beam_pipeline_options.get_all_options()

        # Add timestamp for local jobs
        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        # Get the type of metric to calculate
        metric_types_set = set(metric_types)

        # Get program metrics
        program_metrics = (person_program_events_with_metadata | 'Get Program Metrics' >>
                           GetProgramMetrics(
                               pipeline_options=all_pipeline_options,
                               metric_types=metric_types_set,
                               calculation_end_month=calculation_end_month,
                               calculation_month_count=calculation_month_count))

        if person_id_filter_set:
            logging.warning("Non-empty person filter set - returning before writing metrics.")
            return

        # Convert the metrics into a format that's writable to BQ
        writable_metrics = (program_metrics
                            | 'Convert to dict to be written to BQ' >>
                            beam.ParDo(RecidivizMetricWritableDict()).with_outputs(
                                ProgramMetricType.PROGRAM_PARTICIPATION.value,
                                ProgramMetricType.PROGRAM_REFERRAL.value
                            ))

        # Write the metrics to the output tables in BigQuery
        referrals_table_id = DATAFLOW_METRICS_TO_TABLES[ProgramReferralMetric]
        participation_table_id = DATAFLOW_METRICS_TO_TABLES[ProgramParticipationMetric]

        _ = (writable_metrics.PROGRAM_REFERRAL | f"Write referral metrics to BQ table: {referrals_table_id}" >>
             WriteAppendToBigQuery(
                 output_table=referrals_table_id,
                 output_dataset=output,
             ))

        _ = (writable_metrics.PROGRAM_PARTICIPATION
             | f"Write participation metrics to BQ table: {participation_table_id}" >>
             WriteAppendToBigQuery(
                 output_table=participation_table_id,
                 output_dataset=output,
             ))
