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
"""The supervision calculation pipeline. See recidiviz/tools/run_sandbox_calculation_pipeline.py
for details on how to launch a local run.
"""
import argparse
import datetime
import logging
from typing import Dict, Any, List, Tuple, Set, Optional, cast

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    SetupOptions,
    PipelineOptions,
)
from apache_beam.pvalue import AsList
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.calculator.dataflow_output_storage_config import (
    DATAFLOW_METRICS_TO_TABLES,
)
from recidiviz.calculator.pipeline.supervision import identifier, metric_producer
from recidiviz.calculator.pipeline.supervision.metrics import (
    SupervisionMetric,
    SupervisionPopulationMetric,
    SupervisionOutOfStatePopulationMetric,
    SupervisionRevocationMetric,
    SupervisionSuccessMetric,
    SuccessfulSupervisionSentenceDaysServedMetric,
    SupervisionCaseComplianceMetric,
    SupervisionTerminationMetric,
    SupervisionStartMetric,
    SupervisionDowngradeMetric,
)
from recidiviz.calculator.pipeline.supervision.metrics import SupervisionMetricType
from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import (
    SupervisionTimeBucket,
)
from recidiviz.calculator.pipeline.utils.beam_utils import (
    ConvertDictToKVTuple,
    RecidivizMetricWritableDict,
    ImportTableAsKVTuples,
    ImportTable,
)
from recidiviz.calculator.pipeline.utils.entity_hydration_utils import (
    SetViolationResponseOnIncarcerationPeriod,
    SetViolationOnViolationsResponse,
    ConvertSentencesToStateSpecificType,
)
from recidiviz.calculator.pipeline.utils.event_utils import IdentifierEvent
from recidiviz.calculator.pipeline.utils.execution_utils import (
    get_job_id,
    person_and_kwargs_for_identifier,
    select_all_by_person_query,
)
from recidiviz.calculator.pipeline.utils.extractor_utils import (
    BuildRootEntity,
    WriteAppendToBigQuery,
    ReadFromBigQuery,
)
from recidiviz.calculator.pipeline.utils.person_utils import (
    PersonMetadata,
    BuildPersonMetadata,
    ExtractPersonEventsMetadata,
)
from recidiviz.calculator.pipeline.utils.pipeline_args_utils import (
    add_shared_pipeline_arguments,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_judicial_district_association import (
    SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_to_agent_association import (
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.us_mo_sentence_statuses import (
    US_MO_SENTENCE_STATUSES_VIEW_NAME,
)
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
def clear_job_id() -> None:
    global _job_id
    _job_id = None


@with_input_types(
    beam.typehints.Tuple[entities.StatePerson, List[IdentifierEvent], PersonMetadata]
)
@with_output_types(SupervisionMetric)
class GetSupervisionMetrics(beam.PTransform):
    """Transforms a StatePerson and their SupervisionTimeBuckets into SupervisionMetrics."""

    def __init__(
        self,
        pipeline_options: Dict[str, str],
        metric_types: Set[str],
        calculation_month_count: int,
        calculation_end_month: Optional[str] = None,
    ):
        super().__init__()
        self._pipeline_options = pipeline_options
        self._calculation_end_month = calculation_end_month
        self._calculation_month_count = calculation_month_count

        month_count_string = (
            str(calculation_month_count) if calculation_month_count != -1 else "all"
        )
        end_month_string = (
            calculation_end_month if calculation_end_month else "the current month"
        )
        logging.info(
            "Producing metric output for %s month(s) up to %s",
            month_count_string,
            end_month_string,
        )

        self._metric_inclusions: Dict[SupervisionMetricType, bool] = {}

        for metric_option in SupervisionMetricType:
            if metric_option.value in metric_types or "ALL" in metric_types:
                self._metric_inclusions[metric_option] = True
                logging.info("Producing %s metrics", metric_option.value)
            else:
                self._metric_inclusions[metric_option] = False

    def expand(self, input_or_inputs):
        # Produce SupervisionMetrics
        supervision_metrics = (
            input_or_inputs
            | "Produce SupervisionMetrics"
            >> beam.ParDo(
                ProduceSupervisionMetrics(),
                self._calculation_end_month,
                self._calculation_month_count,
                self._metric_inclusions,
                self._pipeline_options,
            )
        )

        # Return SupervisionMetrics objects
        return supervision_metrics


@with_input_types(beam.typehints.Tuple[int, Dict[str, Any]])
@with_output_types(
    beam.typehints.Tuple[int, Tuple[entities.StatePerson, List[SupervisionTimeBucket]]]
)
class ClassifySupervisionTimeBuckets(beam.DoFn):
    """Classifies time on supervision according to multiple types of measurement."""

    # pylint: disable=arguments-differ
    def process(self, element):
        """Identifies various events related to supervision relevant to calculations."""
        _, person_entities = element

        person, kwargs = person_and_kwargs_for_identifier(person_entities)

        # Find the SupervisionTimeBuckets from the supervision and incarceration
        # periods
        supervision_time_buckets = identifier.find_supervision_time_buckets(**kwargs)

        if not supervision_time_buckets:
            logging.info(
                "No valid supervision time buckets for person with id: %d. Excluding them from the "
                "calculations.",
                person.person_id,
            )
        else:
            yield person.person_id, (person, supervision_time_buckets)

    def to_runner_api_parameter(self, _) -> None:
        pass  # Passing unused abstract method.


@with_input_types(
    beam.typehints.Tuple[entities.StatePerson, List[IdentifierEvent], PersonMetadata],
    beam.typehints.Optional[str],
    beam.typehints.Optional[int],
    beam.typehints.Dict[SupervisionMetricType, bool],
    beam.typehints.Dict[str, Any],
)
@with_output_types(SupervisionMetric)
class ProduceSupervisionMetrics(beam.DoFn):
    """Produces supervision metrics."""

    # pylint: disable=arguments-differ
    def process(
        self,
        element,
        calculation_end_month,
        calculation_month_count,
        metric_inclusions,
        pipeline_options,
    ):
        """Produces various SupervisionMetrics.

        Sends the metric_producer the StatePerson entity and their corresponding SupervisionTimeBuckets for mapping all
        supervision metrics.

        Args:
            element: Dictionary containing the person, SupervisionTimeBuckets, and person_metadata
            calculation_end_month: The year and month of the last month for which metrics should be calculated.
            calculation_month_count: The number of months to limit the monthly calculation output to.
            metric_inclusions: A dictionary where the keys are each SupervisionMetricType, and the values are boolean
                values for whether or not to include that metric type in the calculations
            pipeline_options: A dictionary storing configuration details for the pipeline.
        Yields:
            Each supervision metric.
        """
        person, supervision_time_buckets, person_metadata = element

        pipeline_job_id = job_id(pipeline_options)

        # Assert all events are of type SupervisionTimeBucket
        supervision_time_buckets = cast(
            List[SupervisionTimeBucket], supervision_time_buckets
        )

        # Produce supervision metrics for this person and their supervision time buckets
        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            metric_inclusions,
            calculation_end_month,
            calculation_month_count,
            person_metadata,
            pipeline_job_id,
        )

        # Return each of the supervision metrics
        for metric in metrics:
            yield metric

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


def get_arg_parser() -> argparse.ArgumentParser:
    """Returns the parser for the command-line arguments for this pipeline."""
    parser = argparse.ArgumentParser()

    # Parse arguments
    add_shared_pipeline_arguments(parser, include_calculation_limit_args=True)

    metric_type_options: List[str] = [
        metric_type.value for metric_type in SupervisionMetricType
    ]

    metric_type_options.append("ALL")

    parser.add_argument(
        "--metric_types",
        dest="metric_types",
        type=str,
        nargs="+",
        choices=metric_type_options,
        help="A list of the types of metric to calculate.",
        default={"ALL"},
    )

    return parser


def run(
    apache_beam_pipeline_options: PipelineOptions,
    data_input: str,
    reference_view_input: str,
    static_reference_input: str,
    output: str,
    calculation_month_count: int,
    metric_types: List[str],
    state_code: str,
    calculation_end_month: Optional[str],
    person_filter_ids: Optional[List[int]],
) -> None:
    """Runs the supervision calculation pipeline."""

    # Workaround to load SQLAlchemy objects at start of pipeline. This is necessary because the BuildRootEntity
    # function tries to access attributes of relationship properties on the SQLAlchemy room_schema_class before they
    # have been loaded. However, if *any* SQLAlchemy objects have been instantiated, then the relationship properties
    # are loaded and their attributes can be successfully accessed.
    _ = schema.StatePerson()

    apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = True

    # Get pipeline job details
    all_pipeline_options = apache_beam_pipeline_options.get_all_options()
    project_id = all_pipeline_options["project"]

    if project_id is None:
        raise ValueError(f"No project set in pipeline options: {all_pipeline_options}")

    if state_code is None:
        raise ValueError("No state_code set for pipeline")

    input_dataset = project_id + "." + data_input
    reference_dataset = project_id + "." + reference_view_input
    static_reference_dataset = project_id + "." + static_reference_input

    person_id_filter_set = set(person_filter_ids) if person_filter_ids else None

    with beam.Pipeline(options=apache_beam_pipeline_options) as p:
        # Get StatePersons
        persons = p | "Load Persons" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StatePerson,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Get StateIncarcerationPeriods
        incarceration_periods = p | "Load IncarcerationPeriods" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StateIncarcerationPeriod,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Get StateSupervisionViolations
        supervision_violations = p | "Load SupervisionViolations" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StateSupervisionViolation,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # TODO(#2769): Don't bring this in as a root entity
        # Get StateSupervisionViolationResponses
        supervision_violation_responses = (
            p
            | "Load SupervisionViolationResponses"
            >> BuildRootEntity(
                dataset=input_dataset,
                root_entity_class=entities.StateSupervisionViolationResponse,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=True,
                unifying_id_field_filter_set=person_id_filter_set,
                state_code=state_code,
            )
        )

        # Get StateSupervisionSentences
        supervision_sentences = p | "Load SupervisionSentences" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StateSupervisionSentence,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Get StateIncarcerationSentences
        incarceration_sentences = p | "Load IncarcerationSentences" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StateIncarcerationSentence,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Get StateSupervisionPeriods
        supervision_periods = p | "Load SupervisionPeriods" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StateSupervisionPeriod,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Get StateAssessments
        assessments = p | "Load Assessments" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StateAssessment,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=False,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        supervision_contacts = p | "Load StateSupervisionContacts" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StateSupervisionContact,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=False,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        supervision_period_to_agent_associations_as_kv = (
            p
            | "Load supervision_period_to_agent_associations_as_kv"
            >> ImportTableAsKVTuples(
                dataset_id=reference_dataset,
                table_id=SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
                table_key="person_id",
                state_code_filter=state_code,
                person_id_filter_set=person_id_filter_set,
            )
        )

        # Bring in the judicial districts associated with supervision_periods
        sp_to_judicial_district_kv = (
            p
            | "Load sp_to_judicial_district_kv"
            >> ImportTableAsKVTuples(
                dataset_id=reference_dataset,
                table_id=SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
                state_code_filter=state_code,
                person_id_filter_set=person_id_filter_set,
                table_key="person_id",
            )
        )

        state_race_ethnicity_population_counts = (
            p
            | "Load state_race_ethnicity_population_counts"
            >> ImportTable(
                dataset_id=static_reference_dataset,
                table_id="state_race_ethnicity_population_counts",
                state_code_filter=state_code,
                person_id_filter_set=None,
            )
        )

        if state_code == "US_MO":
            # Bring in the reference table that includes sentence status ranking information
            us_mo_sentence_status_query = select_all_by_person_query(
                reference_dataset,
                US_MO_SENTENCE_STATUSES_VIEW_NAME,
                state_code,
                person_id_filter_set,
            )

            us_mo_sentence_statuses = (
                p
                | "Read MO sentence status table from BigQuery"
                >> ReadFromBigQuery(query=us_mo_sentence_status_query)
            )
        else:
            us_mo_sentence_statuses = (
                p
                | f"Generate empty MO statuses list for non-MO state run: {state_code} "
                >> beam.Create([])
            )

        us_mo_sentence_status_rankings_as_kv = (
            us_mo_sentence_statuses
            | "Convert MO sentence status ranking table to KV tuples"
            >> beam.ParDo(ConvertDictToKVTuple(), "person_id")
        )

        sentences_and_statuses = (
            {
                "incarceration_sentences": incarceration_sentences,
                "supervision_sentences": supervision_sentences,
                "sentence_statuses": us_mo_sentence_status_rankings_as_kv,
            }
            | "Group sentences to the sentence statuses for that person"
            >> beam.CoGroupByKey()
        )

        sentences_converted = (
            sentences_and_statuses
            | "Convert to state-specific sentences"
            >> beam.ParDo(ConvertSentencesToStateSpecificType()).with_outputs(
                "incarceration_sentences", "supervision_sentences"
            )
        )

        # Group StateSupervisionViolationResponses and StateSupervisionViolations by person_id
        supervision_violations_and_responses = (
            {
                "violations": supervision_violations,
                "violation_responses": supervision_violation_responses,
            }
            | "Group StateSupervisionViolationResponses to "
            "StateSupervisionViolations" >> beam.CoGroupByKey()
        )

        # Set the fully hydrated StateSupervisionViolation entities on the corresponding
        # StateSupervisionViolationResponses
        violation_responses_with_hydrated_violations = (
            supervision_violations_and_responses
            | "Set hydrated StateSupervisionViolations on "
            "the StateSupervisionViolationResponses"
            >> beam.ParDo(SetViolationOnViolationsResponse())
        )

        # Group StateIncarcerationPeriods and StateSupervisionViolationResponses by person_id
        incarceration_periods_and_violation_responses = (
            {
                "incarceration_periods": incarceration_periods,
                "violation_responses": violation_responses_with_hydrated_violations,
            }
            | "Group StateIncarcerationPeriods to "
            "StateSupervisionViolationResponses" >> beam.CoGroupByKey()
        )

        # Set the fully hydrated StateSupervisionViolationResponse entities on the corresponding
        # StateIncarcerationPeriods
        incarceration_periods_with_source_violations = (
            incarceration_periods_and_violation_responses
            | "Set hydrated StateSupervisionViolationResponses on "
            "the StateIncarcerationPeriods"
            >> beam.ParDo(SetViolationResponseOnIncarcerationPeriod())
        )

        # Group each StatePerson with their related entities
        person_entities = {
            "person": persons,
            "assessments": assessments,
            "incarceration_periods": incarceration_periods_with_source_violations,
            "supervision_periods": supervision_periods,
            "supervision_sentences": sentences_converted.supervision_sentences,
            "incarceration_sentences": sentences_converted.incarceration_sentences,
            "violation_responses": violation_responses_with_hydrated_violations,
            "supervision_contacts": supervision_contacts,
            "supervision_period_judicial_district_association": sp_to_judicial_district_kv,
            "supervision_period_to_agent_association": supervision_period_to_agent_associations_as_kv,
        } | "Group StatePerson to all entities" >> beam.CoGroupByKey()

        # Identify SupervisionTimeBuckets from the StatePerson's StateSupervisionSentences and StateIncarcerationPeriods
        person_time_buckets = (
            person_entities
            | "Get SupervisionTimeBuckets"
            >> beam.ParDo(ClassifySupervisionTimeBuckets())
        )

        person_metadata = (
            persons
            | "Build the person_metadata dictionary"
            >> beam.ParDo(
                BuildPersonMetadata(), AsList(state_race_ethnicity_population_counts)
            )
        )

        person_time_buckets_with_metadata = (
            {"person_events": person_time_buckets, "person_metadata": person_metadata}
            | "Group SupervisionTimeBuckets with person-level metadata"
            >> beam.CoGroupByKey()
            | "Organize StatePerson, PersonMetadata and SupervisionTimeBuckets for calculations"
            >> beam.ParDo(ExtractPersonEventsMetadata())
        )

        # Get pipeline job details for accessing job_id
        all_pipeline_options = apache_beam_pipeline_options.get_all_options()

        # Get the type of metric to calculate
        metric_types_set = set(metric_types)

        # Add timestamp for local jobs
        job_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S.%f")
        all_pipeline_options["job_timestamp"] = job_timestamp

        # Get supervision metrics
        supervision_metrics = (
            person_time_buckets_with_metadata
            | "Get Supervision Metrics"
            >> GetSupervisionMetrics(
                pipeline_options=all_pipeline_options,
                metric_types=metric_types_set,
                calculation_end_month=calculation_end_month,
                calculation_month_count=calculation_month_count,
            )
        )
        if person_id_filter_set:
            logging.warning(
                "Non-empty person filter set - returning before writing metrics."
            )
            return

        # Convert the metrics into a format that's writable to BQ
        writable_metrics = (
            supervision_metrics
            | "Convert to dict to be written to BQ"
            >> beam.ParDo(RecidivizMetricWritableDict()).with_outputs(
                SupervisionMetricType.SUPERVISION_COMPLIANCE.value,
                SupervisionMetricType.SUPERVISION_POPULATION.value,
                SupervisionMetricType.SUPERVISION_REVOCATION.value,
                SupervisionMetricType.SUPERVISION_START.value,
                SupervisionMetricType.SUPERVISION_SUCCESS.value,
                SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED.value,
                SupervisionMetricType.SUPERVISION_TERMINATION.value,
                SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION.value,
                SupervisionMetricType.SUPERVISION_DOWNGRADE.value,
            )
        )

        terminations_table_id = DATAFLOW_METRICS_TO_TABLES[SupervisionTerminationMetric]
        compliance_table_id = DATAFLOW_METRICS_TO_TABLES[
            SupervisionCaseComplianceMetric
        ]
        populations_table_id = DATAFLOW_METRICS_TO_TABLES[SupervisionPopulationMetric]
        revocations_table_id = DATAFLOW_METRICS_TO_TABLES[SupervisionRevocationMetric]
        successes_table_id = DATAFLOW_METRICS_TO_TABLES[SupervisionSuccessMetric]
        successful_sentence_lengths_table_id = DATAFLOW_METRICS_TO_TABLES[
            SuccessfulSupervisionSentenceDaysServedMetric
        ]
        supervision_starts_table_id = DATAFLOW_METRICS_TO_TABLES[SupervisionStartMetric]
        out_of_state_populations_table_id = DATAFLOW_METRICS_TO_TABLES[
            SupervisionOutOfStatePopulationMetric
        ]
        supervision_downgrade_table_id = DATAFLOW_METRICS_TO_TABLES[
            SupervisionDowngradeMetric
        ]

        _ = (
            writable_metrics.SUPERVISION_POPULATION
            | f"Write population metrics to BQ table: {populations_table_id}"
            >> WriteAppendToBigQuery(
                output_table=populations_table_id,
                output_dataset=output,
            )
        )

        _ = writable_metrics.SUPERVISION_OUT_OF_STATE_POPULATION | f"Write out of state population metrics to BQ table: {out_of_state_populations_table_id}" >> WriteAppendToBigQuery(
            output_table=out_of_state_populations_table_id,
            output_dataset=output,
        )

        _ = (
            writable_metrics.SUPERVISION_REVOCATION
            | f"Write revocation metrics to BQ table: {revocations_table_id}"
            >> WriteAppendToBigQuery(
                output_table=revocations_table_id,
                output_dataset=output,
            )
        )

        _ = (
            writable_metrics.SUPERVISION_SUCCESS
            | f"Write success metrics to BQ table: {successes_table_id}"
            >> WriteAppendToBigQuery(
                output_table=successes_table_id,
                output_dataset=output,
            )
        )

        _ = (
            writable_metrics.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED
            | f"Write supervision successful sentence length metrics to BQ"
            f" table: {successful_sentence_lengths_table_id}"
            >> WriteAppendToBigQuery(
                output_table=successful_sentence_lengths_table_id,
                output_dataset=output,
            )
        )

        _ = (
            writable_metrics.SUPERVISION_TERMINATION
            | f"Write termination metrics to BQ table: {terminations_table_id}"
            >> WriteAppendToBigQuery(
                output_table=terminations_table_id,
                output_dataset=output,
            )
        )

        _ = (
            writable_metrics.SUPERVISION_COMPLIANCE
            | f"Write compliance metrics to BQ table: {compliance_table_id}"
            >> WriteAppendToBigQuery(
                output_table=compliance_table_id,
                output_dataset=output,
            )
        )

        _ = (
            writable_metrics.SUPERVISION_START
            | f"Write start metrics to BQ table: {supervision_starts_table_id}"
            >> WriteAppendToBigQuery(
                output_table=supervision_starts_table_id,
                output_dataset=output,
            )
        )

        _ = (
            writable_metrics.SUPERVISION_DOWNGRADE
            | f"Write downgrade metrics to BQ table: {supervision_downgrade_table_id}"
            >> WriteAppendToBigQuery(
                output_table=supervision_downgrade_table_id,
                output_dataset=output,
            )
        )
