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
"""The incarceration calculation pipeline. See recidiviz/tools/run_sandbox_calculation_pipeline.py
for details on how to launch a local run.
"""

from __future__ import absolute_import

import argparse
import logging

from typing import Any, Dict, List, Tuple, Set, Optional, cast
import datetime

from apache_beam.pvalue import AsList

import apache_beam as beam
from apache_beam.options.pipeline_options import SetupOptions, PipelineOptions
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.calculator.dataflow_output_storage_config import (
    DATAFLOW_METRICS_TO_TABLES,
)
from recidiviz.calculator.pipeline.incarceration import identifier, metric_producer
from recidiviz.calculator.pipeline.incarceration.incarceration_event import (
    IncarcerationEvent,
)
from recidiviz.calculator.pipeline.incarceration.metrics import (
    IncarcerationMetric,
    IncarcerationAdmissionMetric,
    IncarcerationReleaseMetric,
    IncarcerationPopulationMetric,
    IncarcerationMetricType,
)
from recidiviz.calculator.pipeline.utils.beam_utils import (
    ConvertDictToKVTuple,
    RecidivizMetricWritableDict,
    ImportTableAsKVTuples,
    ImportTable,
)
from recidiviz.calculator.pipeline.utils.entity_hydration_utils import (
    SetSentencesOnSentenceGroup,
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
from recidiviz.calculator.query.state.views.reference.incarceration_period_judicial_district_association import (
    INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.persons_to_recent_county_of_residence import (
    PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_NAME,
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
def clear_job_id():
    global _job_id
    _job_id = None


@with_input_types(
    beam.typehints.Tuple[entities.StatePerson, List[IdentifierEvent], PersonMetadata]
)
@with_output_types(IncarcerationMetric)
class GetIncarcerationMetrics(beam.PTransform):
    """Transforms a StatePerson and IncarcerationEvents into IncarcerationMetrics."""

    def __init__(
        self,
        pipeline_options: Dict[str, Any],
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

        self._metric_inclusions: Dict[IncarcerationMetricType, bool] = {}

        for metric_option in IncarcerationMetricType:
            if metric_option.value in metric_types or "ALL" in metric_types:
                self._metric_inclusions[metric_option] = True
                logging.info("Producing %s metrics", metric_option.value)
            else:
                self._metric_inclusions[metric_option] = False

    def expand(self, input_or_inputs):
        # Produce IncarcerationMetrics
        incarceration_metrics = (
            input_or_inputs
            | "Produce IncarcerationMetrics"
            >> beam.ParDo(
                ProduceIncarcerationMetrics(),
                self._calculation_end_month,
                self._calculation_month_count,
                self._metric_inclusions,
                self._pipeline_options,
            )
        )

        # Return IncarcerationMetric objects
        return incarceration_metrics


@with_input_types(beam.typehints.Tuple[int, Dict[str, Any]])
@with_output_types(
    beam.typehints.Tuple[int, Tuple[entities.StatePerson, List[IncarcerationEvent]]]
)
class ClassifyIncarcerationEvents(beam.DoFn):
    """Classifies incarceration periods as admission and release events."""

    # pylint: disable=arguments-differ
    def process(self, element):
        """Identifies instances of admission and release from incarceration."""
        _, person_entities = element

        person, kwargs = person_and_kwargs_for_identifier(person_entities)

        # Find the IncarcerationEvents
        incarceration_events = identifier.find_incarceration_events(**kwargs)

        if not incarceration_events:
            logging.info(
                "No valid incarceration events for person with id: %d. Excluding them from the "
                "calculations.",
                person.person_id,
            )
        else:
            yield person.person_id, (person, incarceration_events)

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


@with_input_types(
    beam.typehints.Tuple[entities.StatePerson, List[IdentifierEvent], PersonMetadata],
    beam.typehints.Optional[str],
    beam.typehints.Optional[int],
    beam.typehints.Dict[IncarcerationMetricType, bool],
    beam.typehints.Dict[str, Any],
)
@with_output_types(IncarcerationMetric)
class ProduceIncarcerationMetrics(beam.DoFn):
    """Produces IncarcerationMetrics."""

    # pylint: disable=arguments-differ
    def process(
        self,
        element,
        calculation_end_month,
        calculation_month_count,
        metric_inclusions,
        pipeline_options,
    ):
        """Produces various incarceration metrics.

        Sends the metric producer the StatePerson entity and their corresponding IncarcerationEvents for mapping all
        incarceration metrics.

        Args:
            element: Tuple containing a StatePerson and their IncarcerationEvents
            calculation_end_month: The year and month of the last month for which metrics should be calculated.
            calculation_month_count: The number of months to limit the monthly calculation output to.
            metric_inclusions: A dictionary where the keys are each IncarcerationMetricType, and the values are boolean
                flags for whether or not to include that metric type in the calculations
            pipeline_options: A dictionary storing configuration details for the pipeline.
        Yields:
            Each IncarcerationMetric.
        """
        person, incarceration_events, person_metadata = element

        pipeline_job_id = job_id(pipeline_options)

        # Assert all events are of type IncarcerationEvent
        incarceration_events = cast(List[IncarcerationEvent], incarceration_events)

        # Produce incarceration metrics for this person and events
        metrics = metric_producer.produce_incarceration_metrics(
            person,
            incarceration_events,
            metric_inclusions,
            calculation_end_month,
            calculation_month_count,
            person_metadata,
            pipeline_job_id,
        )

        # Return each of the IncarcerationMetrics
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
        metric_type.value for metric_type in IncarcerationMetricType
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
):
    """Runs the incarceration calculation pipeline."""

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
        persons = p | "Load StatePersons" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StatePerson,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Get StateSentenceGroups
        sentence_groups = p | "Load StateSentenceGroups" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StateSentenceGroup,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Get StateIncarcerationSentences
        incarceration_sentences = (
            p
            | "Load StateIncarcerationSentences"
            >> BuildRootEntity(
                dataset=input_dataset,
                root_entity_class=entities.StateIncarcerationSentence,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=True,
                unifying_id_field_filter_set=person_id_filter_set,
                state_code=state_code,
            )
        )

        # Get StateSupervisionSentences
        supervision_sentences = p | "Load StateSupervisionSentences" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StateSupervisionSentence,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
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

        supervision_sentences_and_statuses = (
            {
                "incarceration_sentences": incarceration_sentences,
                "supervision_sentences": supervision_sentences,
                "sentence_statuses": us_mo_sentence_status_rankings_as_kv,
            }
            | "Group sentences to the sentence statuses for that person"
            >> beam.CoGroupByKey()
        )

        sentences_converted = (
            supervision_sentences_and_statuses
            | "Convert to state-specific sentences"
            >> beam.ParDo(ConvertSentencesToStateSpecificType()).with_outputs(
                "incarceration_sentences", "supervision_sentences"
            )
        )

        sentences_and_sentence_groups = {
            "sentence_groups": sentence_groups,
            "incarceration_sentences": sentences_converted.incarceration_sentences,
            "supervision_sentences": sentences_converted.supervision_sentences,
        } | "Group sentences to sentence groups" >> beam.CoGroupByKey()

        # Set hydrated sentences on the corresponding sentence groups
        sentence_groups_with_hydrated_sentences = (
            sentences_and_sentence_groups
            | "Set hydrated sentences on sentence groups"
            >> beam.ParDo(SetSentencesOnSentenceGroup())
        )

        # Bring in the table that associates people and their county of residence
        person_id_to_county_kv = (
            p
            | "Load person_id_to_county_kv"
            >> ImportTableAsKVTuples(
                dataset_id=reference_dataset,
                table_id=PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_NAME,
                table_key="person_id",
                state_code_filter=state_code,
                person_id_filter_set=person_id_filter_set,
            )
        )

        ip_to_judicial_district_kv = (
            p
            | "Load ip_to_judicial_district_kv"
            >> ImportTableAsKVTuples(
                dataset_id=reference_dataset,
                table_id=INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
                table_key="person_id",
                state_code_filter=state_code,
                person_id_filter_set=person_id_filter_set,
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

        # Group each StatePerson with their related entities
        person_entities = {
            "person": persons,
            "sentence_groups": sentence_groups_with_hydrated_sentences,
            "incarceration_period_judicial_district_association": ip_to_judicial_district_kv,
            "persons_to_recent_county_of_residence": person_id_to_county_kv,
        } | "Group StatePerson to SentenceGroups" >> beam.CoGroupByKey()

        # Identify IncarcerationEvents events from the StatePerson's StateIncarcerationPeriods
        person_incarceration_events = (
            person_entities
            | "Classify Incarceration Events"
            >> beam.ParDo(ClassifyIncarcerationEvents())
        )

        person_metadata = (
            persons
            | "Build the person_metadata dictionary"
            >> beam.ParDo(
                BuildPersonMetadata(), AsList(state_race_ethnicity_population_counts)
            )
        )

        person_incarceration_events_with_metadata = (
            {
                "person_events": person_incarceration_events,
                "person_metadata": person_metadata,
            }
            | "Group IncarcerationEvents with person-level metadata"
            >> beam.CoGroupByKey()
            | "Organize StatePerson, PersonMetadata and IncarcerationEvents for calculations"
            >> beam.ParDo(ExtractPersonEventsMetadata())
        )

        # Get pipeline job details for accessing job_id
        all_pipeline_options = apache_beam_pipeline_options.get_all_options()

        # Add timestamp for local jobs
        job_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S.%f")
        all_pipeline_options["job_timestamp"] = job_timestamp

        # Get the type of metric to calculate
        metric_types_set = set(metric_types)

        # Get IncarcerationMetrics
        incarceration_metrics = (
            person_incarceration_events_with_metadata
            | "Get Incarceration Metrics"
            >> GetIncarcerationMetrics(
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
            incarceration_metrics
            | "Convert to dict to be written to BQ"
            >> beam.ParDo(RecidivizMetricWritableDict()).with_outputs(
                IncarcerationMetricType.INCARCERATION_ADMISSION.value,
                IncarcerationMetricType.INCARCERATION_POPULATION.value,
                IncarcerationMetricType.INCARCERATION_RELEASE.value,
            )
        )

        # Write the metrics to the output tables in BigQuery
        admissions_table_id = DATAFLOW_METRICS_TO_TABLES[IncarcerationAdmissionMetric]
        population_table_id = DATAFLOW_METRICS_TO_TABLES[IncarcerationPopulationMetric]
        releases_table_id = DATAFLOW_METRICS_TO_TABLES[IncarcerationReleaseMetric]

        _ = (
            writable_metrics.INCARCERATION_ADMISSION
            | f"Write admission metrics to BQ table: {admissions_table_id}"
            >> WriteAppendToBigQuery(
                output_table=admissions_table_id,
                output_dataset=output,
            )
        )

        _ = (
            writable_metrics.INCARCERATION_POPULATION
            | f"Write population metrics to BQ table: {population_table_id}"
            >> WriteAppendToBigQuery(
                output_table=population_table_id,
                output_dataset=output,
            )
        )

        _ = (
            writable_metrics.INCARCERATION_RELEASE
            | f"Write release metrics to BQ table: {releases_table_id}"
            >> WriteAppendToBigQuery(
                output_table=releases_table_id,
                output_dataset=output,
            )
        )
