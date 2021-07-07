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
import datetime
from typing import Any, Dict, List, Optional, Set

import apache_beam as beam
from apache_beam.pvalue import AsList

from recidiviz.calculator.pipeline.base_pipeline import (
    BasePipeline,
    ClassifyEvents,
    GetMetrics,
    PipelineConfig,
)
from recidiviz.calculator.pipeline.pipeline_type import PipelineType
from recidiviz.calculator.pipeline.supervision import identifier, metric_producer
from recidiviz.calculator.pipeline.utils.beam_utils import (
    ConvertDictToKVTuple,
    ImportTable,
    ImportTableAsKVTuples,
)
from recidiviz.calculator.pipeline.utils.entity_hydration_utils import (
    ConvertSentencesToStateSpecificType,
    SetViolationOnViolationsResponse,
)
from recidiviz.calculator.pipeline.utils.execution_utils import (
    select_all_by_person_query,
)
from recidiviz.calculator.pipeline.utils.extractor_utils import (
    BuildRootEntity,
    ReadFromBigQuery,
)
from recidiviz.calculator.pipeline.utils.person_utils import (
    BuildPersonMetadata,
    ExtractPersonEventsMetadata,
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
from recidiviz.persistence.entity.state import entities


class SupervisionPipeline(BasePipeline):
    """Defines the supervision calculation pipeline."""

    def __init__(self) -> None:
        self.pipeline_config = PipelineConfig(
            pipeline_type=PipelineType.SUPERVISION,
            identifier=identifier.SupervisionIdentifier(),
            metric_producer=metric_producer.SupervisionMetricProducer(),
        )
        self.include_calculation_limit_args = True

    def execute_pipeline(
        self,
        pipeline: beam.Pipeline,
        all_pipeline_options: Dict[str, Any],
        state_code: str,
        input_dataset: str,
        reference_dataset: str,
        static_reference_dataset: str,
        metric_types: List[str],
        person_id_filter_set: Optional[Set[int]],
        calculation_month_count: int = -1,
        calculation_end_month: Optional[str] = None,
    ) -> beam.Pipeline:
        # Get StatePersons
        persons = pipeline | "Load Persons" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StatePerson,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Get StateIncarcerationPeriods
        incarceration_periods = (
            pipeline
            | "Load IncarcerationPeriods"
            >> BuildRootEntity(
                dataset=input_dataset,
                root_entity_class=entities.StateIncarcerationPeriod,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=True,
                unifying_id_field_filter_set=person_id_filter_set,
                state_code=state_code,
            )
        )

        # Get StateSupervisionViolations
        supervision_violations = (
            pipeline
            | "Load SupervisionViolations"
            >> BuildRootEntity(
                dataset=input_dataset,
                root_entity_class=entities.StateSupervisionViolation,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=True,
                unifying_id_field_filter_set=person_id_filter_set,
                state_code=state_code,
            )
        )

        # TODO(#2769): Don't bring this in as a root entity
        # Get StateSupervisionViolationResponses
        supervision_violation_responses = (
            pipeline
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
        supervision_sentences = (
            pipeline
            | "Load SupervisionSentences"
            >> BuildRootEntity(
                dataset=input_dataset,
                root_entity_class=entities.StateSupervisionSentence,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=True,
                unifying_id_field_filter_set=person_id_filter_set,
                state_code=state_code,
            )
        )

        # Get StateIncarcerationSentences
        incarceration_sentences = (
            pipeline
            | "Load IncarcerationSentences"
            >> BuildRootEntity(
                dataset=input_dataset,
                root_entity_class=entities.StateIncarcerationSentence,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=True,
                unifying_id_field_filter_set=person_id_filter_set,
                state_code=state_code,
            )
        )

        # Get StateSupervisionPeriods
        supervision_periods = pipeline | "Load SupervisionPeriods" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StateSupervisionPeriod,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Get StateAssessments
        assessments = pipeline | "Load Assessments" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StateAssessment,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=False,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        supervision_contacts = (
            pipeline
            | "Load StateSupervisionContacts"
            >> BuildRootEntity(
                dataset=input_dataset,
                root_entity_class=entities.StateSupervisionContact,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=False,
                unifying_id_field_filter_set=person_id_filter_set,
                state_code=state_code,
            )
        )

        supervision_period_to_agent_associations_as_kv = (
            pipeline
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
            pipeline
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
            pipeline
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
                pipeline
                | "Read MO sentence status table from BigQuery"
                >> ReadFromBigQuery(query=us_mo_sentence_status_query)
            )
        else:
            us_mo_sentence_statuses = (
                pipeline
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

        # Group each StatePerson with their related entities
        person_entities = {
            "person": persons,
            "assessments": assessments,
            "incarceration_periods": incarceration_periods,
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
            >> beam.ParDo(ClassifyEvents(), identifier=self.pipeline_config.identifier)
        )

        person_metadata = (
            persons
            | "Build the person_metadata dictionary"
            >> beam.ParDo(
                BuildPersonMetadata(),
                state_race_ethnicity_population_counts=AsList(
                    state_race_ethnicity_population_counts
                ),
            )
        )

        person_time_buckets_with_metadata = (
            {"person_events": person_time_buckets, "person_metadata": person_metadata}
            | "Group SupervisionTimeBuckets with person-level metadata"
            >> beam.CoGroupByKey()
            | "Organize StatePerson, PersonMetadata and SupervisionTimeBuckets for calculations"
            >> beam.ParDo(ExtractPersonEventsMetadata())
        )

        # Get the type of metric to calculate
        metric_types_set = set(metric_types)

        # Add timestamp for local jobs
        job_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S.%f")
        all_pipeline_options["job_timestamp"] = job_timestamp

        # Get supervision metrics
        supervision_metrics = (
            person_time_buckets_with_metadata
            | "Get Supervision Metrics"
            >> GetMetrics(
                pipeline_options=all_pipeline_options,
                pipeline_config=self.pipeline_config,
                metric_types_to_include=metric_types_set,
                calculation_end_month=calculation_end_month,
                calculation_month_count=calculation_month_count,
            )
        )

        return supervision_metrics
