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
from recidiviz.calculator.pipeline.incarceration import identifier, metric_producer
from recidiviz.calculator.pipeline.pipeline_type import PipelineType
from recidiviz.calculator.pipeline.utils.beam_utils import (
    ConvertDictToKVTuple,
    ImportTable,
    ImportTableAsKVTuples,
)
from recidiviz.calculator.pipeline.utils.entity_hydration_utils import (
    ConvertSentencesToStateSpecificType,
    SetSentencesOnSentenceGroup,
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
from recidiviz.calculator.query.state.views.reference.incarceration_period_judicial_district_association import (
    INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.persons_to_recent_county_of_residence import (
    PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_to_agent_association import (
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.us_mo_sentence_statuses import (
    US_MO_SENTENCE_STATUSES_VIEW_NAME,
)
from recidiviz.persistence.entity.state import entities


class IncarcerationPipeline(BasePipeline):
    """Defines the incarceration calculation pipeline."""

    def __init__(self) -> None:
        self.pipeline_config = PipelineConfig(
            pipeline_type=PipelineType.INCARCERATION,
            identifier=identifier.IncarcerationIdentifier(),
            metric_producer=metric_producer.IncarcerationMetricProducer(),
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
        persons = pipeline | "Load StatePersons" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StatePerson,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Get StateSentenceGroups
        sentence_groups = pipeline | "Load StateSentenceGroups" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StateSentenceGroup,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Get StateIncarcerationSentences
        incarceration_sentences = (
            pipeline
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
        supervision_sentences = (
            pipeline
            | "Load StateSupervisionSentences"
            >> BuildRootEntity(
                dataset=input_dataset,
                root_entity_class=entities.StateSupervisionSentence,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=True,
                unifying_id_field_filter_set=person_id_filter_set,
                state_code=state_code,
            )
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
            pipeline
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
            pipeline
            | "Load ip_to_judicial_district_kv"
            >> ImportTableAsKVTuples(
                dataset_id=reference_dataset,
                table_id=INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
                table_key="person_id",
                state_code_filter=state_code,
                person_id_filter_set=person_id_filter_set,
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
            "sentence_groups": sentence_groups_with_hydrated_sentences,
            "violation_responses": violation_responses_with_hydrated_violations,
            "incarceration_period_judicial_district_association": ip_to_judicial_district_kv,
            "supervision_period_to_agent_association": supervision_period_to_agent_associations_as_kv,
            "persons_to_recent_county_of_residence": person_id_to_county_kv,
        } | "Group StatePerson to SentenceGroups" >> beam.CoGroupByKey()

        # Identify IncarcerationEvents events from the StatePerson's StateIncarcerationPeriods
        person_incarceration_events = (
            person_entities
            | "Classify Incarceration Events"
            >> beam.ParDo(ClassifyEvents(), identifier=self.pipeline_config.identifier)
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

        # Add timestamp for local jobs
        job_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S.%f")
        all_pipeline_options["job_timestamp"] = job_timestamp

        # Get the type of metric to calculate
        metric_types_set = set(metric_types)

        # Get IncarcerationMetrics
        incarceration_metrics = (
            person_incarceration_events_with_metadata
            | "Get Incarceration Metrics"
            >> GetMetrics(
                pipeline_options=all_pipeline_options,
                pipeline_config=self.pipeline_config,
                metric_types_to_include=metric_types_set,
                calculation_end_month=calculation_end_month,
                calculation_month_count=calculation_month_count,
            )
        )

        return incarceration_metrics
