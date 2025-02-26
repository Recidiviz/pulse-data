# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""The comprehensive normalization calculation pipeline. See
recidiviz/tools/run_sandbox_calculation_pipeline.py for details on how to launch a
local run.
"""
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.calculator.query.state.views.reference.state_charge_offense_description_to_labels import (
    STATE_CHARGE_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.state_person_to_state_staff import (
    STATE_PERSON_TO_STATE_STAFF_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.us_mo_sentence_statuses import (
    US_MO_SENTENCE_STATUSES_VIEW_NAME,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state import entities
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.normalization.comprehensive.entity_normalizer import (
    ComprehensiveEntityNormalizer,
)
from recidiviz.pipelines.normalization.pipeline_parameters import (
    NormalizationPipelineParameters,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.assessment_normalization_manager import (
    AssessmentNormalizationManager,
    StateSpecificAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    IncarcerationPeriodNormalizationManager,
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.program_assignment_normalization_manager import (
    ProgramAssignmentNormalizationManager,
    StateSpecificProgramAssignmentNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.sentence_normalization_manager import (
    SentenceNormalizationManager,
    StateSpecificSentenceNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_contact_normalization_manager import (
    SupervisionContactNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
    SupervisionPeriodNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
    ViolationResponseNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalized_entities import (
    NormalizedStateEntity,
)
from recidiviz.pipelines.normalization.utils.normalized_entities_utils import (
    AdditionalAttributesMap,
    normalized_entity_class_with_base_class_name,
)
from recidiviz.pipelines.normalization.utils.normalized_entity_conversion_utils import (
    bq_schema_for_normalized_state_entity,
    convert_entities_to_normalized_dicts,
)
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import (
    WriteToBigQuery,
    json_serializable_dict,
)
from recidiviz.pipelines.utils.beam_utils.extractor_utils import ExtractDataForPipeline
from recidiviz.pipelines.utils.execution_utils import TableRow, kwargs_for_entity_lists
from recidiviz.pipelines.utils.state_utils.state_calculation_config_manager import (
    get_required_state_specific_delegates,
)
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)


class ComprehensiveNormalizationPipeline(BasePipeline[NormalizationPipelineParameters]):
    """Defines the entity normalization pipeline that normalizes all entities with
    configured normalization processes."""

    @classmethod
    def parameters_type(cls) -> Type[NormalizationPipelineParameters]:
        return NormalizationPipelineParameters

    @classmethod
    def pipeline_name(cls) -> str:
        return "COMPREHENSIVE_NORMALIZATION"

    @classmethod
    def required_entities(
        cls,
    ) -> List[Union[Type[Entity], Type[NormalizedStateEntity]]]:
        # Note: This is a list of all of the entities that are required to
        # perform entity normalization on all entities with normalization
        # processes. This is *not* the list of entities that are normalized by
        # this pipeline. See the normalized_entity_classes attribute of each of
        # the EntityNormalizationManagers in the
        # required_entity_normalization_managers below to see all entities
        # normalized by this pipeline.
        return [
            entities.StateSupervisionSentence,
            entities.StateIncarcerationSentence,
            entities.StateIncarcerationPeriod,
            entities.StateSupervisionPeriod,
            entities.StateSupervisionCaseTypeEntry,
            entities.StateSupervisionViolation,
            entities.StateSupervisionViolationTypeEntry,
            entities.StateSupervisionViolatedConditionEntry,
            entities.StateSupervisionViolationResponse,
            entities.StateSupervisionViolationResponseDecisionEntry,
            entities.StateProgramAssignment,
            entities.StateAssessment,
            entities.StatePerson,
            entities.StateCharge,
            entities.StateEarlyDischarge,
            entities.StateSupervisionContact,
        ]

    @classmethod
    def required_reference_tables(cls) -> List[str]:
        return [
            STATE_CHARGE_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_NAME,
            STATE_PERSON_TO_STATE_STAFF_VIEW_NAME,
        ]

    @classmethod
    def required_state_based_reference_tables(cls) -> List[str]:
        return []

    @classmethod
    def state_specific_required_delegates(cls) -> List[Type[StateSpecificDelegate]]:
        return [
            StateSpecificIncarcerationNormalizationDelegate,
            StateSpecificSupervisionNormalizationDelegate,
            StateSpecificViolationResponseNormalizationDelegate,
            StateSpecificProgramAssignmentNormalizationDelegate,
            StateSpecificAssessmentNormalizationDelegate,
            StateSpecificSentenceNormalizationDelegate,
        ]

    @classmethod
    def state_specific_required_reference_tables(cls) -> Dict[StateCode, List[str]]:
        return {
            # We need to bring in the US_MO sentence status table to do
            # do state-specific processing of the sentences for normalizing
            # supervision periods.
            StateCode.US_MO: [US_MO_SENTENCE_STATUSES_VIEW_NAME]
        }

    @classmethod
    def entity_normalizer(cls) -> ComprehensiveEntityNormalizer:
        return ComprehensiveEntityNormalizer()

    @classmethod
    def required_entity_normalization_managers(
        cls,
    ) -> List[Type[EntityNormalizationManager]]:
        return [
            IncarcerationPeriodNormalizationManager,
            ProgramAssignmentNormalizationManager,
            SupervisionPeriodNormalizationManager,
            ViolationResponseNormalizationManager,
            AssessmentNormalizationManager,
            SentenceNormalizationManager,
            SupervisionContactNormalizationManager,
        ]

    def run_pipeline(self, p: Pipeline) -> None:
        """Logic for running the normalization pipeline."""
        # Workaround to load SQLAlchemy objects at start of pipeline. This is
        # necessary because the BuildRootEntity function tries to access attributes
        # of relationship properties on the SQLAlchemy room_schema_class before they
        # have been loaded. However, if *any* SQLAlchemy objects have been instantiated,
        # then the relationship properties are loaded and their attributes can be
        # successfully accessed.
        _ = schema.StatePerson()

        state_code = self.pipeline_parameters.state_code
        person_id_filter_set = (
            {
                int(person_id)
                for person_id in self.pipeline_parameters.person_filter_ids.split(" ")
            }
            if self.pipeline_parameters.person_filter_ids
            else None
        )

        required_reference_tables = (
            self.required_reference_tables().copy()
            + self.state_specific_required_reference_tables().get(
                StateCode(state_code.upper()), []
            )
        )

        required_state_based_reference_tables = (
            self.required_state_based_reference_tables().copy()
        )

        normalized_entity_types: Set[Type[Entity]] = set()
        normalized_entity_class_names: Set[str] = set()
        normalized_entity_associations: Set[str] = set()

        for manager in self.required_entity_normalization_managers():
            for normalized_entity_class in manager.normalized_entity_classes():
                normalized_entity_types.add(normalized_entity_class)
                normalized_entity_class_names.add(normalized_entity_class.__name__)
            for (
                child_entity_class,
                parent_entity_class,
            ) in manager.normalized_entity_associations():
                normalized_entity_associations.add(
                    f"{child_entity_class.__name__}_{parent_entity_class.__name__}"
                )

        writable_entities = (
            p
            | "Load required data"
            >> ExtractDataForPipeline(
                state_code=state_code,
                project_id=self.pipeline_parameters.project,
                entities_dataset=self.pipeline_parameters.data_input,
                normalized_entities_dataset=self.pipeline_parameters.normalized_input,
                reference_dataset=self.pipeline_parameters.reference_view_input,
                required_entity_classes=self.required_entities(),
                required_reference_tables=required_reference_tables,
                required_state_based_reference_tables=required_state_based_reference_tables,
                unifying_class=entities.StatePerson,
                unifying_id_field_filter_set=person_id_filter_set,
            )
            | "Normalize entities"
            >> beam.ParDo(
                NormalizeEntities(),
                state_code=self.pipeline_parameters.state_code,
                entity_normalizer=self.entity_normalizer(),
                state_specific_required_delegates=self.state_specific_required_delegates(),
            )
            | "Convert to dict to be written to BQ"
            >> beam.ParDo(
                NormalizedEntityTreeWritableDicts(),
                state_code=self.pipeline_parameters.state_code,
            ).with_outputs(
                *normalized_entity_class_names, *normalized_entity_associations
            )
        )

        for entity_class_name in normalized_entity_class_names:
            table_id = schema_utils.get_state_database_entity_with_name(
                entity_class_name
            ).__tablename__
            normalized_entity_type = normalized_entity_class_with_base_class_name(
                entity_class_name
            )
            normalized_entity_schema_fields = bq_schema_for_normalized_state_entity(
                normalized_entity_type
            )
            beam_schema_fields: List[beam_bigquery.TableFieldSchema] = [
                beam_bigquery.TableFieldSchema(name=field.name, type=field.field_type)
                for field in normalized_entity_schema_fields
            ]
            bq_schema = beam_bigquery.TableSchema(fields=beam_schema_fields)

            _ = getattr(writable_entities, entity_class_name) | (
                f"Write Normalized{entity_class_name} to BQ table: "
                f"{self.pipeline_parameters.output}.{table_id}"
            ) >> WriteToBigQuery(
                output_table=table_id,
                output_dataset=self.pipeline_parameters.output,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                schema=bq_schema,
            )

        for entity_association in normalized_entity_associations:
            child_class_name, parent_class_name = entity_association.split("_")
            table_id = schema_utils.get_state_database_association_with_names(
                child_class_name, parent_class_name
            ).name

            _ = getattr(writable_entities, entity_association) | (
                f"Write Normalized{child_class_name} to Normalized{parent_class_name} associations to BQ table: "
                f"{self.pipeline_parameters.output}.{table_id}"
            ) >> WriteToBigQuery(
                output_table=table_id,
                output_dataset=self.pipeline_parameters.output,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )


@with_input_types(
    beam.typehints.Tuple[int, Dict[str, Iterable[Any]]],
    str,
    ComprehensiveEntityNormalizer,
    List[Type[StateSpecificDelegate]],
)
@with_output_types(
    beam.typehints.Tuple[int, Dict[str, Sequence[Entity]], AdditionalAttributesMap],
)
class NormalizeEntities(beam.DoFn):
    """Normalizes entities."""

    # Silence `Method 'process_batch' is abstract in class 'DoFn' but is not overridden (abstract-method)`
    # pylint: disable=W0223

    # pylint: disable=arguments-differ
    def process(
        self,
        element: Tuple[int, Dict[str, Iterable[Any]]],
        state_code: str,
        entity_normalizer: ComprehensiveEntityNormalizer,
        state_specific_required_delegates: List[Type[StateSpecificDelegate]],
    ) -> Generator[
        Tuple[int, Dict[str, Sequence[Entity]], AdditionalAttributesMap],
        None,
        None,
    ]:
        """Runs the entities through normalization."""
        person_id, person_entities = element

        entity_kwargs = kwargs_for_entity_lists(person_entities)

        required_delegates = get_required_state_specific_delegates(
            state_code=state_code,
            required_delegates=state_specific_required_delegates,
            entity_kwargs=entity_kwargs,
        )

        all_kwargs: Dict[
            str, Union[Sequence[Entity], List[TableRow], StateSpecificDelegate]
        ] = {
            **entity_kwargs,
            **required_delegates,
        }

        (
            normalized_entities,
            additional_attributes_map,
        ) = entity_normalizer.normalize_entities(person_id, all_kwargs)

        yield person_id, normalized_entities, additional_attributes_map

    # Silence `Method 'process_batch' is abstract in class 'DoFn' but is not overridden (abstract-method)`
    # pylint: disable=W0223


@with_input_types(
    beam.typehints.Tuple[int, Dict[str, Sequence[Entity]], AdditionalAttributesMap], str
)
@with_output_types(beam.typehints.Dict[str, Any])
class NormalizedEntityTreeWritableDicts(beam.DoFn):
    """Builds a dictionary in the format necessary to write the output to BigQuery."""

    # Silence `Method 'process_batch' is abstract in class 'DoFn' but is not overridden (abstract-method)`
    # pylint: disable=W0223

    # pylint: disable=arguments-differ
    def process(
        self,
        element: Tuple[int, Dict[str, Sequence[Entity]], AdditionalAttributesMap],
        state_code: str,
    ) -> Generator[Dict[str, Any], None, None,]:
        """The beam.io.WriteToBigQuery transform requires elements to be in dictionary
        form, where the values are in formats as required by BigQuery I/O connector.

        For a list of required formats, see the "Data types" section of:
            https://beam.apache.org/documentation/io/built-in/google-bigquery/

        Args:
            element: A tuple containing the person_id of a single person,
                a dictionary with all normalized entities indexed by the name of the
                entity, and an AdditionalAttributesMap storing the attributes
                unique to the Normalized version of each entity that will be
                written in the output

        Yields:
            A dictionary representation of the normalized entity in the format
                Dict[str, Any] so that it can be written to BigQuery.
        """
        person_id, normalized_entities, additional_attributes_map = element

        field_index = CoreEntityFieldIndex()
        normalized_entity_list = [
            entity
            for entity_list in normalized_entities.values()
            for entity in entity_list
        ]
        tagged_entity_dict_outputs = convert_entities_to_normalized_dicts(
            person_id=person_id,
            state_code=state_code,
            entities=normalized_entity_list,
            additional_attributes_map=additional_attributes_map,
            field_index=field_index,
        )

        for entity_name, entity_dict in tagged_entity_dict_outputs:
            output_dict = json_serializable_dict(entity_dict)

            yield beam.pvalue.TaggedOutput(entity_name, output_dict)
