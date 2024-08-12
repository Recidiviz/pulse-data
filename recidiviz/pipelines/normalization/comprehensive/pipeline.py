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
recidiviz/tools/calculator/run_sandbox_calculation_pipeline.py for details on how to launch a
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
from more_itertools import one

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_query_provider import StateFilteredQueryProvider
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.reference.state_person_to_state_staff import (
    STATE_PERSON_TO_STATE_STAFF_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.us_mo_sentence_statuses import (
    US_MO_SENTENCE_STATUSES_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
    normalized_entity_class_with_base_class_name,
)
from recidiviz.persistence.entity.serialization import json_serializable_dict
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
)
from recidiviz.pipelines.normalization.utils.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    IncarcerationPeriodNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.program_assignment_normalization_manager import (
    ProgramAssignmentNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.sentence_normalization_manager import (
    SentenceNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.staff_role_period_normalization_manager import (
    StaffRolePeriodNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_contact_normalization_manager import (
    SupervisionContactNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_period_normalization_manager import (
    SupervisionPeriodNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_violation_responses_normalization_manager import (
    ViolationResponseNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalized_entity_conversion_utils import (
    bq_schema_for_normalized_state_entity,
    convert_entities_to_normalized_dicts,
)
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import WriteToBigQuery
from recidiviz.pipelines.utils.beam_utils.extractor_utils import (
    ExtractRootEntityDataForPipeline,
)
from recidiviz.pipelines.utils.execution_utils import TableRow, kwargs_for_entity_lists
from recidiviz.utils.types import assert_type


# TODO(#21376) Properly refactor once strategy for separate normalization is defined.
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
    def required_entities(cls) -> Dict[Type[Entity], List[Type[Entity]]]:
        # Note: This is a list of all of the entities that are required to
        # perform entity normalization on all entities with normalization
        # processes. This is *not* the list of entities that are normalized by
        # this pipeline. See the normalized_entity_classes attribute of each of
        # the EntityNormalizationManagers in the
        # required_entity_normalization_managers below to see all entities
        # normalized by this pipeline.
        return {
            entities.StatePerson: [
                entities.StateSupervisionSentence,
                entities.StateIncarcerationSentence,
                entities.StateSentence,
                entities.StateSentenceStatusSnapshot,
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
            ],
            entities.StateStaff: [
                entities.StateStaffRolePeriod,
                entities.StateStaffSupervisorPeriod,
            ],
        }

    @classmethod
    def input_reference_view_builders(
        cls,
    ) -> Dict[Type[Entity], List[BigQueryViewBuilder]]:
        return {
            entities.StatePerson: [
                STATE_PERSON_TO_STATE_STAFF_VIEW_BUILDER,
            ],
            entities.StateStaff: [],
        }

    @classmethod
    def state_specific_input_reference_view_builders(
        cls,
    ) -> Dict[Type[Entity], Dict[StateCode, List[BigQueryViewBuilder]]]:
        return {
            entities.StatePerson: {
                # We need to bring in the US_MO sentence status table to do
                # do state-specific processing of the sentences for normalizing
                # supervision periods.
                StateCode.US_MO: [
                    # TODO(#30199): Remove dependency on this view in favor of StateSentenceStatusSnapshot
                    US_MO_SENTENCE_STATUSES_VIEW_BUILDER
                ],
            },
            entities.StateStaff: {},
        }

    # TODO(#29514): Update this to build reference queries from templates that we can
    #  inject the state-specific us_xx_state dataset into.
    @classmethod
    def _input_query_providers_for_root_entity(
        cls,
        root_entity_cls: Type[Entity],
        state_code: StateCode,
        address_overrides: BigQueryAddressOverrides | None,
    ) -> Dict[str, StateFilteredQueryProvider]:
        all_builders = {}
        for vb in cls.input_reference_view_builders()[root_entity_cls]:
            all_builders[vb.view_id] = StateFilteredQueryProvider(
                original_query=vb.build(address_overrides=address_overrides),
                state_code_filter=state_code,
            )
        builders_by_state = cls.state_specific_input_reference_view_builders()[
            root_entity_cls
        ]
        for vb in builders_by_state.get(state_code, []):
            all_builders[vb.view_id] = StateFilteredQueryProvider(
                original_query=vb.build(address_overrides=address_overrides),
                state_code_filter=state_code,
            )
        return all_builders

    @classmethod
    def all_input_reference_query_providers(
        cls, state_code: StateCode, address_overrides: BigQueryAddressOverrides | None
    ) -> Dict[str, StateFilteredQueryProvider]:
        all_builders: Dict[str, StateFilteredQueryProvider] = {}

        for root_entity_cls in [entities.StatePerson, entities.StateStaff]:
            all_builders = {
                **all_builders,
                **cls._input_query_providers_for_root_entity(
                    root_entity_cls, state_code, address_overrides
                ),
            }
        return all_builders

    @classmethod
    def required_entity_normalization_managers(
        cls,
    ) -> Dict[Type[Entity], List[Type[EntityNormalizationManager]]]:
        return {
            entities.StatePerson: [
                IncarcerationPeriodNormalizationManager,
                ProgramAssignmentNormalizationManager,
                SupervisionPeriodNormalizationManager,
                ViolationResponseNormalizationManager,
                AssessmentNormalizationManager,
                SentenceNormalizationManager,
                SupervisionContactNormalizationManager,
            ],
            entities.StateStaff: [StaffRolePeriodNormalizationManager],
        }

    def run_pipeline(self, p: Pipeline) -> None:
        """Logic for running the normalization pipeline."""
        # Workaround to load SQLAlchemy objects at start of pipeline. This is
        # necessary because the BuildEntity function tries to access attributes
        # of relationship properties on the SQLAlchemy room_schema_class before they
        # have been loaded. However, if *any* SQLAlchemy objects have been instantiated,
        # then the relationship properties are loaded and their attributes can be
        # successfully accessed.
        _ = schema.StatePerson()
        _ = schema.StateStaff()

        state_code = StateCode(self.pipeline_parameters.state_code.upper())
        person_id_filter_set = (
            {
                int(person_id)
                for person_id in self.pipeline_parameters.person_filter_ids.split(" ")
            }
            if self.pipeline_parameters.person_filter_ids
            else None
        )

        # TODO(#21376) Properly refactor once strategy for separate normalization is defined.
        for root_entity_type in [entities.StatePerson, entities.StateStaff]:
            normalized_entity_types: Set[Type[Entity]] = set()
            normalized_entity_class_names: Set[str] = set()
            normalized_entity_associations: Set[str] = set()

            for manager in self.required_entity_normalization_managers().get(
                root_entity_type, []
            ):
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

            reference_data_queries_by_name = (
                self._input_query_providers_for_root_entity(
                    root_entity_cls=root_entity_type,
                    state_code=state_code,
                    address_overrides=self.pipeline_parameters.input_dataset_overrides,
                )
            )

            writable_entities = (
                p
                | f"Load required data for {root_entity_type.__name__}"
                >> ExtractRootEntityDataForPipeline(
                    state_code=state_code,
                    project_id=self.pipeline_parameters.project,
                    entities_dataset=self.pipeline_parameters.state_data_input,
                    required_entity_classes=self.required_entities().get(
                        root_entity_type
                    ),
                    reference_data_queries_by_name=reference_data_queries_by_name,
                    root_entity_cls=root_entity_type,
                    root_entity_id_filter_set=person_id_filter_set,
                )
                | f"Normalize entities for {root_entity_type.__name__}"
                >> beam.ParDo(
                    NormalizeEntities(),
                    root_entity_type=root_entity_type,
                    entity_normalizer=ComprehensiveEntityNormalizer(state_code),
                )
                | f"Convert to dict to be written to BQ {root_entity_type.__name__}"
                >> beam.ParDo(
                    NormalizedEntityTreeWritableDicts(),
                    state_code=state_code.value,
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
                    beam_bigquery.TableFieldSchema(
                        name=field.name, type=field.field_type
                    )
                    for field in normalized_entity_schema_fields
                ]
                bq_schema = beam_bigquery.TableSchema(fields=beam_schema_fields)

                _ = (
                    getattr(writable_entities, entity_class_name)
                    | f"Map Normalized{entity_class_name} to its primary key"
                    >> beam.Map(
                        self.get_primary_key_from_entity_dict,
                        entity_primary_key_name=normalized_entity_type.get_class_id_name(),  # type: ignore
                    )
                    | f"Group Normalized{entity_class_name} by primary key"
                    >> beam.GroupByKey()
                    | f"Check if primary keys are unique for {entity_class_name}"
                    >> beam.MapTuple(lambda _id, entities: one(entities))
                    | f"Write Normalized{entity_class_name} to BQ table: {self.pipeline_parameters.output}.{table_id}"
                    >> WriteToBigQuery(
                        output_table=table_id,
                        output_dataset=self.pipeline_parameters.output,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                        schema=bq_schema,
                    )
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

    def get_primary_key_from_entity_dict(
        self, entity_dict: Dict[str, Any], entity_primary_key_name: str
    ) -> Tuple[int, Dict[str, Any]]:
        """Returns the primary key of the entity_dict."""
        return assert_type(entity_dict.get(entity_primary_key_name), int), entity_dict


@with_input_types(
    beam.typehints.Tuple[int, Dict[str, Iterable[Any]]],
    Type[Entity],
    ComprehensiveEntityNormalizer,
)
@with_output_types(
    beam.typehints.Tuple[
        int,
        str,
        Dict[str, Sequence[Entity]],
        AdditionalAttributesMap,
    ],
)
class NormalizeEntities(beam.DoFn):
    """Normalizes entities."""

    # Silence `Method 'process_batch' is abstract in class 'DoFn' but is not overridden (abstract-method)`
    # pylint: disable=W0223

    # pylint: disable=arguments-differ
    # TODO(#21376) Properly refactor once strategy for separate normalization is defined.
    def process(
        self,
        element: Tuple[int, Dict[str, Iterable[Any]]],
        root_entity_type: Type[Entity],
        entity_normalizer: ComprehensiveEntityNormalizer,
    ) -> Generator[
        Tuple[
            int,
            str,
            Dict[str, Sequence[Entity]],
            AdditionalAttributesMap,
        ],
        None,
        None,
    ]:
        """Runs the entities through normalization."""
        root_entity_id, person_entities = element

        all_kwargs: Dict[
            str, Union[Sequence[Entity], List[TableRow]]
        ] = kwargs_for_entity_lists(person_entities)

        (
            normalized_entities,
            additional_attributes_map,
        ) = entity_normalizer.normalize_entities(
            root_entity_id, root_entity_type, all_kwargs
        )

        yield (
            root_entity_id,
            root_entity_type.get_class_id_name(),
            normalized_entities,
            additional_attributes_map,
        )

    # Silence `Method 'process_batch' is abstract in class 'DoFn' but is not overridden (abstract-method)`
    # pylint: disable=W0223


@with_input_types(
    beam.typehints.Tuple[
        int, str, Dict[str, Sequence[Entity]], AdditionalAttributesMap
    ],
    str,
)
@with_output_types(beam.typehints.Dict[str, Any])
class NormalizedEntityTreeWritableDicts(beam.DoFn):
    """Builds a dictionary in the format necessary to write the output to BigQuery."""

    # Silence `Method 'process_batch' is abstract in class 'DoFn' but is not overridden (abstract-method)`
    # pylint: disable=W0223

    # pylint: disable=arguments-differ
    # TODO(#21376) Properly refactor once strategy for separate normalization is defined.
    def process(
        self,
        element: Tuple[
            int,
            str,
            Dict[str, Sequence[Entity]],
            AdditionalAttributesMap,
        ],
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
        (
            root_entity_id,
            root_entity_id_name,
            normalized_entities,
            additional_attributes_map,
        ) = element

        normalized_entity_list = [
            entity
            for entity_list in normalized_entities.values()
            for entity in entity_list
        ]
        tagged_entity_dict_outputs = convert_entities_to_normalized_dicts(
            root_entity_id=root_entity_id,
            root_entity_id_name=root_entity_id_name,
            state_code=state_code,
            entities=normalized_entity_list,
            additional_attributes_map=additional_attributes_map,
        )

        for entity_name, entity_dict in tagged_entity_dict_outputs:
            output_dict = json_serializable_dict(entity_dict)

            yield beam.pvalue.TaggedOutput(entity_name, output_dict)
