# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Utils for extracting entities from data sources to be used in pipeline
calculations."""
import abc
from collections import defaultdict
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

import apache_beam as beam
from apache_beam import PCollection, Pipeline
from apache_beam.pvalue import AsList, PBegin
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateEntity,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    state_base_entity_class_for_entity_class,
)
from recidiviz.calculator.pipeline.utils.beam_utils.bigquery_io_utils import (
    ConvertDictToKVTuple,
    ReadFromBigQuery,
)
from recidiviz.calculator.pipeline.utils.execution_utils import (
    EntityAssociation,
    EntityClassName,
    EntityRelationshipKey,
    TableName,
    TableRow,
    UnifyingId,
    select_query,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import (
    UsMoIncarcerationSentence,
    UsMoSupervisionSentence,
)
from recidiviz.calculator.query.state.views.reference.us_mo_sentence_statuses import (
    US_MO_SENTENCE_STATUSES_VIEW_NAME,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.entity import entity_utils
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import SchemaEdgeDirectionChecker
from recidiviz.persistence.entity.state import entities as state_entities

UNIFYING_ID_KEY = "unifying_id"
ROOT_ENTITY_ID_FIELD_KEY = "root_entity_id_field"
RELATED_ENTITY_ID_FIELD_KEY = "related_entity_id_field"

EntityRelationshipDetails = NamedTuple(
    "EntityRelationshipDetails",
    [
        ("property_name", str),
        ("property_entity_class", Type[Entity]),
        ("is_forward_ref", bool),
        ("association_table", str),
        ("association_table_entity_id_field", str),
    ],
)


class ExtractDataForPipeline(beam.PTransform):
    """Builds all of the required entities for a pipeline, and pulls in any
    required reference tables. Hydrates all existing connections between required
    entities.
    """

    def __init__(
        self,
        state_code: str,
        project_id: str,
        entities_dataset: str,
        normalized_entities_dataset: str,
        reference_dataset: str,
        required_entity_classes: Optional[
            List[Union[Type[Entity], Type[NormalizedStateEntity]]]
        ],
        required_reference_tables: Optional[List[str]],
        required_state_based_reference_tables: Optional[List[str]],
        unifying_class: Type[Entity],
        unifying_id_field_filter_set: Optional[Set[UnifyingId]] = None,
    ):
        """Initializes the PTransform with the required arguments.

        Arguments:
            state_code: The state code to filter all results by
            project_id: The project_id of the BigQuery project to query from.
            entities_dataset: The name of the dataset_id to read from BigQuery.
            normalized_entities_dataset: The name of the dataset_id to read from
                BigQuery for any entities in required_entity_classes that are
                of type NormalizedStateEntity.
            required_entity_classes: The list of required entity classes for the
                pipeline. Must not contain any duplicates of any entities.
            unifying_class: The Entity type whose id should be used to connect the
                required entities to each other. All entities listed in
                |required_entity_classes| must have this entity's id field, but this
                class does not necessarily need to be included in the list of
                |required_entity_classes|. This value is usually StatePerson.
            unifying_id_field_filter_set: When non-empty, we will only build entity
                objects that can be connected to root entities with one of these
                unifying ids.
        """
        super().__init__()

        self._state_code = state_code
        self._project_id = project_id

        if not entities_dataset:
            raise ValueError("No valid data source passed to the pipeline.")
        self._entities_dataset = entities_dataset
        self._normalized_entities_dataset = normalized_entities_dataset

        if not reference_dataset:
            raise ValueError("No valid data reference source passed to the pipeline.")
        self._reference_dataset = reference_dataset
        self._required_reference_tables = required_reference_tables or []
        self._required_state_based_reference_tables = (
            required_state_based_reference_tables or []
        )

        if required_entity_classes and len(set(required_entity_classes)) != len(
            required_entity_classes
        ):
            raise ValueError(
                "List of required entities should only contain each "
                "required entity class once. Found duplicates: "
                f"{required_entity_classes}."
            )

        self._entity_class_to_hydrated_entity_class: Dict[
            Type[Entity], Union[Type[Entity], Type[NormalizedStateEntity]]
        ] = {}

        for entity_class in required_entity_classes or []:
            base_entity_class = state_base_entity_class_for_entity_class(entity_class)
            self._entity_class_to_hydrated_entity_class[
                base_entity_class
            ] = entity_class

        self._direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()

        if not unifying_class:
            raise ValueError("No valid unifying_class passed to the pipeline.")
        self._unifying_class = unifying_class
        self._unifying_id_field = unifying_class.get_class_id_name()
        self._unifying_id_field_filter_set = unifying_id_field_filter_set

    def _get_relationships_to_hydrate(
        self,
    ) -> Dict[EntityClassName, List[EntityRelationshipDetails]]:
        """Determines the set of relationships that need to be hydrated between the
        list of required entities. Returns the information that we need to query for
        the values to hydrate each relationship."""
        relationships_to_hydrate: Dict[
            EntityClassName, List[EntityRelationshipDetails]
        ] = {}

        for root_entity_class in self._entity_class_to_hydrated_entity_class:
            root_entity_class_name = root_entity_class.__name__
            if root_entity_class_name not in relationships_to_hydrate:
                relationships_to_hydrate[root_entity_class_name] = []

            root_schema_class = schema_utils.get_state_database_entity_with_name(
                root_entity_class_name
            )

            # This is to enforce typing for mypy
            if not issubclass(root_schema_class, (DatabaseEntity, StateBase)):
                raise ValueError(
                    "Expected root_schema_class to be subclass of "
                    f"DatabaseEntity and StateBase: {root_schema_class}."
                )

            names_to_properties = (
                root_schema_class.get_relationship_property_names_and_properties()
            )
            for property_name, property_object in names_to_properties.items():
                property_class_name = (
                    root_schema_class.get_relationship_property_class_name(
                        property_name
                    )
                )

                property_entity_class = (
                    entity_utils.get_entity_class_in_module_with_name(
                        state_entities, property_class_name
                    )
                )

                property_schema_class = (
                    schema_utils.get_state_database_entity_with_name(
                        property_entity_class.__name__
                    )
                )

                if property_entity_class == state_entities.StatePerson:
                    # Since all person-level entities are connected to StatePerson,
                    # not hydrating this relationship significantly reduces the size
                    # of the python objects.
                    continue

                if property_entity_class in self._entity_class_to_hydrated_entity_class:
                    is_property_forward_edge = self._direction_checker.is_higher_ranked(
                        root_schema_class, property_schema_class
                    )

                    # Many-to-many relationship
                    if property_object.secondary is not None:
                        association_table = property_object.secondary.name
                        association_table_entity_id_field = (
                            property_entity_class.get_class_id_name()
                        )
                    elif is_property_forward_edge:
                        # 1-to-many relationship
                        if property_object.uselist:
                            association_table = property_schema_class.__tablename__
                            association_table_entity_id_field = (
                                property_entity_class.get_class_id_name()
                            )

                        # 1-to-1 relationship (from parent class perspective)
                        else:
                            association_table = root_schema_class.__tablename__
                            association_table_entity_id_field = (
                                property_object.key + "_id"
                            )
                    else:
                        association_table = root_schema_class.__tablename__
                        association_table_entity_id_field = (
                            property_entity_class.get_class_id_name()
                        )

                    relationships_to_hydrate[root_entity_class_name].append(
                        EntityRelationshipDetails(
                            property_name=property_name,
                            property_entity_class=property_entity_class,
                            is_forward_ref=is_property_forward_edge,
                            association_table=association_table,
                            association_table_entity_id_field=association_table_entity_id_field,
                        )
                    )
        return relationships_to_hydrate

    def _get_associations_for_root_entity_class(
        self,
        pipeline: PBegin,
        root_entity_class: Type[Entity],
        hydrated_association_info: Dict[
            EntityClassName, PCollection[Tuple[UnifyingId, EntityAssociation]]
        ],
        relationships_to_hydrate: List[EntityRelationshipDetails],
    ) -> None:
        """Adds association values for the provided |relationships_to_hydrate| to the
        given |hydrated_association_info| dict, if necessary.
        """
        for relationship_property in relationships_to_hydrate:
            related_entity_class = relationship_property.property_entity_class
            related_entity_class_name = related_entity_class.__name__

            if self._unifying_class in (related_entity_class, root_entity_class):
                # All entities will be grouped with the unifying class that they are
                # related to, so there's no need to query for these relationships
                continue

            relationship_key = (
                f"{root_entity_class.__name__}.{related_entity_class_name}"
            )
            reverse_relationship_key = (
                f"{related_entity_class_name}.{root_entity_class.__name__}"
            )

            if (
                relationship_key in hydrated_association_info
                or reverse_relationship_key in hydrated_association_info
            ):
                # The values for this relationship have already been
                # determined
                continue

            root_entity_class_being_hydrated = (
                self._entity_class_to_hydrated_entity_class[root_entity_class]
            )
            related_entity_class_being_hydrated = (
                self._entity_class_to_hydrated_entity_class[related_entity_class]
            )

            root_entity_is_normalized = issubclass(
                root_entity_class_being_hydrated,
                NormalizedStateEntity,
            )

            related_entity_is_normalized = issubclass(
                related_entity_class_being_hydrated,
                NormalizedStateEntity,
            )

            # Assert that either both entities are Normalized versions or neither
            # of them are
            if root_entity_is_normalized != related_entity_is_normalized:
                raise NotImplementedError(
                    "Hydrating Normalized entities that have "
                    "relationships to other non-normalized entities in the "
                    "pipeline is not yet supported."
                )

            # Get the association values for this relationship
            association_values = (
                pipeline | f"Extract association values for "
                f"{root_entity_class_being_hydrated.__name__} to "
                f"{related_entity_class_being_hydrated.__name__} relationship."
                >> _ExtractAssociationValues(
                    project_id=self._project_id,
                    entities_dataset=self._entities_dataset,
                    normalized_entities_dataset=self._normalized_entities_dataset,
                    root_entity_class=root_entity_class_being_hydrated,
                    related_entity_class=related_entity_class_being_hydrated,
                    unifying_id_field=self._unifying_id_field,
                    association_table=relationship_property.association_table,
                    related_id_field=relationship_property.association_table_entity_id_field,
                    unifying_id_field_filter_set=self._unifying_id_field_filter_set,
                    state_code=self._state_code,
                )
            )

            hydrated_association_info[relationship_key] = association_values

    def get_shallow_hydrated_entity_pcollection(
        self,
        pipeline: PBegin,
        entity_class: Union[Type[Entity], Type[NormalizedStateEntity]],
    ) -> PCollection[Tuple[UnifyingId, Entity]]:
        """Returns the hydrated entities of type |entity_class| as a PCollection,
        where each element is a tuple in the format: (unifying_id, entity).
        """
        return (
            pipeline
            | f"Extract {entity_class.__name__} instances"
            >> _ExtractAllEntitiesOfType(
                project_id=self._project_id,
                entities_dataset=self._entities_dataset,
                normalized_entities_dataset=self._normalized_entities_dataset,
                entity_class=entity_class,
                unifying_id_field=self._unifying_id_field,
                unifying_id_field_filter_set=self._unifying_id_field_filter_set,
                state_code=self._state_code,
            )
        )

    def get_associations_info(
        self,
        pipeline: PBegin,
        relationships_to_hydrate: Dict[str, List[EntityRelationshipDetails]],
    ) -> Dict[EntityClassName, PCollection[Tuple[UnifyingId, EntityAssociation]]]:
        """Gets all information required to associate the required entities to each
        other."""
        hydrated_association_info: Dict[
            EntityClassName, PCollection[Tuple[UnifyingId, EntityAssociation]]
        ] = {}
        for root_entity_class in self._entity_class_to_hydrated_entity_class:
            # Populate relationships_to_hydrate with the relationships between all
            # required entities that require hydrating, and add association values
            # for these relationships to the entities_and_associations dict
            self._get_associations_for_root_entity_class(
                pipeline,
                root_entity_class=root_entity_class,
                hydrated_association_info=hydrated_association_info,
                relationships_to_hydrate=relationships_to_hydrate[
                    root_entity_class.__name__
                ],
            )
        return hydrated_association_info

    def expand(
        self, input_or_inputs: PBegin
    ) -> PCollection[
        Tuple[
            UnifyingId,
            Dict[
                Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]
            ],
        ]
    ]:
        """Does the work of building all entities required for a pipeline
        and hydrating all existing relationships between the entities."""
        shallow_hydrated_entities: Dict[
            EntityClassName, PCollection[Tuple[UnifyingId, Entity]]
        ] = {}

        for (
            entity_class,
            entity_class_for_hydration,
        ) in self._entity_class_to_hydrated_entity_class.items():
            shallow_hydrated_entities[
                entity_class.__name__
            ] = self.get_shallow_hydrated_entity_pcollection(
                pipeline=input_or_inputs,
                entity_class=entity_class_for_hydration,
            )

        relationships_to_hydrate = self._get_relationships_to_hydrate()

        hydrated_association_info: Dict[
            EntityClassName, PCollection[Tuple[UnifyingId, EntityAssociation]]
        ] = self.get_associations_info(
            pipeline=input_or_inputs,
            relationships_to_hydrate=relationships_to_hydrate,
        )

        reference_data: Dict[TableName, PCollection[Tuple[UnifyingId, TableRow]]] = {}

        for table_id in self._required_reference_tables:
            reference_data[
                table_id
            ] = input_or_inputs | f"Load {table_id}" >> ImportTableAsKVTuples(
                project_id=self._project_id,
                dataset_id=self._reference_dataset,
                table_id=table_id,
                table_key=self._unifying_id_field,
                state_code_filter=self._state_code,
                unifying_id_field=self._unifying_id_field,
                unifying_id_filter_set=self._unifying_id_field_filter_set,
            )

        state_based_reference_data: Dict[TableName, PCollection[TableRow]] = {}
        for table_id in self._required_state_based_reference_tables:
            state_based_reference_data[
                table_id
            ] = input_or_inputs | f"Load {table_id}" >> ImportTable(
                project_id=self._project_id,
                dataset_id=self._reference_dataset,
                table_id=table_id,
                state_code_filter=self._state_code,
            )

        entities_and_associations: Dict[
            Union[EntityClassName, EntityRelationshipKey, TableName],
            PCollection[Tuple[UnifyingId, Union[EntityAssociation, Entity, TableRow]]],
        ] = {**shallow_hydrated_entities, **hydrated_association_info, **reference_data}

        # Group all entities and association tuples by the unifying_id
        entities_and_association_info_by_unifying_id: PCollection[
            Tuple[
                UnifyingId,
                Dict[
                    Union[EntityClassName, EntityRelationshipKey, TableName],
                    Union[List[Entity], List[EntityAssociation], List[TableRow]],
                ],
            ]
        ] = (
            entities_and_associations
            | f"Group entities, associations, and reference tables by"
            f" {self._unifying_id_field}" >> beam.CoGroupByKey()
        )

        fully_connected_hydrated_entities = (
            entities_and_association_info_by_unifying_id
            | "Connect all entity relationships"
            >> beam.ParDo(
                _ConnectHydratedRelatedEntities(),
                unifying_class=self._unifying_class,
                relationships_to_hydrate=relationships_to_hydrate,
            )
        )

        fully_connected_entities_with_converted_state_types = (
            fully_connected_hydrated_entities
            | "Convert entities to state-specific type"
            >> beam.ParDo(
                ConvertEntitiesToStateSpecificTypes(), state_code=self._state_code
            )
        )

        final_entities_with_state_based_reference = (
            fully_connected_entities_with_converted_state_types
            | "Attach state-based reference data to entities"
            >> beam.ParDo(
                _AttachStateBasedReferenceDataToEntities(),
                state_based_reference_tables=self._required_state_based_reference_tables,
                **{
                    table_id: AsList(rows)
                    for table_id, rows in state_based_reference_data.items()
                },
            )
        )

        return final_entities_with_state_based_reference


@with_input_types(
    beam.typehints.Tuple[
        UnifyingId,
        Dict[
            Union[EntityClassName, EntityRelationshipKey, TableName],
            Union[List[Entity], List[EntityAssociation], List[TableRow]],
        ],
    ],
    beam.typehints.Optional[Type[Entity]],
    beam.typehints.Dict[str, List[EntityRelationshipDetails]],
)
@with_output_types(
    beam.typehints.Tuple[
        UnifyingId,
        Dict[Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]],
    ],
)
class _ConnectHydratedRelatedEntities(beam.DoFn):
    """Connects all entities of the |root_entity_class| type to all
    hydrated related entities for all relationships listed in |relationships|.
    """

    @staticmethod
    def _get_associations(
        *,
        entity_class_name: EntityClassName,
        related_entity_class_name: EntityClassName,
        associations: Dict[EntityRelationshipKey, List[EntityAssociation]],
    ) -> Dict[int, List[int]]:
        """Returns a dictionary where the keys are primary keys for all entities of type
        |entity_class_name| and the value is a list of all entities of type
        |related_entity_class_name| that have an association with that entity.
        """
        relationship_key = f"{entity_class_name}.{related_entity_class_name}"
        # If this relationship isn't stored in the list of associations,
        # then the reverse relationship must be
        is_reverse_relationship = relationship_key not in associations

        if is_reverse_relationship:
            relationship_key = f"{related_entity_class_name}.{entity_class_name}"

        # Get the list of association values between the entity and
        # the related entity
        association_tuples: List[EntityAssociation] = associations[relationship_key]

        # Format the result so the second id is always the id of the related entity
        association_values = defaultdict(list)
        for pk1, pk2 in association_tuples:
            if is_reverse_relationship:
                association_values[pk2].append(pk1)
            else:
                association_values[pk1].append(pk2)
        return association_values

    def _get_fully_hydrated_entities_of_type(
        self,
        *,
        unifying_class_name: EntityClassName,
        entity_class_name: EntityClassName,
        relationships_to_hydrate: List[EntityRelationshipDetails],
        entities: Dict[EntityClassName, List[Entity]],
        associations: Dict[EntityRelationshipKey, List[EntityAssociation]],
    ) -> List[Entity]:
        """Returns the list of entities with all relationships to related entities
        fully hydrated."""
        # Get root entities as a list
        entity_list: List[Entity] = entities[entity_class_name]

        if not entity_list:
            return []

        for relationship_details in relationships_to_hydrate:
            related_entity_class = relationship_details.property_entity_class
            relationship_property_name = relationship_details.property_name

            related_entity_class_name = related_entity_class.__name__

            # Get entities of the type that are stored on this property as a list
            related_entity_candidates: List[Entity] = entities[
                related_entity_class_name
            ]
            related_entity_candidates_by_id: Dict[int, Entity] = {
                e.get_id(): e for e in related_entity_candidates
            }

            if unifying_class_name in (entity_class_name, related_entity_class_name):
                # If either this entity or the related entity is the unifying class,
                # then we know there are direct relationships between this entity and
                # all related entities of this this type.
                for entity in entity_list:
                    entity.set_field_from_list(
                        relationship_property_name, related_entity_candidates
                    )
            else:
                # Map of entity id to list of related entity ids
                associated_entity_ids: Dict[int, List[int]] = self._get_associations(
                    entity_class_name=entity_class_name,
                    related_entity_class_name=related_entity_class_name,
                    associations=associations,
                )

                if not related_entity_candidates and not associated_entity_ids:
                    continue

                if associated_entity_ids and not related_entity_candidates:
                    raise ValueError(
                        f"If there are entities of type "
                        f"{related_entity_class_name} and there exists "
                        f"relationships between that entity and the "
                        f"{entity_class_name} entity, then the "
                        "related_entity_candidates should be non-null."
                        f"related_entity_candidates: ["
                        f"{related_entity_candidates}]\n"
                        f"associations: [{associations}]\n"
                        f"entities: [{entities}]"
                    )

                for entity in entity_list:
                    related_entity_ids = associated_entity_ids[entity.get_id()]

                    if not related_entity_ids:
                        continue

                    related_entities = [
                        related_entity_candidates_by_id[related_entity_id]
                        for related_entity_id in related_entity_ids
                    ]

                    entity.set_field_from_list(
                        relationship_property_name, related_entities
                    )

        return entity_list

    @staticmethod
    def _split_element_data(
        element_data: Dict[
            Union[EntityClassName, EntityRelationshipKey, TableName],
            Union[List[Entity], List[EntityAssociation], List[TableRow]],
        ],
        relationships_to_hydrate: Dict[
            EntityClassName, List[EntityRelationshipDetails]
        ],
    ) -> Tuple[
        Dict[EntityClassName, List[Entity]],
        Dict[EntityRelationshipKey, List[EntityAssociation]],
        Dict[TableName, List[TableRow]],
    ]:
        """Splits the |element_data| into three distinct dictionaries with consistent
        value types."""
        entities: Dict[EntityClassName, List[Entity]] = {}
        associations: Dict[EntityRelationshipKey, List[EntityAssociation]] = {}
        reference_table_data: Dict[TableName, List[TableRow]] = {}

        for key, value in element_data.items():
            list_elements = list(value)

            if key in relationships_to_hydrate:
                # The key is an EntityClassName, assert all list items are Entities
                entities[key] = []
                for list_element in list_elements:
                    assert isinstance(list_element, Entity)
                    entities[key].append(list_element)
            elif "." in key:
                # All EntityRelationshipKeys are formatted as EntityName.OtherEntityName
                # Assert all list items are of type Tuple[int, int]
                associations[key] = []
                for list_element in list_elements:
                    if not isinstance(list_element, tuple):
                        raise ValueError(
                            "Expected list to contain all Tuples. "
                            f"Found: {list_element}."
                        )
                    value_1, value_2 = list_element
                    if not isinstance(value_1, int) or not isinstance(value_2, int):
                        raise ValueError(
                            "Expected tuple of integers, found: ({value_1}, {value2})"
                        )
                    associations[key].append((value_1, value_2))
            else:
                # The key is a TableName, assert all list items are TableRows (of
                # type Dict[str, str])
                reference_table_data[key] = []
                for list_element in list_elements:
                    if not isinstance(list_element, Dict):
                        raise ValueError(
                            "Expected list to contain all elements of "
                            f"type Dict[str, str]. Found: {list_element}."
                        )
                    reference_table_data[key].append(list_element)

        return entities, associations, reference_table_data

    # pylint: disable=arguments-differ
    def process(
        self,
        element: Tuple[
            UnifyingId,
            Dict[
                Union[EntityClassName, EntityRelationshipKey, TableName],
                Union[List[Entity], List[EntityAssociation], List[TableRow]],
            ],
        ],
        unifying_class: Type[Entity],
        relationships_to_hydrate: Dict[
            EntityClassName, List[EntityRelationshipDetails]
        ],
    ) -> Iterable[
        Tuple[
            UnifyingId,
            Dict[
                Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]
            ],
        ]
    ]:
        """Runs the process for getting fully connected hydrated entities."""
        fully_connected_hydrated_entities: Dict[EntityClassName, List[Entity]] = {}

        unifying_id, element_data = element
        if unifying_id is None:
            raise ValueError("Found unexpected null unifying_id.")

        entities, associations, reference_table_data = self._split_element_data(
            element_data=element_data, relationships_to_hydrate=relationships_to_hydrate
        )

        for root_entity_class_name, relationships in relationships_to_hydrate.items():
            fully_connected_hydrated_entities[
                root_entity_class_name
            ] = self._get_fully_hydrated_entities_of_type(
                entity_class_name=root_entity_class_name,
                unifying_class_name=unifying_class.__name__,
                relationships_to_hydrate=relationships,
                entities=entities,
                associations=associations,
            )

        all_pipeline_data: Dict[
            Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]
        ] = {**fully_connected_hydrated_entities, **reference_table_data}

        yield unifying_id, all_pipeline_data

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


@with_input_types(
    beam.typehints.Dict[Any, Any],
    **{
        ROOT_ENTITY_ID_FIELD_KEY: str,
        RELATED_ENTITY_ID_FIELD_KEY: str,
    },
)
@with_output_types(beam.typehints.Tuple[UnifyingId, EntityAssociation])
class _PackageAssociationIDValues(beam.DoFn):
    """Forms a tuple from the given element with the following format:

    (unifying_id, (root_entity_id, related_entity_id))
    """

    def process(
        self, element: Dict[Any, Any], *_args, **kwargs
    ) -> Iterable[Tuple[UnifyingId, EntityAssociation]]:
        root_entity_id_field = kwargs.get(ROOT_ENTITY_ID_FIELD_KEY)
        related_entity_id_field = kwargs.get(RELATED_ENTITY_ID_FIELD_KEY)

        unifying_id = element.get(UNIFYING_ID_KEY)
        root_entity_id = element.get(root_entity_id_field)
        related_entity_id = element.get(related_entity_id_field)

        if unifying_id and root_entity_id and related_entity_id:
            yield unifying_id, (root_entity_id, related_entity_id)

    def to_runner_api_parameter(self, unused_context):
        pass


class _ExtractValuesFromEntityBase(beam.PTransform):
    """Shared functionality between any PTransforms doing entity extraction."""

    def __init__(
        self,
        project_id: str,
        entities_dataset: str,
        normalized_entities_dataset: str,
        entity_class: Union[Type[Entity], Type[NormalizedStateEntity]],
        unifying_id_field: str,
        unifying_id_field_filter_set: Optional[Set[int]],
        state_code: str,
    ):
        super().__init__()
        self._project_id = project_id

        self._unifying_id_field = unifying_id_field
        self._unifying_id_field_filter_set = unifying_id_field_filter_set

        if issubclass(entity_class, NormalizedStateEntity):
            self._base_entity_class = state_base_entity_class_for_entity_class(
                entity_class
            )
            self._dataset = normalized_entities_dataset
        elif issubclass(entity_class, Entity):
            self._base_entity_class = entity_class
            self._dataset = entities_dataset
        else:
            raise ValueError(f"Unexpected entity_class [{entity_class}]")

        self._entity_class_to_hydrate = entity_class
        self._base_schema_class: Type[
            StateBase
        ] = schema_utils.get_state_database_entity_with_name(
            self._base_entity_class.__name__
        )
        self._base_entity_table_name = self._base_schema_class.__tablename__
        self._entity_id_field = self._base_entity_class.get_class_id_name()
        self._state_code = state_code

    def _entity_has_unifying_id_field(self) -> bool:
        return hasattr(self._base_schema_class, self._unifying_id_field)

    def _get_entities_table_sql_query(
        self, columns_to_include: Optional[List[str]] = None
    ):
        if not self._entity_has_unifying_id_field():
            raise ValueError(
                f"Shouldn't be querying table for entity {self._base_entity_class} that doesn't have field "
                f"{self._unifying_id_field} - these values will never get grouped with results, so it's "
                f"a waste to query for them."
            )

        unifying_id_field_filter_set = (
            self._unifying_id_field_filter_set
            if self._entity_has_unifying_id_field()
            else None
        )

        entity_query = select_query(
            project_id=self._project_id,
            dataset=self._dataset,
            table=self._base_entity_table_name,
            state_code_filter=self._state_code,
            unifying_id_field=self._unifying_id_field,
            unifying_id_field_filter_set=unifying_id_field_filter_set,
            columns_to_include=columns_to_include,
        )

        return entity_query

    @abc.abstractmethod
    def expand(self, input_or_inputs):
        pass


class _ExtractAllEntitiesOfType(_ExtractValuesFromEntityBase):
    """Reads all entities of a given type from the corresponding table in BigQuery,
    then hydrates individual entity instances.
    """

    def __init__(
        self,
        project_id: str,
        entities_dataset: str,
        normalized_entities_dataset: str,
        entity_class: Union[Type[Entity], Type[NormalizedStateEntity]],
        unifying_id_field: str,
        unifying_id_field_filter_set: Optional[Set[int]],
        state_code: str,
    ):
        super().__init__(
            project_id,
            entities_dataset,
            normalized_entities_dataset,
            entity_class,
            unifying_id_field,
            unifying_id_field_filter_set,
            state_code,
        )

    def _get_entities_raw_pcollection(self, input_or_inputs: PBegin):
        if not self._entity_has_unifying_id_field():
            empty_output = (
                input_or_inputs
                | f"{self._base_entity_class} does not have {self._unifying_id_field}."
                >> beam.Create([])
            )
            return empty_output

        entity_query = self._get_entities_table_sql_query()

        # Read entities from BQ
        entities_raw = (
            input_or_inputs
            | f"Read {self._base_entity_table_name} from BigQuery"
            >> ReadFromBigQuery(query=entity_query)
        )

        return entities_raw

    def expand(self, input_or_inputs: PBegin):
        entities_raw = self._get_entities_raw_pcollection(input_or_inputs)

        hydrate_kwargs: Dict[str, Any] = {
            "entity_class": self._entity_class_to_hydrate,
            "unifying_id_field": self._unifying_id_field,
        }

        return (
            entities_raw
            | f"Hydrate flat fields of {self._base_entity_class.__name__} instances"
            >> beam.ParDo(_ShallowHydrateEntity(), **hydrate_kwargs)
        )


@with_input_types(beam.typehints.Any)
@with_output_types(beam.typehints.Dict[str, Any])
class ImportTable(beam.PTransform):
    """Reads in rows from the given dataset_id.table_id table in BigQuery. Returns each
    row as a dict."""

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        state_code_filter: str,
        unifying_id_field: Optional[str] = None,
        unifying_id_filter_set: Optional[Set[int]] = None,
    ):
        super().__init__()
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.state_code_filter = state_code_filter
        self.unifying_id_field = unifying_id_field
        self.unifying_id_filter_set = unifying_id_filter_set

    # pylint: disable=arguments-renamed
    def expand(self, pipeline: Pipeline):
        # Bring in the table from BigQuery
        table_query = select_query(
            project_id=self.project_id,
            dataset=self.dataset_id,
            table=self.table_id,
            state_code_filter=self.state_code_filter,
            unifying_id_field=self.unifying_id_field,
            unifying_id_field_filter_set=self.unifying_id_filter_set,
        )

        table_contents = (
            pipeline
            | f"Read {self.dataset_id}.{self.table_id} table from BigQuery"
            >> ReadFromBigQuery(query=table_query)
        )

        return table_contents


@with_input_types(beam.typehints.Any)
@with_output_types(beam.typehints.Tuple[Any, Dict[str, Any]])
class ImportTableAsKVTuples(beam.PTransform):
    """Reads in rows from the given dataset_id.table_id table in BigQuery. Converts the
    output rows into key-value tuples, where the keys are the values for the
    self.table_key column in the table."""

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        table_key: str,
        state_code_filter: str,
        unifying_id_field: Optional[str] = None,
        unifying_id_filter_set: Optional[Set[int]] = None,
    ):
        super().__init__()
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.table_key = table_key
        self.state_code_filter = state_code_filter
        self.unifying_id_field = unifying_id_field
        self.unifying_id_filter_set = unifying_id_filter_set

    # pylint: disable=arguments-renamed
    def expand(self, pipeline: Pipeline):
        # Read in the table from BigQuery
        table_contents = (
            pipeline
            | f"Read {self.dataset_id}.{self.table_id} from BigQuery"
            >> ImportTable(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                state_code_filter=self.state_code_filter,
                unifying_id_field=self.unifying_id_field,
                unifying_id_filter_set=self.unifying_id_filter_set,
            )
        )

        # Convert the table rows into key-value tuples with the value for the
        # self.table_key column as the key
        table_contents_as_kv = (
            table_contents
            | f"Convert {self.dataset_id}.{self.table_id} table to KV tuples"
            >> beam.ParDo(ConvertDictToKVTuple(), self.table_key)
        )

        return table_contents_as_kv


class _ExtractAssociationValues(_ExtractValuesFromEntityBase):
    """Extracts the values needed to associate two entity types."""

    def __init__(
        self,
        project_id: str,
        entities_dataset: str,
        normalized_entities_dataset: str,
        root_entity_class: Union[Type[Entity], Type[NormalizedStateEntity]],
        related_entity_class: Union[Type[Entity], Type[NormalizedStateEntity]],
        related_id_field: str,
        association_table: str,
        unifying_id_field: str,
        unifying_id_field_filter_set: Optional[Set[int]],
        state_code: str,
    ):

        self._root_entity_class = root_entity_class
        self._root_entity_base_class = state_base_entity_class_for_entity_class(
            root_entity_class
        )
        self._root_schema_class: Type[
            StateBase
        ] = schema_utils.get_state_database_entity_with_name(
            self._root_entity_base_class.__name__
        )

        self._related_entity_class = related_entity_class
        self._related_entity_base_class = state_base_entity_class_for_entity_class(
            related_entity_class
        )
        self._related_schema_class: Type[
            StateBase
        ] = schema_utils.get_state_database_entity_with_name(
            self._related_entity_base_class.__name__
        )
        self._association_table = association_table

        if self._association_table == self._related_schema_class.__tablename__:
            # If the provided association_table is the table of the related entity,
            # then we should set that entity as the core entity to be queried from
            self._entity_class_for_query = self._related_entity_class
        else:
            self._entity_class_for_query = self._root_entity_class

        self._root_id_field = self._root_entity_base_class.get_class_id_name()
        self._related_id_field = related_id_field

        super().__init__(
            project_id,
            entities_dataset,
            normalized_entities_dataset,
            self._entity_class_for_query,
            unifying_id_field,
            unifying_id_field_filter_set,
            state_code,
        )

    def _entity_has_all_fields_for_association(self) -> bool:
        return (
            hasattr(self._base_schema_class, self._unifying_id_field)
            and hasattr(self._base_schema_class, self._root_id_field)
            and hasattr(self._base_schema_class, self._related_id_field)
        )

    def _get_association_values_raw_pcollection(
        self, pipeline: PBegin
    ) -> PCollection[Tuple[int, EntityAssociation]]:
        """Returns the PCollection of association values from all relevant rows in the
        association table."""
        if not self._entity_has_unifying_id_field():
            raise ValueError(
                "Should not be querying for the association between two "
                "entities if one entity does not have the unifying "
                f"field. No {self._unifying_id_field} found on schema "
                f"class: {self._base_schema_class}."
            )

        if self._association_table == self._base_schema_class.__tablename__:
            if not self._entity_has_all_fields_for_association():
                raise ValueError(
                    "All three association fields must exist on the "
                    "entity if the entity's table is provided as the "
                    f"association table. association_table: {self._association_table}"
                )

            columns_to_include = [
                f"{self._unifying_id_field} as {UNIFYING_ID_KEY}",
                self._root_id_field,
                self._related_id_field,
            ]

            # Query the three association values directly from the entity table
            association_view_query = self._get_entities_table_sql_query(
                columns_to_include=columns_to_include
            )
        else:
            if self._root_id_field != self._entity_id_field:
                raise ValueError(
                    "If we're querying from an actual association table "
                    "then we expect the _root_id_field to be the same as "
                    "the _entity_id_field. Found: _root_id_field: "
                    f"{self._root_id_field}, _entity_id_field: "
                    f"{self._entity_id_field}."
                )

            # The join is doing a filter - we need to know which entities this instance
            # of the pipeline will end up hydrating to know which association table
            # rows we will need.
            association_view_query = (
                f"SELECT "
                f"{self._base_entity_class.get_entity_name()}.{self._unifying_id_field} as {UNIFYING_ID_KEY}, "
                f"{self._association_table}.{self._root_id_field}, "
                f"{self._association_table}.{self._related_id_field} "
                f"FROM `{self._project_id}.{self._dataset}.{self._association_table}`"
                f" {self._association_table} "
                f"JOIN ({self._get_entities_table_sql_query()}) {self._base_entity_class.get_entity_name()} "
                f"ON {self._base_entity_class.get_entity_name()}.{self._entity_id_field} = "
                f"{self._association_table}.{self._root_id_field}"
            )

        # Read association view from BQ
        association_values_raw = (
            pipeline
            | f"Read {self._association_table} from BigQuery"
            >> ReadFromBigQuery(query=association_view_query)
        )

        return association_values_raw

    def expand(
        self, input_or_inputs: PBegin
    ) -> PCollection[Tuple[UnifyingId, EntityAssociation]]:
        association_values_raw = self._get_association_values_raw_pcollection(
            input_or_inputs
        )

        id_values_kwargs = {
            ROOT_ENTITY_ID_FIELD_KEY: self._root_id_field,
            RELATED_ENTITY_ID_FIELD_KEY: self._related_id_field,
        }

        association_values = (
            association_values_raw | f"Get id fields from"
            f" {self._association_table} in values"
            >> beam.ParDo(_PackageAssociationIDValues(), **id_values_kwargs)
        )

        return association_values


@with_input_types(
    beam.typehints.Dict[str, Any],
    **{"entity_class": Type[Entity], "unifying_id_field": str},
)
@with_output_types(beam.typehints.Tuple[int, Entity])
class _ShallowHydrateEntity(beam.DoFn):
    """Hydrates an Entity."""

    def process(self, element: TableRow, *_args, **kwargs):
        """Builds an entity from key-value pairs.

        Args:
            element: A dictionary containing Entity information.
            **kwargs: This should be a dictionary with values for the
                    following keys:
                        - entity_class: Entity class to be built. Classes must also
                          be subclasses of BuildableAttr.
                        - unifying_id_field: Field in |element| corresponding
                            to an id that is needed to unify this root entity
                            with other related root entities.

        Yields:
            A tuple in the form of (int, Entity).
        """
        # Build the entity from the values in the element
        entity_class = kwargs["entity_class"]
        unifying_id_field = kwargs["unifying_id_field"]

        hydrated_entity = entity_class.build_from_dictionary(element)
        if not isinstance(hydrated_entity, Entity):
            raise ValueError(f"Found unexpected entity type [{type(hydrated_entity)}]")

        unifying_id = _get_value_from_table_row(element, unifying_id_field)

        if not unifying_id:
            raise ValueError(f"Invalid unifying_id_field: {unifying_id_field}")

        yield (unifying_id, hydrated_entity)

    def to_runner_api_parameter(self, unused_context):
        pass


@with_input_types(
    beam.typehints.Tuple[
        UnifyingId,
        Dict[Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]],
    ],
    beam.typehints.List[TableName],
)
@with_output_types(
    beam.typehints.Tuple[
        UnifyingId,
        Dict[Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]],
    ]
)
class _AttachStateBasedReferenceDataToEntities(beam.DoFn):
    """Attaches state-wide reference tables (no unifying ID in the table) as side inputs."""

    # pylint: disable=arguments-differ
    def process(
        self,
        element: Tuple[
            UnifyingId,
            Dict[
                Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]
            ],
        ],
        state_based_reference_tables: List[TableName],
        *_args,
        **kwargs,
    ) -> Iterable[
        Tuple[
            UnifyingId,
            Dict[
                Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]
            ],
        ]
    ]:
        person_id, entities_and_reference_tables = element
        state_based_reference_data: Dict[TableName, List[TableRow]] = {}
        for state_based_reference_table in state_based_reference_tables:
            state_based_reference_data[state_based_reference_table] = kwargs[
                state_based_reference_table
            ]
        final_entities_and_reference_tables = {
            **entities_and_reference_tables,
            **state_based_reference_data,
        }
        yield person_id, final_entities_and_reference_tables

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


@with_input_types(
    beam.typehints.Tuple[
        UnifyingId,
        Dict[Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]],
    ],
    beam.typehints.Optional[str],
)
@with_output_types(
    beam.typehints.Tuple[
        UnifyingId,
        Dict[Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]],
    ],
)
class ConvertEntitiesToStateSpecificTypes(beam.DoFn):
    """Converts entities into state-specific subclasses of those entities,
    for use in state-specific calculate flows."""

    # pylint: disable=arguments-differ
    def process(
        self,
        element: Tuple[
            UnifyingId,
            Dict[
                Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]
            ],
        ],
        state_code: str,
        *_args,
        **_kwargs,
    ) -> Iterable[
        Tuple[
            UnifyingId,
            Dict[
                Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]
            ],
        ]
    ]:
        """For the entities of the given person, convert to a state-specific subclass,
        if necessary.

        Args:
            element: A tuple containing person_id and a dictionary with all of the
                person's entities and sentence statuses (if applicable)
            state_code: The state_code corresponding to the entities

        Yields:
            A replica of the provided element, with some entities replaced by updated
            entities, where applicable.
        """
        person_id, entities_and_reference_tables = element

        if state_code == StateCode.US_MO.value:
            self.update_US_MO_sentences(entities_and_reference_tables)
        yield person_id, entities_and_reference_tables

    def update_US_MO_sentences(
        self,
        entities_and_reference_tables: Dict[
            Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]
        ],
    ) -> None:
        """Updates US_MO StateIncarcerationSentence and StateSupervisionSentence
        entities to state-specific versions of the classes that store additional
        information required to perform calculations in this state."""
        incarceration_sentences: List[Entity] = []
        supervision_sentences: List[Entity] = []

        if (
            state_entities.StateIncarcerationSentence.__name__
            in entities_and_reference_tables
        ):
            incarceration_sentences = cast(
                List[Entity],
                entities_and_reference_tables[
                    state_entities.StateIncarcerationSentence.__name__
                ],
            )

        if (
            state_entities.StateSupervisionSentence.__name__
            in entities_and_reference_tables
        ):
            supervision_sentences = cast(
                List[Entity],
                entities_and_reference_tables[
                    state_entities.StateSupervisionSentence.__name__
                ],
            )

        if not supervision_sentences and not incarceration_sentences:
            return

        if US_MO_SENTENCE_STATUSES_VIEW_NAME not in entities_and_reference_tables:
            raise ValueError(
                f"Must hydrate [{US_MO_SENTENCE_STATUSES_VIEW_NAME}] to "
                f"use sentences in a US_MO pipeline."
            )
        all_sentence_statuses: Union[
            List[Entity], List[TableRow]
        ] = entities_and_reference_tables.pop(US_MO_SENTENCE_STATUSES_VIEW_NAME)

        us_mo_sentence_statuses_by_sentence: Dict[
            str, List[Dict[str, str]]
        ] = defaultdict(list)

        if all_sentence_statuses:
            # Build a dictionary that maps each sentence_external_id to a list of
            # dictionaries containing status updates for this sentence
            for status_dict in all_sentence_statuses:
                if not isinstance(status_dict, Dict):
                    raise ValueError(
                        "Expected sentence status element to be of type "
                        f"Dict. Found {type(status_dict)}."
                    )

                sentence_external_id = status_dict.get("sentence_external_id")

                if sentence_external_id:
                    us_mo_sentence_statuses_by_sentence[sentence_external_id].append(
                        status_dict
                    )

        updated_incarceration_sentences: List[UsMoIncarcerationSentence] = []

        for incarceration_sentence in incarceration_sentences:
            if not isinstance(
                incarceration_sentence, state_entities.StateIncarcerationSentence
            ):
                raise ValueError(
                    "Expected entity to be of type "
                    f"StateIncarcerationSentence. Found {type(incarceration_sentence)}."
                )

            if incarceration_sentence.state_code != StateCode.US_MO.value:
                raise ValueError(
                    f"Found sentence that isn't of US_MO: {incarceration_sentence}"
                )

            sentence_statuses = []
            if (
                incarceration_sentence.external_id
                in us_mo_sentence_statuses_by_sentence
            ):
                sentence_statuses = us_mo_sentence_statuses_by_sentence[
                    incarceration_sentence.external_id
                ]

            state_specific_incarceration_sentence = (
                UsMoIncarcerationSentence.from_incarceration_sentence(
                    incarceration_sentence, sentence_statuses
                )
            )

            updated_incarceration_sentences.append(
                state_specific_incarceration_sentence
            )

        entities_and_reference_tables[
            state_entities.StateIncarcerationSentence.__name__
        ] = cast(List[Entity], updated_incarceration_sentences)

        updated_supervision_sentences: List[UsMoSupervisionSentence] = []

        for supervision_sentence in supervision_sentences:
            if not isinstance(
                supervision_sentence, state_entities.StateSupervisionSentence
            ):
                raise ValueError(
                    "Expected entity to be of type "
                    f"StateSupervisionSentence. Found {type(supervision_sentence)}."
                )
            if supervision_sentence.state_code != StateCode.US_MO.value:
                raise ValueError(
                    f"Found sentence that isn't of US_MO: {supervision_sentence}"
                )
            sentence_statuses = []
            if supervision_sentence.external_id in us_mo_sentence_statuses_by_sentence:
                sentence_statuses = us_mo_sentence_statuses_by_sentence[
                    supervision_sentence.external_id
                ]

            state_specific_supervision_sentence = (
                UsMoSupervisionSentence.from_supervision_sentence(
                    supervision_sentence, sentence_statuses
                )
            )

            updated_supervision_sentences.append(state_specific_supervision_sentence)

        entities_and_reference_tables[
            state_entities.StateSupervisionSentence.__name__
        ] = cast(List[Entity], updated_supervision_sentences)

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


def _get_value_from_table_row(table_row: TableRow, field: str) -> Any:
    value = table_row.get(field)

    return value
