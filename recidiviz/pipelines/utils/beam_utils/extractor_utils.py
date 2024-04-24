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
    Generator,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.pvalue import PBegin
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.big_query.big_query_query_provider import (
    BigQueryQueryProvider,
    StateFilteredQueryProvider,
)
from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.entity import entity_utils
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import SchemaEdgeDirectionChecker
from recidiviz.persistence.entity.normalized_entities_utils import (
    state_base_entity_class_for_entity_class,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import ReadFromBigQuery
from recidiviz.pipelines.utils.beam_utils.load_query_results_keyed_by_column import (
    LoadQueryResultsKeyedByColumn,
)
from recidiviz.pipelines.utils.execution_utils import (
    EntityAssociation,
    EntityClassName,
    EntityRelationshipKey,
    RootEntityId,
    TableName,
    TableRow,
    select_query,
)
from recidiviz.pipelines.utils.reference_query_providers import (
    RootEntityIdFilteredQueryProvider,
)

ROOT_ENTITY_ID_KEY = "root_entity_id"

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


class ExtractRootEntityDataForPipeline(beam.PTransform):
    """
    Reads and hydrates all root entity-level data from BigQuery, including hydrating
    all existing connections between entities.
    """

    def __init__(
        self,
        *,
        state_code: StateCode,
        project_id: str,
        entities_dataset: str,
        required_entity_classes: Optional[
            List[Union[Type[Entity], Type[NormalizedStateEntity]]]
        ],
        reference_data_queries_by_name: Dict[str, StateFilteredQueryProvider],
        root_entity_cls: (
            Type[state_entities.StatePerson] | Type[state_entities.StateStaff]
        ),
        root_entity_id_filter_set: Optional[Set[RootEntityId]] = None,
    ):
        """Initializes the PTransform with the required arguments.

        Arguments:
            state_code: The state code to filter all results by
            project_id: The project_id of the BigQuery project to query from.
            entities_dataset: The name of the BigQuery dataset_id to read the required
                entities from.
            required_entity_classes: The list of required entity classes for the
                pipeline. Must not contain any duplicates of any entities.
            reference_data_queries_by_name: Queries whose results should be returned
                alongside the hydrated entity data.
            root_entity_cls: The Entity type whose id should be used to connect the
                required entities to each other. All entities listed in
                |required_entity_classes| must have this entity's id field, but this
                class does not necessarily need to be included in the list of
                |required_entity_classes|. This value is usually StatePerson.
            root_entity_id_filter_set: When non-empty, we will only build entity
                objects that can be connected to root entities with one of these
                root entity ids.
        """
        super().__init__()

        self._state_code = state_code
        self._project_id = project_id

        if not entities_dataset:
            raise ValueError("No valid data source passed to the pipeline.")
        self._entities_dataset = entities_dataset

        filtered_reference_data_queries_by_name: Mapping[str, BigQueryQueryProvider]
        if root_entity_id_filter_set:
            filtered_reference_data_queries_by_name = {
                query_name: RootEntityIdFilteredQueryProvider(
                    original_query=query,
                    root_entity_cls=root_entity_cls,
                    root_entity_id_filter_set=root_entity_id_filter_set,
                )
                for query_name, query in reference_data_queries_by_name.items()
            }
        else:
            filtered_reference_data_queries_by_name = reference_data_queries_by_name

        self._reference_data_queries_by_name = filtered_reference_data_queries_by_name

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

        if not root_entity_cls:
            raise ValueError("No valid root_entity_cls passed to the pipeline.")
        self._root_entity_cls = root_entity_cls
        self._root_entity_id_field = root_entity_cls.get_class_id_name()
        self._root_entity_id_field_filter_set = root_entity_id_filter_set

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
            EntityClassName, PCollection[Tuple[RootEntityId, EntityAssociation]]
        ],
        relationships_to_hydrate: List[EntityRelationshipDetails],
    ) -> None:
        """Adds association values for the provided |relationships_to_hydrate| to the
        given |hydrated_association_info| dict, if necessary.
        """
        for relationship_property in relationships_to_hydrate:
            related_entity_class = relationship_property.property_entity_class
            related_entity_class_name = related_entity_class.__name__

            if self._root_entity_cls in (related_entity_class, root_entity_class):
                # All entities will be grouped with the root entity class that they are
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
                    entity_class=root_entity_class_being_hydrated,
                    related_entity_class=related_entity_class_being_hydrated,
                    root_entity_id_field=self._root_entity_id_field,
                    association_table=relationship_property.association_table,
                    related_id_field=relationship_property.association_table_entity_id_field,
                    root_entity_id_filter_set=self._root_entity_id_field_filter_set,
                    state_code=self._state_code.value,
                )
            )

            hydrated_association_info[relationship_key] = association_values

    def get_shallow_hydrated_entity_pcollection(
        self,
        pipeline: PBegin,
        entity_class: Union[Type[Entity], Type[NormalizedStateEntity]],
    ) -> PCollection[Tuple[RootEntityId, Entity]]:
        """Returns the hydrated entities of type |entity_class| as a PCollection,
        where each element is a tuple in the format: (root_entity_id, entity).
        """
        return (
            pipeline
            | f"Extract {entity_class.__name__} instances"
            >> _ExtractAllEntitiesOfType(
                project_id=self._project_id,
                entities_dataset=self._entities_dataset,
                entity_class=entity_class,
                root_entity_id_field=self._root_entity_id_field,
                root_entity_id_filter_set=self._root_entity_id_field_filter_set,
                state_code=self._state_code.value,
            )
        )

    def get_associations_info(
        self,
        pipeline: PBegin,
        relationships_to_hydrate: Dict[str, List[EntityRelationshipDetails]],
    ) -> Dict[EntityClassName, PCollection[Tuple[RootEntityId, EntityAssociation]]]:
        """Gets all information required to associate the required entities to each
        other."""
        hydrated_association_info: Dict[
            EntityClassName, PCollection[Tuple[RootEntityId, EntityAssociation]]
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
            RootEntityId,
            Dict[
                Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]
            ],
        ]
    ]:
        """Does the work of building all entities required for a pipeline
        and hydrating all existing relationships between the entities."""
        shallow_hydrated_entities: Dict[
            EntityClassName, PCollection[Tuple[RootEntityId, Entity]]
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
            EntityClassName, PCollection[Tuple[RootEntityId, EntityAssociation]]
        ] = self.get_associations_info(
            pipeline=input_or_inputs,
            relationships_to_hydrate=relationships_to_hydrate,
        )

        reference_data: Dict[TableName, PCollection[Tuple[RootEntityId, TableRow]]] = {}

        for query_name, query_provider in self._reference_data_queries_by_name.items():
            reference_data[
                query_name
            ] = input_or_inputs | f"Load {query_name}" >> LoadQueryResultsKeyedByColumn(
                key_column_name=self._root_entity_id_field,
                query_name=query_name,
                query_provider=query_provider,
            )

        entities_and_associations: Dict[
            Union[EntityClassName, EntityRelationshipKey, TableName],
            PCollection[
                Tuple[RootEntityId, Union[EntityAssociation, Entity, TableRow]]
            ],
        ] = {**shallow_hydrated_entities, **hydrated_association_info, **reference_data}

        # Group all entities and association tuples by the root_entity_id
        entities_and_association_info_by_root_entity_id: PCollection[
            Tuple[
                RootEntityId,
                Dict[
                    Union[EntityClassName, EntityRelationshipKey, TableName],
                    Union[List[Entity], List[EntityAssociation], List[TableRow]],
                ],
            ]
        ] = (
            entities_and_associations
            | f"Group entities, associations, and reference tables by"
            f" {self._root_entity_id_field}" >> beam.CoGroupByKey()
        )

        fully_connected_hydrated_entities = (
            entities_and_association_info_by_root_entity_id
            | "Connect all entity relationships"
            >> beam.ParDo(
                _ConnectHydratedRelatedEntities(
                    root_entity_class=self._root_entity_cls,
                    relationships_to_hydrate=relationships_to_hydrate,
                ),
            )
        )

        return fully_connected_hydrated_entities


@with_input_types(
    beam.typehints.Tuple[
        RootEntityId,
        Dict[
            Union[EntityClassName, EntityRelationshipKey, TableName],
            Union[Iterable[Entity], Iterable[EntityAssociation], Iterable[TableRow]],
        ],
    ],
)
@with_output_types(
    beam.typehints.Tuple[
        RootEntityId,
        Dict[Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]],
    ],
)
class _ConnectHydratedRelatedEntities(beam.DoFn):
    """Connects all entities of the |root_entity_class| type to all
    hydrated related entities for all relationships listed in |relationships|.
    """

    # Silence `Method 'process_batch' is abstract in class 'DoFn' but is not overridden (abstract-method)`
    # pylint: disable=W0223

    def __init__(
        self,
        root_entity_class: Type[Entity],
        relationships_to_hydrate: Dict[
            EntityClassName, List[EntityRelationshipDetails]
        ],
    ) -> None:
        super().__init__()
        self._root_entity_class = root_entity_class
        self._relationships_to_hydrate = relationships_to_hydrate

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
        entity_class_name: EntityClassName,
        relationships_to_hydrate: List[EntityRelationshipDetails],
        entities: Dict[EntityClassName, List[Entity]],
        associations: Dict[EntityRelationshipKey, List[EntityAssociation]],
    ) -> List[Entity]:
        """Returns the list of entities with all relationships to related entities
        fully hydrated."""
        root_entity_class_name = self._root_entity_class.__name__
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

            if root_entity_class_name in (entity_class_name, related_entity_class_name):
                # If either this entity or the related entity is the root entity class,
                # then we know there are direct relationships between this entity and
                # all related entities of this type.
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
            Union[Iterable[Entity], Iterable[EntityAssociation], Iterable[TableRow]],
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
            RootEntityId,
            Dict[
                Union[EntityClassName, EntityRelationshipKey, TableName],
                Union[
                    Iterable[Entity], Iterable[EntityAssociation], Iterable[TableRow]
                ],
            ],
        ],
    ) -> Iterable[
        Tuple[
            RootEntityId,
            Dict[
                Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]
            ],
        ]
    ]:
        """Runs the process for getting fully connected hydrated entities."""
        fully_connected_hydrated_entities: Dict[EntityClassName, List[Entity]] = {}

        root_entity_id, element_data = element
        if root_entity_id is None:
            raise ValueError("Found unexpected null root_entity_id.")

        entities, associations, reference_table_data = self._split_element_data(
            element_data=element_data,
            relationships_to_hydrate=self._relationships_to_hydrate,
        )

        for (
            root_entity_class_name,
            relationships,
        ) in self._relationships_to_hydrate.items():
            fully_connected_hydrated_entities[
                root_entity_class_name
            ] = self._get_fully_hydrated_entities_of_type(
                entity_class_name=root_entity_class_name,
                relationships_to_hydrate=relationships,
                entities=entities,
                associations=associations,
            )

        all_pipeline_data: Dict[
            Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]
        ] = {**fully_connected_hydrated_entities, **reference_table_data}

        yield root_entity_id, all_pipeline_data


@with_input_types(beam.typehints.Dict[Any, Any])
@with_output_types(beam.typehints.Tuple[RootEntityId, EntityAssociation])
class _PackageAssociationIDValues(beam.DoFn):
    """Forms a tuple from the given element with the following format:

    (root_entity_id, (entity_id, related_entity_id))
    """

    # Silence `Method 'process_batch' is abstract in class 'DoFn' but is not overridden (abstract-method)`
    # pylint: disable=W0223

    def __init__(self, entity_id_field: str, related_entity_id_field: str) -> None:
        super().__init__()
        self._entity_id_field = entity_id_field
        self._related_entity_id_field = related_entity_id_field

    # pylint: disable=arguments-differ
    def process(
        self, element: Dict[Any, Any]
    ) -> Iterable[Tuple[RootEntityId, EntityAssociation]]:
        root_entity_id = element.get(ROOT_ENTITY_ID_KEY)
        entity_id = element.get(self._entity_id_field)
        related_entity_id = element.get(self._related_entity_id_field)

        if root_entity_id and entity_id and related_entity_id:
            yield root_entity_id, (entity_id, related_entity_id)


class _ExtractValuesFromEntityBase(beam.PTransform):
    """Shared functionality between any PTransforms doing entity extraction."""

    def __init__(
        self,
        project_id: str,
        entities_dataset: str,
        entity_class: Union[Type[Entity], Type[NormalizedStateEntity]],
        root_entity_id_field: str,
        root_entity_id_filter_set: Optional[Set[RootEntityId]],
        state_code: str,
    ):
        super().__init__()
        self._project_id = project_id

        self._root_entity_id_field = root_entity_id_field
        self._root_entity_id_filter_set = root_entity_id_filter_set
        self._dataset = entities_dataset

        if issubclass(entity_class, NormalizedStateEntity):
            self._base_entity_to_hydrate_class = (
                state_base_entity_class_for_entity_class(entity_class)
            )
        elif issubclass(entity_class, Entity):
            self._base_entity_to_hydrate_class = entity_class
        else:
            raise ValueError(f"Unexpected entity_class [{entity_class}]")

        self._entity_to_hydrate_class = entity_class
        self._base_entity_to_hydrate_schema_class: Type[
            StateBase
        ] = schema_utils.get_state_database_entity_with_name(
            self._base_entity_to_hydrate_class.__name__
        )
        self._base_entity_to_hydrate_table_name = (
            self._base_entity_to_hydrate_schema_class.__tablename__
        )
        self._entity_to_hydrate_id_field = (
            self._base_entity_to_hydrate_class.get_class_id_name()
        )
        self._state_code = state_code

    def _entity_has_root_entity_id_field(self) -> bool:
        return hasattr(
            self._base_entity_to_hydrate_schema_class, self._root_entity_id_field
        )

    def _get_entities_table_sql_query(
        self, columns_to_include: Optional[List[str]] = None
    ) -> str:
        if not self._entity_has_root_entity_id_field():
            raise ValueError(
                f"Shouldn't be querying table for entity {self._base_entity_to_hydrate_class} that doesn't have field "
                f"{self._root_entity_id_field} - these values will never get grouped with results, so it's "
                f"a waste to query for them."
            )

        root_entity_id_filter_set = (
            self._root_entity_id_filter_set
            if self._entity_has_root_entity_id_field()
            else None
        )

        entity_query = select_query(
            project_id=self._project_id,
            dataset=self._dataset,
            table=self._base_entity_to_hydrate_table_name,
            state_code_filter=self._state_code,
            root_entity_id_field=self._root_entity_id_field,
            root_entity_id_filter_set=root_entity_id_filter_set,
            columns_to_include=columns_to_include,
        )

        return entity_query

    @abc.abstractmethod
    def expand(self, input_or_inputs: PBegin) -> PCollection[Tuple[RootEntityId, Any]]:
        pass


class _ExtractAllEntitiesOfType(_ExtractValuesFromEntityBase):
    """Reads all entities of a given type from the corresponding table in BigQuery,
    then hydrates individual entity instances.
    """

    def __init__(
        self,
        project_id: str,
        entities_dataset: str,
        entity_class: Union[Type[Entity], Type[NormalizedStateEntity]],
        root_entity_id_field: str,
        root_entity_id_filter_set: Optional[Set[RootEntityId]],
        state_code: str,
    ) -> None:
        super().__init__(
            project_id,
            entities_dataset,
            entity_class,
            root_entity_id_field,
            root_entity_id_filter_set,
            state_code,
        )

    def _get_entities_raw_pcollection(
        self, input_or_inputs: PBegin
    ) -> PCollection[Dict[str, Any]]:
        if not self._entity_has_root_entity_id_field():
            empty_output = (
                input_or_inputs
                | f"{self._base_entity_to_hydrate_class} does not have {self._root_entity_id_field}."
                >> beam.Create([])
            )
            return empty_output

        entity_query = self._get_entities_table_sql_query()

        # Read entities from BQ
        entities_raw = (
            input_or_inputs
            | f"Read {self._base_entity_to_hydrate_table_name} from BigQuery"
            >> ReadFromBigQuery(query=entity_query)
        )

        return entities_raw

    def expand(
        self, input_or_inputs: PBegin
    ) -> PCollection[Tuple[RootEntityId, Entity]]:
        entities_raw = self._get_entities_raw_pcollection(input_or_inputs)
        return (
            entities_raw
            | f"Hydrate flat fields of {self._base_entity_to_hydrate_class.__name__} instances"
            >> beam.ParDo(
                _ShallowHydrateEntity(
                    entity_class=self._entity_to_hydrate_class,
                    root_entity_id_field=self._root_entity_id_field,
                ),
            )
        )


class _ExtractAssociationValues(_ExtractValuesFromEntityBase):
    """Extracts the values needed to associate two entity types."""

    def __init__(
        self,
        project_id: str,
        entities_dataset: str,
        entity_class: Union[Type[Entity], Type[NormalizedStateEntity]],
        related_entity_class: Union[Type[Entity], Type[NormalizedStateEntity]],
        related_id_field: str,
        association_table: str,
        root_entity_id_field: str,
        root_entity_id_filter_set: Optional[Set[RootEntityId]],
        state_code: str,
    ):
        self._entity_base_class = state_base_entity_class_for_entity_class(entity_class)
        self._entity_schema_class: Type[
            StateBase
        ] = schema_utils.get_state_database_entity_with_name(
            self._entity_base_class.__name__
        )

        self._related_entity_class = related_entity_class
        self._related_entity_base_class = state_base_entity_class_for_entity_class(
            related_entity_class
        )
        self._related_entity_schema_class: Type[
            StateBase
        ] = schema_utils.get_state_database_entity_with_name(
            self._related_entity_base_class.__name__
        )
        self._association_table = association_table

        if self._association_table == self._related_entity_schema_class.__tablename__:
            # If the provided association_table is the table of the related entity,
            # then we should set that entity as the core entity to be queried from
            self._entity_class_for_query = self._related_entity_class
        else:
            self._entity_class_for_query = entity_class

        self._entity_id_field = self._entity_base_class.get_class_id_name()
        self._related_entity_id_field = related_id_field

        super().__init__(
            project_id,
            entities_dataset,
            self._entity_class_for_query,
            root_entity_id_field,
            root_entity_id_filter_set,
            state_code,
        )

    def _entity_has_all_fields_for_association(self) -> bool:
        return (
            hasattr(
                self._base_entity_to_hydrate_schema_class, self._root_entity_id_field
            )
            and hasattr(
                self._base_entity_to_hydrate_schema_class, self._entity_id_field
            )
            and hasattr(
                self._base_entity_to_hydrate_schema_class, self._related_entity_id_field
            )
        )

    def _get_association_values_raw_pcollection(
        self, pipeline: PBegin
    ) -> PCollection[Tuple[int, EntityAssociation]]:
        """Returns the PCollection of association values from all relevant rows in the
        association table."""
        if not self._entity_has_root_entity_id_field():
            raise ValueError(
                "Should not be querying for the association between two "
                "entities if one entity does not have the root entity id "
                f"field. No {self._root_entity_id_field} found on schema "
                f"class: {self._base_entity_to_hydrate_schema_class}."
            )

        if (
            self._association_table
            == self._base_entity_to_hydrate_schema_class.__tablename__
        ):
            if not self._entity_has_all_fields_for_association():
                raise ValueError(
                    "All three association fields must exist on the "
                    "entity if the entity's table is provided as the "
                    f"association table. association_table: {self._association_table}"
                )

            columns_to_include = [
                f"{self._root_entity_id_field} as {ROOT_ENTITY_ID_KEY}",
                self._entity_id_field,
                self._related_entity_id_field,
            ]

            # Query the three association values directly from the entity table
            association_view_query = self._get_entities_table_sql_query(
                columns_to_include=columns_to_include
            )
        else:
            if self._entity_id_field != self._entity_to_hydrate_id_field:
                raise ValueError(
                    "If we're querying from an actual association table "
                    "then we expect the _root_id_field to be the same as "
                    "the _entity_id_field. Found: _root_id_field: "
                    f"{self._entity_id_field}, _entity_id_field: "
                    f"{self._entity_to_hydrate_id_field}."
                )

            # The join is doing a filter - we need to know which entities this instance
            # of the pipeline will end up hydrating to know which association table
            # rows we will need.
            association_view_query = (
                f"SELECT "
                f"{self._base_entity_to_hydrate_class.get_entity_name()}.{self._root_entity_id_field} as {ROOT_ENTITY_ID_KEY}, "
                f"{self._association_table}.{self._entity_id_field}, "
                f"{self._association_table}.{self._related_entity_id_field} "
                f"FROM `{self._project_id}.{self._dataset}.{self._association_table}`"
                f" {self._association_table} "
                f"JOIN ({self._get_entities_table_sql_query()}) {self._base_entity_to_hydrate_class.get_entity_name()} "
                f"ON {self._base_entity_to_hydrate_class.get_entity_name()}.{self._entity_to_hydrate_id_field} = "
                f"{self._association_table}.{self._entity_id_field}"
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
    ) -> PCollection[Tuple[RootEntityId, EntityAssociation]]:
        association_values_raw = self._get_association_values_raw_pcollection(
            input_or_inputs
        )

        association_values = (
            association_values_raw | f"Get id fields from"
            f" {self._association_table} in values"
            >> beam.ParDo(
                _PackageAssociationIDValues(
                    entity_id_field=self._entity_id_field,
                    related_entity_id_field=self._related_entity_id_field,
                ),
            )
        )

        return association_values


@with_input_types(
    beam.typehints.Dict[str, Any],
)
@with_output_types(beam.typehints.Tuple[int, Entity])
class _ShallowHydrateEntity(beam.DoFn):
    """Hydrates an Entity."""

    # Silence `Method 'process_batch' is abstract in class 'DoFn' but is not overridden (abstract-method)`
    # pylint: disable=W0223

    def __init__(
        self,
        root_entity_id_field: str,
        entity_class: Type[Entity | NormalizedStateEntity],
    ) -> None:
        """
        Args:
            entity_class: The class of the Entity to hydrate
            root_entity_id_field: The name of the root entity id field on |entity_class|
        """
        super().__init__()
        self._root_entity_id_field = root_entity_id_field
        self._entity_class = entity_class

    # pylint: disable=arguments-differ
    def process(self, element: TableRow) -> Generator[Tuple[int, Entity], None, None]:
        """Builds an entity from key-value pairs.

        Args:
            element: A dictionary containing Entity information.
        Yields:
            A tuple in the form of (int, Entity).
        """
        # Build the entity from the values in the element
        if not issubclass(self._entity_class, BuildableAttr):
            raise ValueError(
                f"Expected entity class [{self._entity_class}] to be a subclass of "
                f"BuildableAttr."
            )
        hydrated_entity = self._entity_class.build_from_dictionary(element)
        if not isinstance(hydrated_entity, Entity):
            raise ValueError(f"Found unexpected entity type [{type(hydrated_entity)}]")

        root_entity_id = _get_value_from_table_row(element, self._root_entity_id_field)

        if not root_entity_id:
            raise ValueError(
                f"Invalid root_entity_id_field: {self._root_entity_id_field}"
            )

        yield (root_entity_id, hydrated_entity)


def _get_value_from_table_row(table_row: TableRow, field: str) -> Any:
    value = table_row.get(field)

    return value
