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
import logging
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
)

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.pvalue import PBegin
from apache_beam.typehints import with_input_types, with_output_types
from more_itertools import one
from sqlalchemy.orm.relationships import RelationshipProperty

from recidiviz.calculator.pipeline.utils.beam_utils import ReadFromBigQuery
from recidiviz.calculator.pipeline.utils.execution_utils import select_query
from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.entity import entity_utils
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import (
    SchemaEdgeDirectionChecker,
    is_property_forward_ref,
    is_property_list,
)
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

# The name of an entity, e.g. StatePerson.
EntityClassName = str

# The names of of two entities that are related to one another,
# eg. StateSupervisionSentence.StateCharge
EntityRelationshipKey = str

# The unifying id that can be used to group related objects together (e.g. person_id)
UnifyingId = int

# Primary keys of two entities that share a relationship. The first int is the parent
# object primary key and the second int is the child object primary key.
EntityAssociation = Tuple[int, int]


class ExtractEntitiesForPipeline(beam.PTransform):
    """Builds all of the required entities for a pipeline. Hydrates all existing
    connections between required entities.
    """

    def __init__(
        self,
        state_code: str,
        dataset: str,
        required_entity_classes: List[Type[Entity]],
        unifying_class: Type[Entity],
        unifying_id_field_filter_set: Optional[Set[UnifyingId]] = None,
    ):
        """Initializes the PTransform with the required arguments.

        Arguments:
            state_code: The state code to filter all results by
            dataset: The name of the dataset to read from BigQuery.
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

        if not dataset:
            raise ValueError("No valid data source passed to the pipeline.")
        self._dataset = dataset

        if not required_entity_classes:
            raise ValueError(
                f"{self.__class__.__name__}: Expecting required_entity_classes to "
                "be not None."
            )

        if len(set(required_entity_classes)) != len(required_entity_classes):
            raise ValueError(
                "List of required entities should only contain each "
                "required entity class once. Found duplicates: "
                f"{required_entity_classes}."
            )

        self._required_entities = required_entity_classes
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

        for root_entity_class in self._required_entities:
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

                if property_entity_class in self._required_entities:
                    is_property_forward_edge = self._direction_checker.is_higher_ranked(
                        root_schema_class, property_schema_class
                    )

                    if is_property_forward_edge:
                        # Many-to-many relationship
                        if property_object.secondary is not None:
                            association_table = property_object.secondary.name
                            association_table_entity_id_field = (
                                property_entity_class.get_class_id_name()
                            )

                        # 1-to-many relationship
                        elif property_object.uselist:
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

            if (
                related_entity_class == self._unifying_class
                or root_entity_class == self._unifying_class
            ):
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

            # Get the association values for this relationship
            association_values = (
                pipeline | f"Extract association values for "
                f"{root_entity_class.__name__} to "
                f"{related_entity_class.__name__} relationship."
                >> _ExtractAssociationValues(
                    dataset=self._dataset,
                    root_entity_class=root_entity_class,
                    related_entity_class=related_entity_class,
                    unifying_id_field=self._unifying_id_field,
                    association_table=relationship_property.association_table,
                    related_id_field=relationship_property.association_table_entity_id_field,
                    unifying_id_field_filter_set=self._unifying_id_field_filter_set,
                    state_code=self._state_code,
                )
            )

            hydrated_association_info[relationship_key] = association_values

    def get_shallow_hydrated_entity_pcollection(
        self, pipeline: PBegin, entity_class: Type[Entity]
    ) -> PCollection[Tuple[UnifyingId, Entity]]:
        """Returns the hydrated entities of type |entity_class| as a PCollection,
        where each element is a tuple in the format: (unifying_id, entity).
        """
        return (
            pipeline | f"Extract {entity_class.__name__} "
            f"instances"
            >> _ExtractEntity(
                dataset=self._dataset,
                entity_class=entity_class,
                unifying_id_field=self._unifying_id_field,
                parent_id_field=None,
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
        for root_entity_class in self._required_entities:
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
    ) -> PCollection[Tuple[UnifyingId, Dict[EntityClassName, Iterable[Entity]]]]:
        """Does the work of building all entities required for a pipeline
        and hydrating all existing relationships between the entities."""
        shallow_hydrated_entities: Dict[
            EntityClassName, PCollection[Tuple[UnifyingId, Entity]]
        ] = {}

        for entity_class in self._required_entities:
            shallow_hydrated_entities[
                entity_class.__name__
            ] = self.get_shallow_hydrated_entity_pcollection(
                pipeline=input_or_inputs, entity_class=entity_class
            )

        relationships_to_hydrate = self._get_relationships_to_hydrate()

        hydrated_association_info: Dict[
            EntityClassName, PCollection[Tuple[UnifyingId, EntityAssociation]]
        ] = self.get_associations_info(
            pipeline=input_or_inputs,
            relationships_to_hydrate=relationships_to_hydrate,
        )

        entities_and_associations: Dict[
            EntityClassName,
            PCollection[Tuple[UnifyingId, Union[EntityAssociation, Entity]]],
        ] = {
            **shallow_hydrated_entities,
            **hydrated_association_info,
        }

        # Group all entities and association tuples by the unifying_id
        entities_and_association_info_by_unifying_id: PCollection[
            Tuple[
                UnifyingId,
                Dict[
                    Union[EntityClassName, EntityRelationshipKey],
                    Union[List[Entity], List[EntityAssociation]],
                ],
            ]
        ] = (
            entities_and_associations
            | f"Group entities and associations by {self._unifying_id_field}"
            >> beam.CoGroupByKey()
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

        return fully_connected_hydrated_entities


@with_input_types(
    beam.typehints.Tuple[
        UnifyingId,
        Dict[
            Union[EntityClassName, EntityRelationshipKey],
            Union[List[Entity], List[EntityAssociation]],
        ],
    ],
    beam.typehints.Optional[Type[Entity]],
    beam.typehints.Dict[str, List[EntityRelationshipDetails]],
)
@with_output_types(
    beam.typehints.Tuple[UnifyingId, Dict[EntityClassName, Iterable[Entity]]],
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

    # pylint: disable=arguments-differ
    def process(
        self,
        element: Tuple[
            UnifyingId,
            Dict[
                Union[EntityClassName, EntityRelationshipKey],
                Union[List[Entity], List[EntityAssociation]],
            ],
        ],
        unifying_class: Type[Entity],
        relationships_to_hydrate: Dict[
            EntityClassName, List[EntityRelationshipDetails]
        ],
    ) -> Iterable[Tuple[UnifyingId, Dict[EntityClassName, List[Entity]]]]:

        fully_connected_hydrated_entities: Dict[EntityClassName, List[Entity]] = {}

        unifying_id, persons_entities_associations = element

        # TODO(#2769): Split this into a helper function
        entities: Dict[EntityClassName, List[Entity]] = {}
        associations: Dict[EntityRelationshipKey, List[EntityAssociation]] = {}

        for key, value in persons_entities_associations.items():
            list_elements = list(value)

            if "." in key:
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
                # The key is an EntityClassName, assert all list items are Entities
                entities[key] = []
                for list_element in list_elements:
                    if not isinstance(list_element, Entity):
                        raise ValueError(
                            "Expected list to contain all elements of "
                            f"type Entity. Found: {list_element}."
                        )
                    entities[key].append(list_element)

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

        yield unifying_id, fully_connected_hydrated_entities

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


# TODO(#2769): Delete this function once all pipelines are using v2 entity hydration
class BuildRootEntity(beam.PTransform):
    """Builds a root Entity by extracting it and the entities it is related
    to.
    """

    def __init__(
        self,
        dataset: str,
        root_entity_class: Type[Entity],
        unifying_id_field: str,
        build_related_entities: bool,
        state_code: str,
        unifying_id_field_filter_set: Optional[Set[int]] = None,
    ):
        """Initializes the PTransform with the required arguments.

        Arguments:
            dataset: The name of the dataset to read from BigQuery.
            root_entity_class: The Entity class of the root entity to be built
                as defined in the state entity layer.
            unifying_id_field: The column or attribute name of the id that
                should be used to connect the related entities to the root
                entity. The root entity and all related entities must have this
                field in its database table. This value is usually 'person_id'.
            build_related_entities: When True, also builds and attaches all
                forward-edge children of this entity.
            state_code: The state code to filter all results by
            unifying_id_field_filter_set: When non-empty, we will only build entity
                objects that can be connected to root entities with one of these
                unifying ids.
        """

        super().__init__()
        self._dataset = dataset

        if not root_entity_class:
            raise ValueError(
                f"{self.__class__.__name__}: Expecting root_entity_class to be not None."
            )

        self._root_entity_class = root_entity_class
        self._root_schema_class: Type[
            StateBase
        ] = schema_utils.get_state_database_entity_with_name(
            self._root_entity_class.__name__
        )
        self._root_table_name = self._root_schema_class.__tablename__
        self._unifying_id_field = unifying_id_field
        self._build_related_entities = build_related_entities
        self._unifying_id_field_filter_set = unifying_id_field_filter_set
        self._state_code = state_code

        if not dataset:
            raise ValueError("No valid data source passed to the pipeline.")

        _validate_schema_entity_pair(self._root_schema_class, self._root_entity_class)

        if not unifying_id_field:
            raise ValueError("No valid unifying_id_field passed to the" " pipeline.")

        if not hasattr(self._root_schema_class, unifying_id_field):
            raise ValueError(
                f"Root entity class [{self._root_schema_class.__name__}] does not have unifying id field "
                f"[{unifying_id_field}]"
            )

    def expand(self, input_or_inputs):
        """Does the work of fetching the root and related entities and grouping them"""

        # Get root entities
        root_entities = (
            input_or_inputs | f"Extract root {self._root_entity_class.__name__}"
            f" instances"
            >> _ExtractEntity(
                dataset=self._dataset,
                entity_class=self._root_entity_class,
                unifying_id_field=self._unifying_id_field,
                parent_id_field=None,
                unifying_id_field_filter_set=self._unifying_id_field_filter_set,
                state_code=self._state_code,
            )
        )

        if self._build_related_entities:
            # Get the related property entities
            properties_dict = (
                input_or_inputs | "Extract relationship property entities for "
                f"the {self._root_entity_class.__name__} "
                "instances"
                >> _ExtractRelationshipPropertyEntities(
                    dataset=self._dataset,
                    parent_schema_class=self._root_schema_class,
                    parent_id_field=self._root_entity_class.get_class_id_name(),
                    unifying_id_field=self._unifying_id_field,
                    unifying_id_field_filter_set=self._unifying_id_field_filter_set,
                    state_code=self._state_code,
                )
            )
        else:
            properties_dict = {}

        # Add root entities to the properties dict
        properties_dict[self._root_table_name] = root_entities

        # Group the cross-entity attributes to the root entities
        grouped_entities = (
            properties_dict | f"Group {self._root_entity_class.__name__}"
            f" instances to cross-entity attributes" >> beam.CoGroupByKey()
        )

        hydrate_kwargs = {"schema_class": self._root_schema_class}

        # Returned hydrated root entity instances
        return (
            grouped_entities | f"Hydrate cross-entity relationships on the"
            f" {self._root_entity_class.__name__} instances."
            >> beam.ParDo(
                _HydrateRootEntitiesWithRelationshipPropertyEntities(), **hydrate_kwargs
            )
        )


class _ExtractEntityBase(beam.PTransform):
    """Shared functionality between any PTransforms doing entity extraction."""

    def __init__(
        self,
        dataset: str,
        entity_class: Type[Entity],
        unifying_id_field: str,
        parent_id_field: Optional[str],
        unifying_id_field_filter_set: Optional[Set[int]],
        state_code: str,
    ):
        super().__init__()
        self._dataset = dataset

        self._unifying_id_field = unifying_id_field
        self._unifying_id_field_filter_set = unifying_id_field_filter_set

        self._parent_id_field = parent_id_field

        self._entity_class = entity_class
        self._schema_class: Type[
            StateBase
        ] = schema_utils.get_state_database_entity_with_name(
            self._entity_class.__name__
        )
        self._entity_table_name = self._schema_class.__tablename__
        self._entity_id_field = self._entity_class.get_class_id_name()
        self._state_code = state_code

    def _entity_has_unifying_id_field(self):
        return hasattr(self._schema_class, self._unifying_id_field)

    def _entity_has_state_code_field(self):
        return hasattr(self._schema_class, "state_code")

    def _is_unifying_id_field_in_filter_set(self, association_raw_tuple):
        if (
            not self._unifying_id_field_filter_set
            or not self._entity_has_unifying_id_field()
        ):
            return True

        return (
            getattr(association_raw_tuple, self._unifying_id_field)
            in self._unifying_id_field_filter_set
        )

    def _get_entities_table_sql_query(
        self, columns_to_include: Optional[List[str]] = None
    ):
        if not self._entity_has_unifying_id_field():
            raise ValueError(
                f"Shouldn't be querying table for entity {self._entity_class} that doesn't have field "
                f"{self._unifying_id_field} - these values will never get grouped with results, so it's "
                f"a waste to query for them."
            )

        unifying_id_field_filter_set = (
            self._unifying_id_field_filter_set
            if self._entity_has_unifying_id_field()
            else None
        )

        entity_query = select_query(
            dataset=self._dataset,
            table=self._entity_table_name,
            state_code_filter=self._state_code,
            unifying_id_field=self._unifying_id_field,
            unifying_id_field_filter_set=unifying_id_field_filter_set,
            columns_to_include=columns_to_include,
        )

        return entity_query

    def _get_entities_raw_pcollection(self, input_or_inputs: PBegin):
        if not self._entity_has_unifying_id_field():
            empty_output = (
                input_or_inputs
                | f"{self._entity_class} does not have {self._unifying_id_field}."
                >> beam.Create([])
            )
            return empty_output

        entity_query = self._get_entities_table_sql_query()

        # Read entities from BQ
        entities_raw = (
            input_or_inputs
            | f"Read {self._entity_table_name} from BigQuery"
            >> ReadFromBigQuery(query=entity_query)
        )

        return entities_raw

    @abc.abstractmethod
    def expand(self, input_or_inputs):
        pass


# TODO(#2769): Update this to only ever shallow hydrate entities.
# TODO(#2769): Rename to _ExtractAllEntitiesOfType?
# TODO(#2769): Can we eliminate _ExtractEntityBase subclassing?
class _ExtractEntity(_ExtractEntityBase):
    """Reads an Entity from a table in BigQuery, then hydrates individual entity instances.

    If |parent_id_field| is None, then this entity is the root entity, and should
    be hydrated as such. This packages the Entity in a structure of
    (unifying_id, Entity). The parent_id is not attached to the entity in this
    case because we are hydrating the root entity.

    If a |parent_id_field| is given, then this hydrates the entity as a
    relationship entity, and packages the Entity in a structure of
    (unifying_id, (parent_id, Entity)).

    The parent_id is attached to the entity in this case because we will need
    this id to later stitch this related entity to its parent entity.
    """

    def __init__(
        self,
        dataset: str,
        entity_class: Type[Entity],
        unifying_id_field: str,
        parent_id_field: Optional[str],
        unifying_id_field_filter_set: Optional[Set[int]],
        state_code: str,
    ):
        super().__init__(
            dataset,
            entity_class,
            unifying_id_field,
            parent_id_field,
            unifying_id_field_filter_set,
            state_code,
        )

    def expand(self, input_or_inputs: PBegin):
        is_root_entity = self._parent_id_field is None

        entities_raw = self._get_entities_raw_pcollection(input_or_inputs)

        hydrate_kwargs: Dict[str, Any] = {"entity_class": self._entity_class}

        if is_root_entity:
            # This is a root entity. Hydrate it as root entity.
            hydrate_kwargs["unifying_id_field"] = self._unifying_id_field

            return (
                entities_raw
                | f"Hydrate flat fields of {self._entity_class.__name__} instances"
                >> beam.ParDo(_ShallowHydrateEntity(), **hydrate_kwargs)
            )

        # This is not a root entity. Hydrate it.
        hydrate_kwargs["outer_connection_id_field"] = self._unifying_id_field
        hydrate_kwargs["inner_connection_id_field"] = self._parent_id_field

        return (
            entities_raw
            | f"Hydrate {self._entity_class.__name__} instances"
            >> beam.ParDo(_HydrateEntity(), **hydrate_kwargs)
        )


class _ExtractAssociationValues(_ExtractEntityBase):
    """Extracts the values needed to associate two entity types."""

    def __init__(
        self,
        dataset: str,
        root_entity_class: Type[Entity],
        related_entity_class: Type[Entity],
        related_id_field: str,
        association_table: str,
        unifying_id_field: str,
        unifying_id_field_filter_set: Optional[Set[int]],
        state_code: str,
    ):
        self._root_entity_class = root_entity_class
        self._root_schema_class: Type[
            StateBase
        ] = schema_utils.get_state_database_entity_with_name(
            self._root_entity_class.__name__
        )
        self._related_entity_class = related_entity_class
        self._related_schema_class: Type[
            StateBase
        ] = schema_utils.get_state_database_entity_with_name(
            self._related_entity_class.__name__
        )
        self._association_table = association_table

        if self._association_table == self._related_schema_class.__tablename__:
            # If the provided association_table is the table of the related entity,
            # then we should set that entity as the core entity to be queried from
            self._entity_class_for_query = self._related_entity_class
        else:
            self._entity_class_for_query = self._root_entity_class

        self._root_id_field = self._root_entity_class.get_class_id_name()
        self._related_id_field = related_id_field

        super().__init__(
            dataset,
            self._entity_class_for_query,
            unifying_id_field,
            None,
            unifying_id_field_filter_set,
            state_code,
        )

    def _entity_has_all_fields_for_association(self) -> bool:
        return (
            hasattr(self._schema_class, self._unifying_id_field)
            and hasattr(self._schema_class, self._root_id_field)
            and hasattr(self._schema_class, self._related_id_field)
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
                f"class: {self._schema_class}."
            )

        if self._association_table == self._schema_class.__tablename__:
            if not self._entity_has_all_fields_for_association():
                raise ValueError(
                    "All three association fields must exist on the "
                    "entity if the entity's table is provided as the "
                    "association table."
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
                f"{self._entity_class.get_entity_name()}.{self._unifying_id_field} as {UNIFYING_ID_KEY}, "
                f"{self._association_table}.{self._root_id_field}, "
                f"{self._association_table}.{self._related_id_field} "
                f"FROM `{self._dataset}.{self._association_table}` {self._association_table} "
                f"JOIN ({self._get_entities_table_sql_query()}) {self._entity_class.get_entity_name()} "
                f"ON {self._entity_class.get_entity_name()}.{self._entity_id_field} = "
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


# TODO(#2769): Delete this once all pipelines are using v2 entity hydration
class _ExtractRelationshipPropertyEntities(beam.PTransform):
    """Extracts entities that are related to a parent entity."""

    def __init__(
        self,
        dataset: str,
        parent_schema_class: Type[StateBase],
        parent_id_field: str,
        unifying_id_field: str,
        unifying_id_field_filter_set: Optional[Set[int]],
        state_code: str,
    ):
        super().__init__()
        self._dataset = dataset
        self._parent_schema_class = parent_schema_class
        self._parent_id_field = parent_id_field
        self._unifying_id_field = unifying_id_field
        self._unifying_id_field_filter_set = unifying_id_field_filter_set
        self._state_code = state_code

    def expand(self, input_or_inputs):
        """Extracts all related entities"""
        names_to_properties = (
            self._parent_schema_class.get_relationship_property_names_and_properties()
        )
        properties_dict = {}
        for property_name, property_object in names_to_properties.items():
            # Get class name associated with the property
            property_class_name = _property_class_name_from_property_object(
                property_object
            )

            property_entity_class = entity_utils.get_entity_class_in_module_with_name(
                state_entities, property_class_name
            )

            property_schema_class = schema_utils.get_state_database_entity_with_name(
                property_class_name
            )

            direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
            is_property_forward_edge = direction_checker.is_higher_ranked(
                self._parent_schema_class, property_schema_class
            )
            if is_property_forward_edge:
                # Many-to-many relationship
                if property_object.secondary is not None:
                    association_table = property_object.secondary.name
                    entity_id_field = property_entity_class.get_class_id_name()

                    # Extract the cross-entity relationship
                    entities = input_or_inputs | f"Extract {property_name}" >> _ExtractEntityWithAssociationTable(
                        dataset=self._dataset,
                        entity_class=property_entity_class,
                        unifying_id_field=self._unifying_id_field,
                        parent_id_field=self._parent_id_field,
                        association_table=association_table,
                        association_table_parent_id_field=self._parent_id_field,
                        association_table_entity_id_field=entity_id_field,
                        unifying_id_field_filter_set=self._unifying_id_field_filter_set,
                        state_code=self._state_code,
                    )

                # 1-to-many relationship
                elif property_object.uselist:
                    # Extract the cross-entity relationship
                    entities = input_or_inputs | f"Extract {property_name}" >> _ExtractEntity(
                        dataset=self._dataset,
                        entity_class=property_entity_class,
                        unifying_id_field=self._unifying_id_field,
                        parent_id_field=self._parent_id_field,
                        unifying_id_field_filter_set=self._unifying_id_field_filter_set,
                        state_code=self._state_code,
                    )

                # 1-to-1 relationship (from parent class perspective)
                else:
                    association_table = self._parent_schema_class.__tablename__
                    association_table_entity_id_field = property_object.key + "_id"

                    # Extract the cross-entity relationship
                    entities = input_or_inputs | f"Extract {property_name}" >> _ExtractEntityWithAssociationTable(
                        dataset=self._dataset,
                        entity_class=property_entity_class,
                        unifying_id_field=self._unifying_id_field,
                        parent_id_field=self._parent_id_field,
                        association_table=association_table,
                        association_table_parent_id_field=self._parent_id_field,
                        association_table_entity_id_field=association_table_entity_id_field,
                        unifying_id_field_filter_set=self._unifying_id_field_filter_set,
                        state_code=self._state_code,
                    )

                properties_dict[property_name] = entities
            else:
                print(
                    f"NOT A FORWARD EDGE {self._parent_schema_class} -> {property_schema_class}"
                )
        return properties_dict


# TODO(#2769): Delete this once all pipelines are using v2 entity hydration
class _ExtractEntityWithAssociationTable(_ExtractEntityBase):
    """Reads entities that require reading from association tables in order to connect the entity to a parent entity.

    First, reads in the entity data from a table in BigQuery. Then, reads in the ids of the parent entity and the
    associated child entity, and forms tuples for each couple. Hydrates the associated entities, and yields an
    instance of an associated entity for each parent entity it is related to.
    """

    def __init__(
        self,
        dataset: str,
        entity_class: Type[Entity],
        unifying_id_field: str,
        parent_id_field: str,
        association_table: str,
        association_table_parent_id_field: str,
        association_table_entity_id_field: str,
        unifying_id_field_filter_set: Optional[Set[int]],
        state_code: str,
    ):
        super().__init__(
            dataset,
            entity_class,
            unifying_id_field,
            parent_id_field,
            unifying_id_field_filter_set,
            state_code,
        )

        self._association_table_parent_id_field = association_table_parent_id_field
        self._association_table_entity_id_field = association_table_entity_id_field
        self._association_table = association_table

    def _get_association_tuples_raw_pcollection(self, input_or_inputs):
        """Returns the PCollection of association tuples from all relevant rows in the association table."""
        if not self._entity_has_unifying_id_field():
            empty_output = (
                input_or_inputs
                | f"{self._entity_class} does not have {self._unifying_id_field}."
                f"Dropping this associated entity." >> beam.Create([])
            )
            return empty_output

        # The join is doing a filter - we need to know which entities this instance of the pipeline will end up
        # hydrating to know which association table rows we will need.
        association_table_query = (
            f"SELECT "
            f"{self._association_table}.{self._parent_id_field}, "
            f"{self._association_table}.{self._association_table_entity_id_field} "
            f"FROM `{self._dataset}.{self._association_table}` {self._association_table} "
            f"JOIN ({self._get_entities_table_sql_query()}) {self._entity_class.get_entity_name()} "
            f"ON {self._entity_class.get_entity_name()}.{self._entity_id_field} = "
            f"{self._association_table}.{self._association_table_entity_id_field}"
        )

        # Read association table from BQ
        association_tuples_raw = (
            input_or_inputs
            | f"Read {self._association_table} from BigQuery"
            >> ReadFromBigQuery(query=association_table_query)
        )

        return association_tuples_raw

    def expand(self, input_or_inputs):
        entities_raw = self._get_entities_raw_pcollection(
            input_or_inputs,
        )
        association_tuples_raw = self._get_association_tuples_raw_pcollection(
            input_or_inputs
        )

        hydrate_kwargs = {
            "entity_class": self._entity_class,
            "outer_connection_id_field": self._entity_class.get_class_id_name(),
            "inner_connection_id_field": self._unifying_id_field,
        }

        hydrated_entities = (
            entities_raw
            | f"Hydrate {self._entity_class} instances"
            >> beam.ParDo(_HydrateEntity(), **hydrate_kwargs)
        )

        id_tuples_kwargs = {
            "association_table_parent_id_field": self._association_table_parent_id_field,
            "association_table_entity_id_field": self._association_table_entity_id_field,
        }

        association_tuples = (
            association_tuples_raw | f"Get parent_ids and entity_ids from"
            f" {self._association_table} in tuples"
            >> beam.ParDo(_FormAssociationIDTuples(), **id_tuples_kwargs)
        )

        entities_tuples = (
            {
                "child_entity_with_unifying_id": hydrated_entities,
                "parent_entity_ids": association_tuples,
            }
            | f"Group hydrated {self._entity_class} instances to associated"
            f" ids" >> beam.CoGroupByKey()
        )

        return (
            entities_tuples
            | f"Repackage {self._entity_class} and id tuples"
            >> beam.ParDo(_RepackageUnifyingIParentIdStructure())
        )


@with_input_types(
    beam.typehints.Dict[str, Any],
    **{"entity_class": Type[BuildableAttr], "unifying_id_field": str},
)
@with_output_types(beam.typehints.Tuple[int, BuildableAttr])
class _ShallowHydrateEntity(beam.DoFn):
    """Hydrates a BuildableAttr Entity."""

    def process(self, element: Dict[str, str], *_args, **kwargs):
        """Builds an entity from key-value pairs.

        Args:
            element: A dictionary containing Entity information.
            **kwargs: This should be a dictionary with values for the
                    following keys:
                        - entity_class: Entity class of type BuildableAttr to
                            be built.
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

        unifying_id = _get_value_from_element(element, unifying_id_field)

        if not unifying_id:
            raise ValueError(f"Invalid unifying_id_field: {unifying_id_field}")

        yield (unifying_id, hydrated_entity)

    def to_runner_api_parameter(self, unused_context):
        pass


# TODO(#2769): Delete this once all pipelines are using v2 entity hydration
@with_input_types(
    beam.typehints.Dict[Any, Any],
    **{
        "entity_class": Type[BuildableAttr],
        "outer_connection_id_field": str,
        "inner_connection_id_field": str,
    },
)
@with_output_types(beam.typehints.Tuple[int, Tuple[int, BuildableAttr]])
class _HydrateEntity(beam.DoFn):
    """Hydrates a BuildableAttr Entity."""

    def process(self, element, *_args, **kwargs):
        """Builds an entity from key-value pairs.

        Args:
            element: A dictionary containing Entity information.
            **kwargs: This should be a dictionary with values for the
                    following keys:
                        - entity_class: Entity class of type BuildableAttr to
                            be built.
                        - outer_connection_id_field: Field in |element|
                            corresponding to an id that should be packaged
                            as the outermost int in the resulting tuple. When
                            hydrating 1:many relationships, this is the
                            unifying_id_field. When hydrating many:many or 1:1
                            relationships, this is the id field of the entity
                            being hydrated.
                        - inner_connection_id_field: Field in |element|
                            corresponding to an id that should be packaged
                            as the innermost int in the resulting tuple. When
                            hydrating 1:many relationships, this is the
                            parent_entity_id_field. When hydrating many:many or
                            1:1 relationships, this is the unifying_id_field.

        Yields:
            A tuple in the form of (int, (int, Entity)).

        Examples:
            - When hydrating an assessment on an incarceration_period (1:many),
             this yields:
                (person_id, (incarceration_period_id, StateAssessment))

            - When hydrating a race on a person (1:many), this yields:
                (person_id, (person_id, StatePersonRace))

                (Note: when hydrating relationship entities off of StatePerson,
                the unifying_id and the parent_id will be the same.)

            - When hydrating incarceration_sentences on an incarceration_period
             (many:many), this yields:
                (incarceration_sentence_id,
                    (person_id, StateIncarcerationSentence))

            - When hydrating a judge on a court case (1:1), this yields:
                (agent_id, (person_id, StateAgent))
        """
        # Build the entity from the values in the element
        entity_class = kwargs.get("entity_class")
        outer_connection_id_field = kwargs.get("outer_connection_id_field")
        inner_connection_id_field = kwargs.get("inner_connection_id_field")

        hydrated_entity = entity_class.build_from_dictionary(element)

        if outer_connection_id_field not in element:
            logging.warning(
                "Invalid outer_connection_id_field: %s." "Dropping this entity.",
                outer_connection_id_field,
            )
            return

        outer_connection_id = _get_value_from_element(
            element, outer_connection_id_field
        )

        if not outer_connection_id:
            # We won't be able to connect this entity to other entities
            return

        if inner_connection_id_field not in element:
            logging.warning(
                "Invalid inner_connection_id_field: %s." "Dropping this entity.",
                inner_connection_id_field,
            )
            return

        inner_connection_id = _get_value_from_element(
            element, inner_connection_id_field
        )

        if not inner_connection_id:
            # We won't be able to connect this entity to other entities
            return

        yield (outer_connection_id, (inner_connection_id, hydrated_entity))

    def to_runner_api_parameter(self, unused_context):
        pass


# TODO(#2769): Delete this once all pipelines are using v2 entity hydration
@with_input_types(
    beam.typehints.Tuple[int, Dict[str, Any]], **{"schema_class": Type[StateBase]}
)
@with_output_types(beam.typehints.Tuple[int, BuildableAttr])
class _HydrateRootEntitiesWithRelationshipPropertyEntities(beam.DoFn):
    """Hydrates the cross-entity relationship properties on root entities."""

    def process(self, element, *_args, **kwargs):
        """Connects related entities to the relevant root entities.

        Args:
            element: A tuple containing the unifying id and a dictionary with
                the following structure:
                    {root_entities: [Entity],
                    property_name_1: [(root_id, Entity)],
                    property_name_2: [(root_id, Entity)], ... }

                    They key for the root entities is the table name of the
                    schema class for the root entity type.

                    The "property" keys are the names of the properties for each
                    corresponding relationship attribute on the root entity.

            **kwargs: This should be a dictionary with values for the
                    following keys:
                    - schema_class: The type of the Base class in the schema for
                        the root entity.

        The hydration works like this:

        For each root entity, go through the each property name for all of the
        attributes on the entity that are relationships to other entities. If
        there is a corresponding relationship property group for this property
        name in the entities_dict, then iterate through that group to see if
        the root_id attached to that entity matches the root_entity id. When
        you find a related entity that should be connected to the root_entity,
        add this entity as a related property on the root_entity. This process
        hydrates all given related properties on each root_entity.
        """
        schema_class = kwargs.get("schema_class")

        if not schema_class:
            raise (
                ValueError(
                    "Must pass schema Base class to "
                    "HydrateRootEntitiesWithRelationship"
                    "PropertyEntities."
                )
            )

        relationship_property_names = schema_class.get_relationship_property_names()

        unifying_id, entities_dict = element

        # Get the root entities
        root_entities = entities_dict.get(schema_class.__tablename__)

        for root_entity in root_entities:
            for property_name in relationship_property_names:
                # Get the hydrated instances of this property
                relationship_property_group = entities_dict.get(property_name)

                if not relationship_property_group:
                    continue

                entities = [
                    entity
                    for root_id, entity in relationship_property_group
                    if root_id == root_entity.get_id()
                ]

                if not entities:
                    continue

                if is_property_list(root_entity, property_name):
                    getattr(root_entity, property_name).extend(entities)
                # TODO(#1886): We should include properties that aren't forward refs, but are entity types here.
                elif is_property_forward_ref(root_entity, property_name):
                    if len(entities) > 1:
                        raise ValueError(
                            "Attempting to set a list of entities"
                            " on an attribute that is not a"
                            f"list. Property: {property_name}"
                            f" Entities: {entities}"
                        )

                    setattr(root_entity, property_name, entities[0])
                else:
                    raise ValueError(
                        "Attempting to set a non-relationship"
                        f" property: {property_name}. The function"
                        f" `get_relationship_property_names()`"
                        " is returning an invalid property."
                    )

            yield (unifying_id, root_entity)

    def to_runner_api_parameter(self, unused_context):
        pass


# TODO(#2769): Delete this once all pipelines are using v2 entity hydration
@with_input_types(beam.typehints.Tuple[int, Dict[str, Any]])
@with_output_types(beam.typehints.Tuple[int, Tuple[int, BuildableAttr]])
class _RepackageUnifyingIParentIdStructure(beam.DoFn):
    """Repackages the child entity, unifying id, and associated parent entity
    ids into tuples with the structure:

        (unifying_id, (parent_entity_id, child_entity))

    Yields one instance of this tuple for every parent entity that this related
    entity is related to.
    """

    def process(self, element, *_args, **_kwargs):
        _, structure_dict = element

        child_entity_with_unifying_id = structure_dict.get(
            "child_entity_with_unifying_id"
        )

        parent_entity_ids = structure_dict.get("parent_entity_ids")

        if child_entity_with_unifying_id and parent_entity_ids:
            unifying_id, child_entity = one(child_entity_with_unifying_id)

            for parent_entity_id in parent_entity_ids:
                yield (unifying_id, (parent_entity_id, child_entity))

    def to_runner_api_parameter(self, unused_context):
        pass


# TODO(#2769): Delete this once all pipelines are using v2 entity hydration
@with_input_types(
    beam.typehints.Dict[Any, Any],
    **{
        "association_table_parent_id_field": str,
        "association_table_entity_id_field": str,
    },
)
@with_output_types(beam.typehints.Tuple[int, int])
class _FormAssociationIDTuples(beam.DoFn):
    """Forms tuples of two ids from the given element for the corresponding
    parent_id_field and entity_id_field.

    These ids can be None if there is an un-hydrated optional relationship on
    an entity, so this only yields a tuple if both ids exist.
    """

    def process(self, element, *_args, **kwargs):
        parent_id_field = kwargs.get("association_table_parent_id_field")
        entity_id_field = kwargs.get("association_table_entity_id_field")

        entity_id = element.get(entity_id_field)
        parent_id = element.get(parent_id_field)

        if entity_id and parent_id:
            yield (entity_id, parent_id)

    def to_runner_api_parameter(self, unused_context):
        pass


def _get_value_from_element(element: Dict[str, Any], field: str) -> Any:
    value = element.get(field)

    return value


# TODO(#2769): Delete this once all pipelines are using v2 entity hydration
def _validate_schema_entity_pair(
    schema_class: Type[StateBase], entity_class: Type[Entity]
):
    """Throws an error if the schema_class and entity_class do not match, or
    if either of them are None.
    """
    if (
        not schema_class
        or not entity_class
        or schema_class.__name__ != entity_class.__name__
    ):
        raise ValueError(
            "Must send valid, matching schema and entity classes " "to BuildRootEntity."
        )


# TODO(#2769): Delete this once all pipelines are using v2 entity hydration
def _property_class_name_from_property_object(
    property_object: RelationshipProperty,
) -> str:
    if hasattr(property_object.mapper, "class_"):
        return property_object.mapper.class_.__name__
    return property_object.argument
