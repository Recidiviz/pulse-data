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
"""Utils for extracting entities from data sources to be used in pipeline
calculations."""
import abc
import logging
from typing import Any, Dict, Optional, Type, Tuple, Set
from more_itertools import one

import apache_beam as beam
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.attr_utils import is_property_list, \
    is_property_forward_ref
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.entity import entity_utils
from recidiviz.persistence.entity.entity_utils import SchemaEdgeDirectionChecker
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.database import schema_utils


class BuildRootEntity(beam.PTransform):
    """Builds a root Entity by extracting it and the entities it is related
    to.
    """

    def __init__(self, dataset: Optional[str],
                 data_dict: Optional[Dict[str, Any]],
                 root_entity_class: Type[state_entities.Entity],
                 unifying_id_field: str,
                 build_related_entities: bool,
                 unifying_id_field_filter_set: Optional[Set[int]] = None):
        """Initializes the PTransform with the required arguments.

        Arguments:
            dataset: The name of the dataset to read from BigQuery.
            data_dict: If reading from a static data source, data_dict contains
                all required data to build the root entity, where the key names
                correspond to the table names in BigQuery for the corresponding
                entity to be built.
            root_entity_class: The Entity class of the root entity to be built
                as defined in the state entity layer.
            unifying_id_field: The column or attribute name of the id that
                should be used to connect the related entities to the root
                entity. The root entity and all related entities must have this
                field in its database table. This value is usually 'person_id'.
            build_related_entities: When True, also builds and attaches all
                forward-edge children of this entity.
            unifying_id_field_filter_set: When non-empty, we will only build entity
                objects that can be connected to root entities with one of these
                unifying ids.
        """

        super(BuildRootEntity, self).__init__()
        self._dataset = dataset
        self._data_dict = data_dict

        if not root_entity_class:
            raise ValueError(f'{self.__class__.__name__}: Expecting root_entity_class to be not None.')

        self._root_entity_class = root_entity_class
        self._root_schema_class: Type[StateBase] = \
            schema_utils.get_state_database_entity_with_name(self._root_entity_class.__name__)
        self._root_table_name = self._root_schema_class.__tablename__
        self._unifying_id_field = unifying_id_field
        self._build_related_entities = build_related_entities
        self._unifying_id_field_filter_set = unifying_id_field_filter_set

        if not dataset and not data_dict:
            raise ValueError("No valid data source passed to the pipeline.")

        _validate_schema_entity_pair(self._root_schema_class,
                                     self._root_entity_class)

        if not unifying_id_field:
            raise ValueError("No valid unifying_id_field passed to the"
                             " pipeline.")

        if not hasattr(self._root_schema_class, unifying_id_field):
            raise ValueError(f'Root entity class [{self._root_schema_class.__name__}] does not have unifying id field '
                             f'[{unifying_id_field}]')

    # TODO(2769): Update this to expand recursively, perhaps changing the _build_related_entities bool to a much more
    #  detailed config about which paths down the entity tree we want to explore, with specification on each node about
    #  whether we want to hydrate fields or just relationship to child objects.
    def expand(self, input_or_inputs):

        # Get root entities
        root_entities = (input_or_inputs
                         | f"Extract root {self._root_entity_class.__name__}"
                         f" instances" >>
                         _ExtractEntity(dataset=self._dataset,
                                        data_dict=self._data_dict,
                                        entity_class=self._root_entity_class,
                                        unifying_id_field=self._unifying_id_field,
                                        parent_id_field=None,
                                        unifying_id_field_filter_set=self._unifying_id_field_filter_set))

        if self._build_related_entities:
            # Get the related property entities
            properties_dict = (input_or_inputs
                               | 'Extract relationship property entities for '
                               f"the {self._root_entity_class.__name__} "
                               "instances" >>
                               _ExtractRelationshipPropertyEntities(
                                   dataset=self._dataset,
                                   data_dict=self._data_dict,
                                   parent_schema_class=self._root_schema_class,
                                   parent_id_field=self._root_entity_class.get_class_id_name(),
                                   unifying_id_field=self._unifying_id_field,
                                   unifying_id_field_filter_set=self._unifying_id_field_filter_set
                               ))
        else:
            properties_dict = {}

        # Add root entities to the properties dict
        properties_dict[self._root_table_name] = root_entities

        # Group the cross-entity attributes to the root entities
        grouped_entities = (properties_dict
                            | f"Group {self._root_entity_class.__name__}"
                            f" instances to cross-entity attributes" >>
                            beam.CoGroupByKey()
                            )

        hydrate_kwargs = {'schema_class': self._root_schema_class}

        # Returned hydrated root entity instances
        return (grouped_entities
                | f"Hydrate cross-entity relationships on the"
                f" {self._root_entity_class.__name__} instances." >>
                beam.ParDo(
                    _HydrateRootEntitiesWithRelationshipPropertyEntities(),
                    **hydrate_kwargs))


class _ExtractEntityBase(beam.PTransform):
    """Shared functionality between any PTransforms doing entity extraction."""
    def __init__(self,
                 dataset: Optional[str],
                 data_dict: Optional[Dict[str, Any]],
                 entity_class: Type[state_entities.Entity],
                 unifying_id_field: str,
                 parent_id_field: Optional[str],
                 unifying_id_field_filter_set: Optional[Set[int]]):
        super(_ExtractEntityBase, self).__init__()
        self._dataset = dataset
        self._data_dict = data_dict
        self._entity_class = entity_class
        self._schema_class: Type[StateBase] = \
            schema_utils.get_state_database_entity_with_name(self._entity_class.__name__)
        self._entity_table_name = self._schema_class.__tablename__
        self._unifying_id_field = unifying_id_field
        self._parent_id_field = parent_id_field
        self._unifying_id_field_filter_set = unifying_id_field_filter_set

    def _entity_has_unifying_id_field(self):
        return hasattr(self._schema_class, self._unifying_id_field)

    def _is_unifying_id_field_in_filter_set(self, association_raw_tuple):
        if not self._unifying_id_field_filter_set or not self._entity_has_unifying_id_field():
            return True

        return getattr(association_raw_tuple, self._unifying_id_field) in self._unifying_id_field_filter_set

    def _get_entities_table_sql_query(self):
        if not self._entity_has_unifying_id_field():
            raise ValueError(f"Shouldn't be querying table for entity {self._entity_class} that doesn't have field "
                             f"{self._unifying_id_field} - these values will never get grouped with results, so it's "
                             f"a waste to query for them.")

        entity_query = f"SELECT * FROM `{self._dataset}.{self._entity_table_name}`"

        if self._entity_has_unifying_id_field() and self._unifying_id_field_filter_set:
            id_str_set = {str(unifying_id) for unifying_id in self._unifying_id_field_filter_set if str(unifying_id)}

            entity_query = entity_query + \
                           f" WHERE {self._unifying_id_field} IN ({', '.join(id_str_set)})"

        return entity_query

    def _get_entities_raw_pcollection(self, input_or_inputs):
        if not self._entity_has_unifying_id_field():
            return []

        if self._data_dict:
            entities_raw = (input_or_inputs
                            | f"Read {self._entity_table_name} from data_dict" >>
                            _CreatePCollectionFromDict(data_dict=self._data_dict,
                                                       field=self._entity_table_name))
        elif self._dataset:
            entity_query = self._get_entities_table_sql_query()

            # Read entities from BQ
            entities_raw = (input_or_inputs
                            | f"Read {self._entity_table_name} from BigQuery" >>
                            beam.io.Read(beam.io.BigQuerySource
                                         (query=entity_query,
                                          use_standard_sql=True)))
        else:
            raise ValueError("No valid data source passed to the pipeline.")

        return entities_raw

    @abc.abstractmethod
    def expand(self, input_or_inputs):
        pass


class _ExtractEntity(_ExtractEntityBase):
    """Reads an Entity from either a table in BigQuery or from the data_dict
    dictionary, then hydrates individual entity instances.

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

    def __init__(self, dataset: Optional[str],
                 data_dict: Optional[Dict[str, Any]],
                 entity_class: Type[state_entities.Entity],
                 unifying_id_field: str,
                 parent_id_field: Optional[str],
                 unifying_id_field_filter_set: Optional[Set[int]]):
        super(_ExtractEntity, self).__init__(
            dataset, data_dict, entity_class, unifying_id_field, parent_id_field, unifying_id_field_filter_set)

    def expand(self, input_or_inputs):
        entities_raw = self._get_entities_raw_pcollection(input_or_inputs)

        hydrate_kwargs = {'entity_class': self._entity_class}

        if self._parent_id_field is None:
            # This is a root entity. Hydrate it as root entity.
            hydrate_kwargs['unifying_id_field'] = \
                self._unifying_id_field

            return (entities_raw
                    | f"Hydrate root {self._entity_class.__name__} instances" >>
                    beam.ParDo(_HydrateRootEntity(), **hydrate_kwargs))

        # This is not a root entity. Hydrate it.
        hydrate_kwargs['outer_connection_id_field'] = \
            self._unifying_id_field
        hydrate_kwargs['inner_connection_id_field'] = \
            self._parent_id_field

        return (entities_raw
                | f"Hydrate {self._entity_class.__name__} instances" >>
                beam.ParDo(_HydrateEntity(), **hydrate_kwargs))


class _ExtractRelationshipPropertyEntities(beam.PTransform):
    """Extracts entities that are related to a parent entity."""

    def __init__(self,
                 dataset: Optional[str],
                 data_dict: Optional[Dict[str, Any]],
                 parent_schema_class: Type[StateBase],
                 parent_id_field: str,
                 unifying_id_field: str,
                 unifying_id_field_filter_set: Optional[Set[int]]):
        super(_ExtractRelationshipPropertyEntities, self).__init__()
        self._dataset = dataset
        self._data_dict = data_dict
        self._parent_schema_class = parent_schema_class
        self._parent_id_field = parent_id_field
        self._unifying_id_field = unifying_id_field
        self._unifying_id_field_filter_set = unifying_id_field_filter_set

    def expand(self, input_or_inputs):
        names_to_properties = self._parent_schema_class. \
            get_relationship_property_names_and_properties()

        properties_dict = {}
        for property_name, property_object in names_to_properties.items():
            # Get class name associated with the property
            property_class_name = property_object.argument.arg

            property_entity_class = entity_utils.get_entity_class_in_module_with_name(
                state_entities, property_class_name)
            property_schema_class = \
                schema_utils.get_state_database_entity_with_name(property_class_name)
            property_table_name = property_schema_class.__tablename__

            direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
            is_property_forward_edge = direction_checker.is_higher_ranked(self._parent_schema_class,
                                                                          property_schema_class)
            if (self._dataset or property_table_name in self._data_dict) and is_property_forward_edge:
                # Many-to-many relationship
                if property_object.secondary is not None:
                    association_table = property_object.secondary.name
                    entity_id_field = property_entity_class.get_class_id_name()

                    # Extract the cross-entity relationship
                    entities = (input_or_inputs
                                | f"Extract {property_name}" >>
                                _ExtractEntityWithAssociationTable(
                                    dataset=self._dataset,
                                    data_dict=self._data_dict,
                                    entity_class=property_entity_class,
                                    parent_id_field=self._parent_id_field,
                                    entity_id_field=entity_id_field,
                                    association_table=association_table,
                                    unifying_id_field=self._unifying_id_field,
                                    unifying_id_field_filter_set=self._unifying_id_field_filter_set)
                                )

                # 1-to-many relationship
                elif property_object.uselist:
                    # Extract the cross-entity relationship
                    entities = (input_or_inputs
                                | f"Extract {property_name}" >>
                                _ExtractEntity(
                                    dataset=self._dataset,
                                    data_dict=self._data_dict,
                                    entity_class=property_entity_class,
                                    unifying_id_field=self._unifying_id_field,
                                    parent_id_field=self._parent_id_field,
                                    unifying_id_field_filter_set=self._unifying_id_field_filter_set)
                                )

                # 1-to-1 relationship (from parent class perspective)
                else:
                    association_table = self._parent_schema_class.__tablename__
                    entity_id_field = property_object.key + '_id'

                    # Extract the cross-entity relationship
                    entities = (input_or_inputs
                                | f"Extract {property_name}" >>
                                _ExtractEntityWithAssociationTable(
                                    dataset=self._dataset,
                                    data_dict=self._data_dict,
                                    entity_class=property_entity_class,
                                    parent_id_field=self._parent_id_field,
                                    entity_id_field=entity_id_field,
                                    association_table=association_table,
                                    unifying_id_field=self._unifying_id_field,
                                    unifying_id_field_filter_set=self._unifying_id_field_filter_set)
                                )

                properties_dict[property_name] = entities

        return properties_dict


class _ExtractEntityWithAssociationTable(_ExtractEntityBase):
    """Reads entities that require reading from association tables in order to
    connect the entity to a parent entity.

    First, reads in the entity data from either a table in BigQuery or from the
    data_dict. Then, reads in the ids of the parent entity and the associated child
    entity, and forms tuples for each couple. Hydrates the associated entities,
    and yields an instance of an associated entity for each parent entity it is
    related to.
    """

    def __init__(self, dataset: Optional[str],
                 data_dict: Optional[Dict[str, Any]],
                 entity_class: Type[state_entities.Entity],
                 parent_id_field: str,
                 entity_id_field: str,
                 association_table: str,
                 unifying_id_field: str,
                 unifying_id_field_filter_set: Optional[Set[int]]):
        super(_ExtractEntityWithAssociationTable, self).__init__(
            dataset, data_dict, entity_class, unifying_id_field, parent_id_field, unifying_id_field_filter_set)

        self._entity_id_field = entity_id_field
        self._association_table = association_table

    def _get_association_tuples_raw_pcollection(self, input_or_inputs):
        """Returns the PCollection of association tuples from all relevant rows in the association table."""
        if not self._entity_has_unifying_id_field():
            return []

        if self._data_dict:
            # Read association table from the data_dict
            association_tuples_raw = (
                input_or_inputs
                | f"Read in {self._association_table} from data_dict" >>
                _CreatePCollectionFromDict(data_dict=self._data_dict,
                                           field=self._association_table))

        elif self._dataset:
            # The join is doing a filter - we need to know which entities this instance of the pipeline will end up
            # hydrating to know which association table rows we will need.
            association_table_query = \
                f"SELECT " \
                f"{self._association_table}.{self._parent_id_field}, " \
                f"{self._association_table}.{self._entity_id_field} " \
                f"FROM `{self._dataset}.{self._association_table}` {self._association_table} " \
                f"JOIN ({self._get_entities_table_sql_query()}) {self._entity_class.get_entity_name()} " \
                f"ON {self._entity_class.get_entity_name()}.{self._entity_id_field} = " \
                f"{self._association_table}.{self._entity_id_field}"

            # Read association table from BQ
            association_tuples_raw = (
                input_or_inputs
                | f"Read {self._association_table} from BigQuery" >>
                beam.io.Read(beam.io.BigQuerySource
                             (query=association_table_query,
                              use_standard_sql=True)))
        else:
            raise ValueError("No valid data source passed to the pipeline.")

        return association_tuples_raw

    def expand(self, input_or_inputs):
        entities_raw = self._get_entities_raw_pcollection(input_or_inputs)
        association_tuples_raw = self._get_association_tuples_raw_pcollection(input_or_inputs)

        hydrate_kwargs = {'entity_class': self._entity_class,
                          'outer_connection_id_field':
                              self._entity_class.get_class_id_name(),
                          'inner_connection_id_field':
                              self._unifying_id_field}

        hydrated_entities = (entities_raw
                             | f"Hydrate {self._entity_class} instances" >>
                             beam.ParDo(_HydrateEntity(),
                                        **hydrate_kwargs))

        id_tuples_kwargs = {'parent_id_field': self._parent_id_field,
                            'entity_id_field': self._entity_id_field}

        association_tuples = (
            association_tuples_raw
            | f"Get parent_ids and entity_ids from"
            f" {self._association_table} in tuples" >>
            beam.ParDo(_FormAssociationIDTuples(), **id_tuples_kwargs)
        )

        entities_tuples = (
            {'child_entity_with_unifying_id': hydrated_entities,
             'parent_entity_ids': association_tuples}
            | f"Group hydrated {self._entity_class} instances to associated"
            f" ids"
            >> beam.CoGroupByKey()
        )

        return (entities_tuples
                | f"Repackage {self._entity_class} and id tuples" >>
                beam.ParDo(_RepackageUnifyingIParentIdStructure()))


@with_input_types(beam.typehints.Dict[Any, Any],
                  **{'entity_class': Type[BuildableAttr],
                     'unifying_id_field': str})
@with_output_types(beam.typehints.Tuple[int, BuildableAttr])
class _HydrateRootEntity(beam.DoFn):
    """Hydrates a BuildableAttr Entity."""

    def process(self, element, *args, **kwargs):
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
        entity_class = kwargs.get('entity_class')
        unifying_id_field = kwargs.get('unifying_id_field')

        hydrated_entity = entity_class.build_from_dictionary(element)

        unifying_id = _get_value_from_element(element, unifying_id_field)

        if not unifying_id:
            raise ValueError(f"Invalid unifying_id_field: {unifying_id_field}")

        yield (unifying_id, hydrated_entity)

    def to_runner_api_parameter(self, unused_context):
        pass


@with_input_types(beam.typehints.Dict[Any, Any],
                  **{'entity_class': Type[BuildableAttr],
                     'outer_connection_id_field': str,
                     'inner_connection_id_field': str})
@with_output_types(beam.typehints.Tuple[int, Tuple[int, BuildableAttr]])
class _HydrateEntity(beam.DoFn):
    """Hydrates a BuildableAttr Entity."""

    def process(self, element, *args, **kwargs):
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

            - When hydrating a bond on a charge (1:1), this yields:
                (bond_id, (person_id, StateBond))
        """
        # Build the entity from the values in the element
        entity_class = kwargs.get('entity_class')
        outer_connection_id_field = kwargs.get('outer_connection_id_field')
        inner_connection_id_field = kwargs.get('inner_connection_id_field')

        hydrated_entity = entity_class.build_from_dictionary(element)

        if outer_connection_id_field not in element:
            logging.warning("Invalid outer_connection_id_field: %s."
                            "Dropping this entity.", outer_connection_id_field)
            return

        outer_connection_id = _get_value_from_element(element,
                                                      outer_connection_id_field)

        if not outer_connection_id:
            # We won't be able to connect this entity to other entities
            return

        if inner_connection_id_field not in element:
            logging.warning("Invalid inner_connection_id_field: %s."
                            "Dropping this entity.", inner_connection_id_field)
            return

        inner_connection_id = _get_value_from_element(element,
                                                      inner_connection_id_field)

        if not inner_connection_id:
            # We won't be able to connect this entity to other entities
            return

        yield (outer_connection_id, (inner_connection_id, hydrated_entity))

    def to_runner_api_parameter(self, unused_context):
        pass


@with_input_types(beam.typehints.Tuple[int, Dict[str, Any]],
                  **{'schema_class': Type[StateBase]})
@with_output_types(beam.typehints.Tuple[int, BuildableAttr])
class _HydrateRootEntitiesWithRelationshipPropertyEntities(beam.DoFn):
    """Hydrates the cross-entity relationship properties on root entities."""

    def process(self, element, *args, **kwargs):
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
        schema_class = kwargs.get('schema_class')

        if not schema_class:
            raise(ValueError("Must pass schema Base class to "
                             "HydrateRootEntitiesWithRelationship"
                             "PropertyEntities."))

        relationship_property_names = \
            schema_class.get_relationship_property_names()

        unifying_id, entities_dict = element

        # Get the root entities
        root_entities = entities_dict.get(schema_class.__tablename__)

        for root_entity in root_entities:
            for property_name in relationship_property_names:
                # Get the hydrated instances of this property
                relationship_property_group = \
                    entities_dict.get(property_name)

                if not relationship_property_group:
                    continue

                entities = [entity
                            for root_id, entity in relationship_property_group
                            if root_id == root_entity.get_id()]

                if not entities:
                    continue

                if is_property_list(root_entity, property_name):
                    getattr(root_entity, property_name).extend(entities)
                elif is_property_forward_ref(root_entity, property_name):
                    if len(entities) > 1:
                        raise ValueError("Attempting to set a list of entities"
                                         " on an attribute that is not a"
                                         f"list. Property: {property_name}"
                                         f" Entities: {entities}")

                    setattr(root_entity, property_name, entities[0])
                else:
                    raise ValueError("Attempting to set a non-relationship"
                                     f" property: {property_name}. The function"
                                     f" `get_relationship_property_names()`"
                                     " is returning an invalid property.")

            yield (unifying_id, root_entity)

    def to_runner_api_parameter(self, unused_context):
        pass


@with_input_types(beam.typehints.Tuple[int, Dict[str, Any]])
@with_output_types(beam.typehints.Tuple[int, Tuple[int, BuildableAttr]])
class _RepackageUnifyingIParentIdStructure(beam.DoFn):
    """Repackages the child entity, unifying id, and associated parent entity
    ids into tuples with the structure:

        (unifying_id, (parent_entity_id, child_entity))

    Yields one instance of this tuple for every parent entity that this related
    entity is related to.
    """

    def process(self, element, *args, **kwargs):
        _, structure_dict = element

        child_entity_with_unifying_id = \
            structure_dict.get('child_entity_with_unifying_id')

        parent_entity_ids = structure_dict.get('parent_entity_ids')

        if child_entity_with_unifying_id and parent_entity_ids:
            unifying_id, child_entity = \
                one(child_entity_with_unifying_id)

            for parent_entity_id in parent_entity_ids:
                yield (unifying_id, (parent_entity_id, child_entity))

    def to_runner_api_parameter(self, unused_context):
        pass


@with_input_types(beam.typehints.Dict[Any, Any],
                  **{'parent_id_field': str, 'entity_id_field': str})
@with_output_types(beam.typehints.Tuple[int, int])
class _FormAssociationIDTuples(beam.DoFn):
    """Forms tuples of two ids from the given element for the corresponding
    parent_id_field and entity_id_field.

    These ids can be None if there is an un-hydrated optional relationship on
    an entity, so this only yields a tuple if both ids exist.
    """

    def process(self, element, *args, **kwargs):
        parent_id_field = kwargs.get('parent_id_field')
        entity_id_field = kwargs.get('entity_id_field')

        entity_id = element.get(entity_id_field)
        parent_id = element.get(parent_id_field)

        if entity_id and parent_id:
            yield(entity_id, parent_id)

    def to_runner_api_parameter(self, unused_context):
        pass


class _CreatePCollectionFromDict(beam.PTransform):
    """Creates a PCollection from the values in given data_dict corresponding to
    the given field."""

    def __init__(self, data_dict: Dict[str, Any], field: str):
        super(_CreatePCollectionFromDict, self).__init__()
        self._data_dict = data_dict
        self._field = field

    def expand(self, input_or_inputs):
        entity_data = self._data_dict.get(self._field)

        if entity_data:
            return (input_or_inputs
                    | f"Load {self._field}" >>
                    beam.Create(entity_data))

        raise ValueError(f"No valid data source passed to the pipeline for table: {self._field}")


def _get_value_from_element(element: Dict[str, Any], field: str) -> Any:
    value = element.get(field)

    return value


def _validate_schema_entity_pair(schema_class: Type[StateBase],
                                 entity_class: Type[state_entities.Entity]):
    """Throws an error if the schema_class and entity_class do not match, or
    if either of them are None.
    """
    if not schema_class or not entity_class or \
            schema_class.__name__ != entity_class.__name__:
        raise ValueError("Must send valid, matching schema and entity classes "
                         "to BuildRootEntity.")
