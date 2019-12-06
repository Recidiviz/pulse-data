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
import logging
from typing import Any, Dict, Optional, Type, Tuple
from more_itertools import one

import apache_beam as beam
from apache_beam.typehints import with_input_types, with_output_types
from sqlalchemy.orm import RelationshipProperty

from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.attr_utils import is_property_list, \
    is_property_forward_ref
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.entity import entity_utils
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.database import schema_utils


class BuildRootEntity(beam.PTransform):
    """Builds a root Entity by extracting it and the entities it is related
    to.
    """

    # TODO(1913): Add more query filtering parameters
    def __init__(self, dataset: Optional[str],
                 data_dict: Optional[Dict[str, Any]],
                 root_schema_class: Type[StateBase],
                 root_entity_class: Type[state_entities.Entity],
                 unifying_id_field: str,
                 build_related_entities: bool):
        """Initializes the PTransform with the required arguments.

        Arguments:
            dataset: The name of the dataset to read from BigQuery.
            data_dict: If reading from a static data source, data_dict contains
                all required data to build the root entity, where the key names
                correspond to the table names in BigQuery for the corresponding
                entity to be built.
            root_schema_class: The Base class of the root entity to be built as
                defined in the schema.
            root_entity_class: The Entity class of the root entity to be built
                as defined in the state entity layer.
            unifying_id_field: The column or attribute name of the id that
                should be used to connect the related entities to the root
                entity. The root entity and all related entities must have this
                field in its database table. This value is usually 'person_id'.
        """

        super(BuildRootEntity, self).__init__()
        self._dataset = dataset
        self._data_dict = data_dict
        self._root_schema_class = root_schema_class
        self._root_entity_class = root_entity_class
        self._unifying_id_field = unifying_id_field
        self._build_related_entities = build_related_entities

        if not dataset and not data_dict:
            raise ValueError("No valid data source passed to the pipeline.")

        _validate_schema_entity_pair(self._root_schema_class,
                                     self._root_entity_class)

        if not unifying_id_field:
            raise ValueError("No valid unifying_id_field passed to the"
                             " pipeline.")

    def expand(self, input_or_inputs):
        root_entity_tablename = self._root_schema_class.__tablename__

        # Get root entities
        root_entities = (input_or_inputs
                         | f"Extract root {self._root_entity_class.__name__}"
                         f" instances" >>
                         _ExtractEntity(dataset=self._dataset,
                                        data_dict=self._data_dict,
                                        table_name=root_entity_tablename,
                                        entity_class=self._root_entity_class,
                                        unifying_id_field=
                                        self._unifying_id_field,
                                        root_id_field=None))

        if self._build_related_entities:
            # Get the related property entities
            properties_dict = (input_or_inputs
                               | 'Extract relationship property entities for '
                               f"the {self._root_entity_class.__name__} "
                               "instances" >>
                               _ExtractRelationshipPropertyEntities(
                                   dataset=self._dataset,
                                   data_dict=self._data_dict,
                                   root_schema_class=self._root_schema_class,
                                   root_id_field=
                                   self._root_entity_class.get_class_id_name(),
                                   unifying_id_field=self._unifying_id_field
                               ))
        else:
            properties_dict = {}

        # Add root entities to the properties dict
        properties_dict[root_entity_tablename] = root_entities

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


class _ExtractEntity(beam.PTransform):
    """Reads an Entity from either a table in BigQuery or from the data_dict
    dictionary, then hydrates individual entity instances.

    If |root_id_field| is None, then this entity is the root entity, and should
    be hydrated as such. This packages the Entity in a structure of
    (unifying_id, Entity). The root_id is not attached to the entity in this
    case because we are hydrating the root entity.

    If a |root_id_field| is given, then this hydrates the entity as a
    relationship entity, and packages the Entity in a structure of
    (unifying_id, (root_id, Entity)).

    The root_id is attached to the entity in this case because we will need
    this id to later stitch this related entity to its root entity.
    """
    # TODO(1913): Add more query filtering parameters

    def __init__(self, dataset: Optional[str],
                 data_dict: Optional[Dict[str, Any]],
                 table_name: str,
                 entity_class: Type[state_entities.Entity],
                 unifying_id_field: str,
                 root_id_field: Optional[str]):
        super(_ExtractEntity, self).__init__()
        self._dataset = dataset
        self._data_dict = data_dict
        self._table_name = table_name
        self._entity_class = entity_class
        self._unifying_id_field = unifying_id_field
        self._root_id_field = root_id_field

    def expand(self, input_or_inputs):
        if self._data_dict:
            # Read entities from data_dict
            entities_raw = (
                input_or_inputs
                | f"Read {self._table_name} from data_dict" >>
                _CreatePCollectionFromDict(data_dict=self._data_dict,
                                           field=self._table_name))

        elif self._dataset:
            entity_query = f"SELECT * FROM `{self._dataset}." \
                f"{self._table_name}`"

            # Read entities from BQ
            entities_raw = (input_or_inputs
                            | f"Read {self._table_name} from BigQuery" >>
                            beam.io.Read(beam.io.BigQuerySource
                                         (query=entity_query,
                                          use_standard_sql=True)))
        else:
            raise ValueError("No valid data source passed to the pipeline.")

        hydrate_kwargs = {'entity_class': self._entity_class}

        if self._root_id_field is None:
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
            self._root_id_field

        return (entities_raw
                | f"Hydrate {self._entity_class.__name__} instances" >>
                beam.ParDo(_HydrateEntity(), **hydrate_kwargs))


class _ExtractRelationshipPropertyEntities(beam.PTransform):
    """Extracts entities that are related to a root entity."""
    # TODO(1913): Add more query filtering parameters

    def __init__(self, dataset: Optional[str],
                 data_dict: Optional[Dict[str, Any]],
                 root_schema_class: Type[StateBase], root_id_field: str,
                 unifying_id_field: str):
        super(_ExtractRelationshipPropertyEntities, self).__init__()
        self._dataset = dataset
        self._data_dict = data_dict
        self._root_schema_class = root_schema_class
        self._root_id_field = root_id_field
        self._unifying_id_field = unifying_id_field

    def expand(self, input_or_inputs):
        names_to_properties = self._root_schema_class. \
            get_relationship_property_names_and_properties()

        # TODO(2707): Remove this workaround in favor of a long-term approach
        if self._root_schema_class.__tablename__ == \
                'state_supervision_violation_response':
            self._create_transient_supervision_violation_relationship(
                names_to_properties)

        properties_dict = {}

        for property_name, property_object in names_to_properties.items():
            # Get class name associated with the property
            if isinstance(property_object,
                          self._TransientlyReplicatedRelationship):
                class_name = property_object.argument
            else:
                class_name = property_object.argument.arg

            entity_class = entity_utils.get_entity_class_in_module_with_name(
                state_entities, class_name)
            schema_class = \
                schema_utils.get_state_database_entity_with_name(class_name)
            table_name = schema_class.__tablename__

            if self._dataset or table_name in self._data_dict:
                # Many-to-many relationship
                if property_object.secondary is not None:
                    association_table = property_object.secondary.name
                    associated_id_field = entity_class.get_class_id_name()

                    # Extract the cross-entity relationship
                    entities = (input_or_inputs
                                | f"Extract {property_name}" >>
                                _ExtractEntityWithAssociationTable(
                                    dataset=self._dataset,
                                    data_dict=self._data_dict,
                                    table_name=table_name,
                                    entity_class=entity_class,
                                    root_id_field=self._root_id_field,
                                    associated_id_field=associated_id_field,
                                    association_table=association_table,
                                    unifying_id_field=self._unifying_id_field)
                                )

                # 1-to-many relationship
                elif property_object.uselist:
                    # Extract the cross-entity relationship
                    entities = (input_or_inputs
                                | f"Extract {property_name}" >>
                                _ExtractEntity(
                                    dataset=self._dataset,
                                    data_dict=self._data_dict,
                                    table_name=table_name,
                                    entity_class=entity_class,
                                    unifying_id_field=self._unifying_id_field,
                                    root_id_field=self._root_id_field)
                                )

                # 1-to-1 relationship (from root schema class perspective)
                else:
                    association_table = self._root_schema_class.__tablename__
                    associated_id_field = property_object.key + '_id'

                    # Extract the cross-entity relationship
                    entities = (input_or_inputs
                                | f"Extract {property_name}" >>
                                _ExtractEntityWithAssociationTable(
                                    dataset=self._dataset,
                                    data_dict=self._data_dict,
                                    table_name=table_name,
                                    entity_class=entity_class,
                                    root_id_field=self._root_id_field,
                                    associated_id_field=associated_id_field,
                                    association_table=association_table,
                                    unifying_id_field=self._unifying_id_field)
                                )

                properties_dict[property_name] = entities

        return properties_dict

    class _TransientlyReplicatedRelationship(RelationshipProperty):
        """A transient, minimal version of a SqlAlchemy RelationshipProperty.

        This is used as a workaround to allow this Dataflow entity extraction
        logic to hydrate along the backedge references that used to exist on
        the schema.py entities but no longer do as of commit
        a72dc5435024e06148ac891204a5667c72c4df1e.

        We need to "create" a model of that relationship, without persisting it
        in any way, to let the logic below traverse from a
        StateSupervisionViolationResponse to a StateSupervisionViolation.
        """

    def _create_transient_supervision_violation_relationship(
            self, names_to_properties):
        transient_relationship = self._TransientlyReplicatedRelationship(
            'StateSupervisionViolation',
            uselist=False,
            back_populates='supervision_violation_responses',
        )
        transient_relationship.key = 'supervision_violation'

        names_to_properties['supervision_violation'] = \
            transient_relationship


class _ExtractEntityWithAssociationTable(beam.PTransform):
    """Reads entities that require reading from association tables in order to
    connect the entity to a root entity.

    First, reads in the entity data from either a table in BigQuery or from the
    data_dict. Then, reads in the ids of the root entity and the associated
    entity, and forms tuples for each couple. Hydrates the associated entities,
    and yields an instance of an associated entity for each root entity it is
    related to.
    """

    # TODO(1913): Add more query filtering parameters
    def __init__(self, dataset: Optional[str],
                 data_dict: Optional[Dict[str, Any]], table_name: str,
                 entity_class: Type[state_entities.Entity],
                 root_id_field: str, associated_id_field: str,
                 association_table: str,
                 unifying_id_field: str):
        super(_ExtractEntityWithAssociationTable, self).__init__()
        self._dataset = dataset
        self._data_dict = data_dict
        self._table_name = table_name
        self._entity_class = entity_class
        self._root_id_field = root_id_field
        self._associated_id_field = associated_id_field
        self._association_table = association_table
        self._unifying_id_field = unifying_id_field

    def expand(self, input_or_inputs):
        if self._data_dict:
            # Read entities from the data_dict
            entities_raw = (
                input_or_inputs
                | f"Read {self._table_name} from data_dict" >>
                _CreatePCollectionFromDict(data_dict=self._data_dict,
                                           field=self._table_name))

            # Read association table from the data_dict
            association_tuples_raw = (
                input_or_inputs
                | f"Read in {self._association_table} from data_dict" >>
                _CreatePCollectionFromDict(data_dict=self._data_dict,
                                           field=self._association_table))

        elif self._dataset:
            entity_query = f"SELECT * FROM `{self._dataset}." \
                f"{self._table_name}`"

            # Read entities from BQ
            entities_raw = (input_or_inputs
                            | f"Read {self._table_name} from BigQuery" >>
                            beam.io.Read(beam.io.BigQuerySource
                                         (query=entity_query,
                                          use_standard_sql=True)))

            association_table_query = f"SELECT {self._root_id_field}, " \
                f"{self._associated_id_field} FROM `{self._dataset}." \
                f"{self._association_table}`"

            # Read association table from BQ
            association_tuples_raw = (
                input_or_inputs
                | f"Read {self._association_table} from BigQuery" >>
                beam.io.Read(beam.io.BigQuerySource
                             (query=association_table_query,
                              use_standard_sql=True)))
        else:
            raise ValueError("No valid data source passed to the pipeline.")

        hydrate_kwargs = {'entity_class': self._entity_class,
                          'outer_connection_id_field':
                              self._entity_class.get_class_id_name(),
                          'inner_connection_id_field':
                              self._unifying_id_field}

        hydrated_entities = (entities_raw
                             | f"Hydrate {self._entity_class} instances" >>
                             beam.ParDo(_HydrateEntity(),
                                        **hydrate_kwargs))

        id_tuples_kwargs = {'root_id_field': self._root_id_field,
                            'associated_id_field': self._associated_id_field}

        association_tuples = (
            association_tuples_raw
            | f"Get root_ids and associated_ids from"
            f" {self._association_table} in tuples" >>
            beam.ParDo(_FormAssociationIDTuples(), **id_tuples_kwargs)
        )

        entities_tuples = (
            {'unifying_id_related_entity': hydrated_entities,
             'root_entity_ids': association_tuples}
            | f"Group hydrated {self._entity_class} instances to associated"
            f" ids"
            >> beam.CoGroupByKey()
        )

        return (entities_tuples
                | f"Repackage {self._entity_class} and id tuples" >>
                beam.ParDo(_RepackageUnifyingIdRootIdStructure()))


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
                            root_entity_id_field. When hydrating many:many or
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
                the unifying_id and the root_id will be the same.)

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
class _RepackageUnifyingIdRootIdStructure(beam.DoFn):
    """Repackages the related entity, unifying id, and associated root entity
    ids into tuples with the structure:

        (unifying_id, (root_entity_id, related_entity))

    Yields one instance of this tuple for every root entity that this related
    entity is related to.
    """

    def process(self, element, *args, **kwargs):
        _, structure_dict = element

        unifying_id_related_entity = \
            structure_dict.get('unifying_id_related_entity')

        root_entity_ids = structure_dict.get('root_entity_ids')

        if unifying_id_related_entity and root_entity_ids:
            unifying_id, related_entity = \
                one(unifying_id_related_entity)

            for root_entity_id in root_entity_ids:
                yield (unifying_id, (root_entity_id, related_entity))

    def to_runner_api_parameter(self, unused_context):
        pass


@with_input_types(beam.typehints.Dict[Any, Any],
                  **{'root_id_field': str, 'associated_id_field': str})
@with_output_types(beam.typehints.Tuple[int, int])
class _FormAssociationIDTuples(beam.DoFn):
    """Forms tuples of two ids from the given element for the corresponding
    root_id_field and associated_id_field.

    These ids can be None if there is an un-hydrated optional relationship on
    an entity, so this only yields a tuple if both ids exist.
    """

    def process(self, element, *args, **kwargs):
        root_id_field = kwargs.get('root_id_field')
        associated_id_field = kwargs.get('associated_id_field')

        associated_id = element.get(associated_id_field)
        root_id = element.get(root_id_field)

        if associated_id and root_id:
            yield(associated_id, root_id)

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

        raise ValueError("No valid data source passed to the pipeline.")


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
