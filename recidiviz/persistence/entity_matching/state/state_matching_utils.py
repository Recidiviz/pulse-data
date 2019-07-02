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
# ============================================================================
"""State schema specific utils for match database entities with ingested
entities."""
from typing import List, cast, Any

import attr

from recidiviz.common.constants import enum_canonical_strings
from recidiviz.persistence.entity.base_entity import Entity, ExternalIdEntity
from recidiviz.persistence.entity.entity_utils import \
    EntityFieldType, get_set_entity_field_names
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StatePersonExternalId, StatePersonAlias, StatePersonRace, \
    StatePersonEthnicity
from recidiviz.persistence.errors import EntityMatchingError


class EntityTree:
    """Object that contains an entity and the list of ancestors traversed to get
    to this entity from the root Person node."""
    def __init__(self, entity: Entity, ancestor_chain: List[Entity]):
        if not entity:
            raise EntityMatchingError(
                "When creating EntityTree object, entity field must be set",
                'entity_tree')

        # The final child in this EntityTree.
        self.entity = entity

        # The list of ancestors for the entity above. This list is ordered from
        # furthest to closest ancestor.
        self.ancestor_chain = ancestor_chain[:]

    def generate_parent_tree(self) -> 'EntityTree':
        """Returns an EntityTree object for the direct parent of this
        EntityTree.
        """
        return EntityTree(entity=self.ancestor_chain[-1],
                          ancestor_chain=self.ancestor_chain[:-1])

    def generate_child_trees(self, children: List[Entity]) \
            -> List['EntityTree']:
        """For each of the provided |children| creates a new EntityTree object
        by adding the child to this EntityTree. Returns these new EntityTrees.
        """
        result = []
        for child in children:
            result.append(EntityTree(
                entity=child,
                ancestor_chain=self.ancestor_chain + [self.entity]))
        return result

    def __eq__(self, other):
        return self.entity == other.entity \
               and self.ancestor_chain == other.ancestor_chain


class IndividualMatchResult:
    """Object that represents the result of a match attempt for an
    ingested_entity_tree."""
    def __init__(self, ingested_entity_tree: EntityTree,
                 merged_entity_trees: List[EntityTree]):
        if not ingested_entity_tree:
            raise EntityMatchingError(
                "When creating IndividualMatchResult object, "
                "ingested_entity_tree field must be set",
                'individual_match_result')

        # The initial EntityTree to be matched to DB EntityTrees.
        self.ingested_entity_tree = ingested_entity_tree

        # If matching was successful, these are results of merging the
        # ingested_entity_tree with any of it's DB matches.
        self.merged_entity_trees = merged_entity_trees


class MatchResults:
    """Object that represents the results of a match attempt for a group of
    ingested and database EntityTree objects"""
    def __init__(self, individual_match_results: List[IndividualMatchResult],
                 unmatched_db_entities: List[Entity]):
        if not individual_match_results:
            raise EntityMatchingError(
                "When creating MatchResults object, individual_match_results "
                "field must be set/non empty", 'individual_match_result')

        # Results for each individual ingested EntityTree.
        self.individual_match_results = individual_match_results

        # List of db entities that were unmatched.
        self.unmatched_db_entities = unmatched_db_entities


def is_match(*, ingested_entity: EntityTree, db_entity: EntityTree) -> bool:
    """Returns true if the provided |ingested_entity| matches the provided
    |db_entity|. Otherwise returns False.
    """
    return _is_match(ingested_entity=ingested_entity.entity,
                     db_entity=db_entity.entity)


def _is_match(*, ingested_entity: Entity, db_entity: Entity) -> bool:
    """Returns true if the provided |ingested_entity| matches the provided
    |db_entity|. Otherwise returns False.
    """
    if not ingested_entity or not db_entity:
        return ingested_entity == db_entity

    if ingested_entity.__class__ != db_entity.__class__:
        raise EntityMatchingError(
            f"is_match received entities of two different classes: "
            f"ingested entity {ingested_entity.__class__.__name__} and "
            f"db_entity {db_entity.__class__.__name__}",
            ingested_entity.get_entity_name())

    if isinstance(ingested_entity, StatePerson):
        db_entity = cast(StatePerson, db_entity)
        for ingested_external_id in ingested_entity.external_ids:
            for db_external_id in db_entity.external_ids:
                if _is_match(ingested_entity=ingested_external_id,
                             db_entity=db_external_id):
                    return True
        return False

    if isinstance(ingested_entity, StatePersonExternalId):
        db_entity = cast(StatePersonExternalId, db_entity)
        return ingested_entity.state_code == db_entity.state_code \
               and ingested_entity.external_id == db_entity.external_id \
               and ingested_entity.id_type == db_entity.id_type

    # As person has already been matched, assume that any of these 'person
    # attribute' entities are matches if their state_codes align.
    if isinstance(ingested_entity, StatePersonAlias):
        db_entity = cast(StatePersonAlias, db_entity)
        return ingested_entity.state_code == db_entity.state_code
    if isinstance(ingested_entity, StatePersonRace):
        db_entity = cast(StatePersonRace, db_entity)
        return ingested_entity.state_code == db_entity.state_code
    if isinstance(ingested_entity, StatePersonEthnicity):
        db_entity = cast(StatePersonEthnicity, db_entity)
        return ingested_entity.state_code == db_entity.state_code

    db_entity = cast(ExternalIdEntity, db_entity)
    ingested_entity = cast(ExternalIdEntity, ingested_entity)
    return ingested_entity.external_id == db_entity.external_id


def merge_flat_fields(*, ingested_entity: Entity, db_entity: Entity) -> Entity:
    """Merges all set non-relationship fields on the |ingested_entity| onto the
    |db_entity|. Returns the newly merged entity.
    """
    for child_field_name in get_set_entity_field_names(
            ingested_entity, EntityFieldType.FLAT_FIELD):
        # Do not overwrite with default status
        if child_field_name == 'status' and has_default_status(ingested_entity):
            continue

        set_field(db_entity, child_field_name,
                  get_field(ingested_entity, child_field_name))

    return db_entity


def remove_back_edges(entity: Entity):
    """ Removes all backedges from the provided |entity| and all of its
    children.
    """
    for child_field_name in get_set_entity_field_names(
            entity, EntityFieldType.BACK_EDGE):
        child_field = get_field(entity, child_field_name)
        if isinstance(child_field, list):
            set_field(entity, child_field_name, [])
        else:
            set_field(entity, child_field_name, None)

    for child_field_name in get_set_entity_field_names(
            entity, EntityFieldType.FORWARD_EDGE):
        child_field = get_field(entity, child_field_name)
        if isinstance(child_field, list):
            for child in child_field:
                remove_back_edges(child)
        else:
            remove_back_edges(child_field)


def add_person_to_entity_graph(persons: List[StatePerson]):
    """Adds a person back edge to children of each provided person in
    |persons|.
    """
    for person in persons:
        _add_person_to_entity_graph_helper(person, person)


def _add_person_to_entity_graph_helper(person: StatePerson, entity: Entity):
    _set_person_on_entity(person, entity)

    for child_field_name in get_set_entity_field_names(
            entity, EntityFieldType.FORWARD_EDGE):
        child_field = get_field(entity, child_field_name)
        if isinstance(child_field, list):
            for child in child_field:
                _add_person_to_entity_graph_helper(person, child)
        else:
            _add_person_to_entity_graph_helper(person, child_field)


def _set_person_on_entity(person: StatePerson, entity: Entity):
    if hasattr(entity, 'person'):
        set_field(entity, 'person', person)


def has_default_status(entity: Entity) -> bool:
    if hasattr(entity, 'status'):
        status = get_field(entity, 'status')
        return status \
               and status.value == enum_canonical_strings.present_without_info
    return False


def is_placeholder(entity: Entity) -> bool:
    """Determines if the provided entity is a placeholder. Conceptually, a
    placeholder is an object that we have no information about, but have
    inferred its existence based on other objects we do have information about.
    Generally, an entity is a placeholder if all of the optional flat fields are
    empty or set to a default value.
    """

    # Although these are not flat fields, they represent characteristics of a
    # person. If present, we do have information about the provided person, and
    # therefore it is not a placeholder.
    if isinstance(entity, StatePerson):
        entity = cast(StatePerson, entity)
        if any([entity.external_ids, entity.races, entity.aliases,
                entity.ethnicities]):
            return False

    copy = attr.evolve(entity)

    # Clear id
    copy.clear_id()

    # Clear state code as it's non nullable
    if hasattr(copy, 'state_code'):
        set_field(copy, 'state_code', None)

    # Clear status if set to default value
    if has_default_status(entity):
        set_field(copy, 'status', None)

    set_flat_fields = get_set_entity_field_names(
        copy, EntityFieldType.FLAT_FIELD)
    return not bool(set_flat_fields)


def get_field(entity: Entity, field_name: str):
    if not hasattr(entity, field_name):
        raise EntityMatchingError(
            f"Expected entity {entity} to have field {field_name}, but it did "
            f"not.", entity.get_entity_name())
    return getattr(entity, field_name)


def set_field(entity: Entity, field_name: str, value: Any):
    if not hasattr(entity, field_name):
        raise EntityMatchingError(
            f"Expected entity {entity} to have field {field_name}, but it did "
            f"not.", entity.get_entity_name())
    return setattr(entity, field_name, value)


def generate_child_entity_trees(
        child_field_name: str, entity_trees: List[EntityTree]) \
        -> List[EntityTree]:
    """Generates a new EntityTree object for each found child of the provided
    |entity_trees| with the field name |child_field_name|.
    """
    child_trees = []
    for entity_tree in entity_trees:
        child_field = get_field(entity_tree.entity, child_field_name)

        # If child_field is unset, skip
        if not child_field:
            continue
        if isinstance(child_field, list):
            children = child_field
        else:
            children = [child_field]
        child_trees.extend(entity_tree.generate_child_trees(children))
    return child_trees


def remove_child_from_entity(
        *, entity: Entity, child_field_name: str, child_to_remove: Entity):
    """If present, removes the |child_to_remove| from the |child_field_name|
    field on the |entity|.
    """
    child_field = get_field(entity, child_field_name)

    if isinstance(child_field, list):
        if child_to_remove in child_field:
            child_field.remove(child_to_remove)
    elif isinstance(child_field, Entity):
        if child_field == child_to_remove:
            child_field = None
    set_field(entity, child_field_name, child_field)


def add_child_to_entity(
        *, entity: Entity, child_field_name: str, child_to_add: Entity):
    """Adds the |child_to_add| to the |child_field_name| field on the
    |entity|.
    """
    child_field = get_field(entity, child_field_name)

    if isinstance(child_field, list):
        if child_to_add not in child_field:
            child_field.append(child_to_add)
    if isinstance(child_field, Entity):
        if child_field and child_field != child_to_add:
            raise EntityMatchingError(
                f"Attempting to add child {child_to_add} to entity {entity}, "
                f"but {child_field_name} already had different value "
                f"{child_field}", entity.get_entity_name())
        child_field = child_to_add
    set_field(entity, child_field_name, child_field)
