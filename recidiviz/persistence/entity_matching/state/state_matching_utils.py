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
"""State specific utils for entity matching. Utils in this file are generic to any DatabaseEntity."""
import logging
from collections import defaultdict
from typing import List, cast, Optional, Set, Type, Dict, Sequence

from recidiviz.common.constants import enum_canonical_strings
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_court_case import StateCourtType
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema, dao
from recidiviz.persistence.database.schema_utils import \
    get_non_history_state_database_entities
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.entity_utils import \
    EntityFieldType, is_placeholder, \
    get_set_entity_field_names, SchemaEdgeDirectionChecker
from recidiviz.common.common_utils import check_all_objs_have_type
from recidiviz.persistence.entity_matching.entity_matching_types import \
    EntityTree
from recidiviz.persistence.errors import EntityMatchingError


def is_match(ingested_entity: EntityTree,
             db_entity: EntityTree) -> bool:
    """Returns true if the provided |ingested_entity| matches the provided
    |db_entity|. Otherwise returns False.
    """
    return _is_match(ingested_entity=ingested_entity.entity,
                     db_entity=db_entity.entity)


def _is_match(*,
              ingested_entity: DatabaseEntity,
              db_entity: DatabaseEntity) -> bool:
    """Returns true if the provided |ingested_entity| matches the provided
    |db_entity|. Otherwise returns False.
    """
    if not ingested_entity or not db_entity:
        return ingested_entity == db_entity

    if ingested_entity.__class__ != db_entity.__class__:
        raise ValueError(
            f"is_match received entities of two different classes: ingested entity [{ingested_entity}] and db_entity "
            f"[{db_entity}]")

    if not isinstance(ingested_entity, DatabaseEntity):
        raise ValueError(f"Unexpected type for ingested entity [{ingested_entity}]: [{type(ingested_entity)}]")
    if not isinstance(db_entity, DatabaseEntity):
        raise ValueError(f"Unexpected type for db entity [{db_entity}]: [{type(db_entity)}]")

    if isinstance(ingested_entity, schema.StatePerson):
        db_entity = cast(schema.StatePerson, db_entity)
        for ingested_external_id in ingested_entity.external_ids:
            for db_external_id in db_entity.external_ids:
                if _is_match(ingested_entity=ingested_external_id,
                             db_entity=db_external_id):
                    return True
        return False

    # Aside from people, all entities are state specific.
    if ingested_entity.get_field('state_code') \
            != db_entity.get_field('state_code'):
        return False

    # TODO(#2671): Update all person attributes below to use complete entity
    # equality instead of just comparing individual fields.
    if isinstance(ingested_entity, schema.StatePersonExternalId):
        db_entity = cast(schema.StatePersonExternalId, db_entity)
        return ingested_entity.external_id == db_entity.external_id \
            and ingested_entity.id_type == db_entity.id_type

    # As person has already been matched, assume that any of these 'person
    # attribute' entities are matches if specific attributes match.
    if isinstance(ingested_entity, schema.StatePersonAlias):
        db_entity = cast(schema.StatePersonAlias, db_entity)
        return ingested_entity.full_name == db_entity.full_name
    if isinstance(ingested_entity, schema.StatePersonRace):
        db_entity = cast(schema.StatePersonRace, db_entity)
        return ingested_entity.race == db_entity.race
    if isinstance(ingested_entity, schema.StatePersonEthnicity):
        db_entity = cast(schema.StatePersonEthnicity, db_entity)
        return ingested_entity.ethnicity == db_entity.ethnicity

    if isinstance(ingested_entity,
                  (schema.StateSupervisionViolationResponseDecisionEntry,
                   schema.StateSupervisionViolatedConditionEntry,
                   schema.StateSupervisionViolationTypeEntry,
                   schema.StateSupervisionCaseTypeEntry)):
        return _base_entity_match(ingested_entity, db_entity, skip_fields=set())

    # Placeholders entities are considered equal
    if ingested_entity.get_external_id() is None \
            and db_entity.get_external_id() is None:
        return is_placeholder(ingested_entity) and is_placeholder(db_entity)
    return ingested_entity.get_external_id() == db_entity.get_external_id()


def nonnull_fields_entity_match(
        ingested_entity: EntityTree,
        db_entity: EntityTree,
        skip_fields: Optional[Set[str]] = None) -> bool:
    """
    Matching logic for comparing entities that might not have external ids, by
    comparing all flat fields in the given entities. Should only be used for
    entities that we know might not have external_ids based on the ingested
    state data.
    """
    if skip_fields is None:
        skip_fields = set()

    a = ingested_entity.entity
    b = db_entity.entity
    return _base_entity_match(a, b, skip_fields=skip_fields, allow_null_mismatch=True)


def _base_entity_match(
        a: DatabaseEntity,
        b: DatabaseEntity,
        skip_fields: Set[str],
        allow_null_mismatch: bool = False
) -> bool:
    """Returns whether two objects of the same type are an entity match.

    Args:
        a: The first entity to match
        b: The second entity to match
        skip_fields: A list of names of fields that should be ignored when determining if two objects match based on
            flat fields.
        allow_null_mismatch: Allow for two objects to still match if one has a null value in a field where the other's
            is nonnull.
    """

    # Placeholders never match
    if is_placeholder(a) or is_placeholder(b):
        return False

    # Compare external ids if one is present
    if a.get_external_id() or b.get_external_id():
        return a.get_external_id() == b.get_external_id()

    # Compare all flat fields of the two entities
    all_set_flat_field_names = \
        get_set_entity_field_names(a, EntityFieldType.FLAT_FIELD) | \
        get_set_entity_field_names(b, EntityFieldType.FLAT_FIELD)
    for field_name in all_set_flat_field_names:
        # Skip primary key
        if field_name == a.get_class_id_name() or field_name in skip_fields:
            continue
        a_field = a.get_field(field_name)
        b_field = b.get_field(field_name)

        if allow_null_mismatch and (a_field is None or b_field is None):
            # Do not disqualify a match if one of the fields is null
            continue

        if a_field != b_field:
            return False

    return True


def generate_child_entity_trees(
        child_field_name: str, entity_trees: List[EntityTree]
) -> List[EntityTree]:
    """Generates a new EntityTree object for each found child of the provided
    |entity_trees| with the field name |child_field_name|.
    """
    child_trees = []
    for entity_tree in entity_trees:
        child_field = entity_tree.entity.get_field(child_field_name)

        # If child_field is unset, skip
        if not child_field:
            continue
        if isinstance(child_field, list):
            children = child_field
        else:
            children = [child_field]
        child_trees.extend(entity_tree.generate_child_trees(children))
    return child_trees


def is_multiple_id_entity(entity: DatabaseEntity):
    """Returns True if the given entity can have multiple external ids."""
    return entity.__class__ in get_multiple_id_classes()


def get_multiple_id_classes() -> List[Type[DatabaseEntity]]:
    """Returns a list of all classes that have multiple external ids."""
    to_return: List[Type[DatabaseEntity]] = []
    for cls in get_non_history_state_database_entities():
        if 'external_ids' in cls.get_relationship_property_names():
            if cls != schema.StatePerson:
                raise ValueError(
                    f"Untested multiple id class. To remove this, please add "
                    f"test coverage to ensure entities of class {cls} are "
                    f"merged properly when there are 2 ingested entities and "
                    f"when there are 2 database entities. See tests for state"
                    f"person for examples.")
            to_return.append(cls)
    return to_return


def get_external_id_keys_from_multiple_id_entity(
        entity: DatabaseEntity) -> List[str]:
    """Returns a list of strings that uniquely represent all external ids
    on the given entity.
    """
    external_str_ids = []
    for external_id in entity.get_field_as_list('external_ids'):
        str_id = external_id.id_type + '|' + external_id.external_id
        external_str_ids.append(str_id)
    return external_str_ids


def remove_child_from_entity(
        *,
        entity: DatabaseEntity,
        child_field_name: str,
        child_to_remove: DatabaseEntity):
    """If present, removes the |child_to_remove| from the |child_field_name|
    field on the |entity|.
    """

    child_field = entity.get_field(child_field_name)

    if isinstance(child_field, list):
        if child_to_remove in child_field:
            child_field.remove(child_to_remove)
    elif isinstance(child_field, DatabaseEntity):
        if child_field == child_to_remove:
            child_field = None
    entity.set_field(child_field_name, child_field)


def add_child_to_entity(
        *,
        entity: DatabaseEntity,
        child_field_name: str,
        child_to_add: DatabaseEntity):
    """Adds the |child_to_add| to the |child_field_name| field on the
    |entity|.
    """

    child_field = entity.get_field(child_field_name)

    if isinstance(child_field, list):
        if child_to_add not in child_field:
            child_field.append(child_to_add)
    else:
        if child_field and child_field != child_to_add:
            raise EntityMatchingError(
                f"Attempting to add child [{child_to_add}] to entity [{entity}], but field [{child_field_name}] "
                f"already had different value [{child_field}]", entity.get_entity_name())
        child_field = child_to_add
        entity.set_field(child_field_name, child_field)


# TODO(#2244): Create general approach for required fields/default values
def convert_to_placeholder(entity: DatabaseEntity):
    for field_name in get_set_entity_field_names(entity, EntityFieldType.FLAT_FIELD):
        if field_name == entity.get_class_id_name():
            continue
        if field_name == 'state_code':
            continue
        if field_name == 'status':
            entity.set_field(field_name, enum_canonical_strings.present_without_info)
            continue
        if field_name == 'incarceration_type':
            entity.set_field(field_name, StateIncarcerationType.STATE_PRISON.value)
            continue
        if field_name == 'court_type':
            entity.set_field(field_name, StateCourtType.PRESENT_WITHOUT_INFO.value)
            continue
        if field_name == 'agent_type':
            entity.set_field(field_name, StateAgentType.PRESENT_WITHOUT_INFO.value)
            continue
        entity.clear_field(field_name)


# TODO(#2244): Create general approach for required fields/default values
def default_merge_flat_fields(
        *, new_entity: DatabaseEntity, old_entity: DatabaseEntity) -> DatabaseEntity:
    """Merges all set non-relationship fields on the |new_entity| onto the |old_entity|. Returns the newly merged
    entity."""
    for child_field_name in get_set_entity_field_names(new_entity, EntityFieldType.FLAT_FIELD):
        if child_field_name == old_entity.get_class_id_name():
            continue
        # Do not overwrite with default status
        if child_field_name == 'status' and new_entity.has_default_status():
            continue

        old_entity.set_field(child_field_name, new_entity.get_field(child_field_name))

    return old_entity


def get_total_entities_of_cls(
        persons: List[schema.StatePerson], cls: Type) -> int:
    """Counts the total number of unique objects of type |cls| in the entity
    graphs passed in by |persons|.
    """
    check_all_objs_have_type(persons, schema.StatePerson)
    return len(get_all_entities_of_cls(persons, cls))


def get_external_ids_of_cls(persons: List[schema.StatePerson],
                            cls: Type[DatabaseEntity]) -> Set[str]:
    """Returns the external ids of all entities of type |cls| found in the
    provided |persons| trees.
    """
    check_all_objs_have_type(persons, schema.StatePerson)

    ids: Set[str] = set()
    entities = get_all_entities_of_cls(persons, cls)
    for entity in entities:
        external_ids = get_external_ids_from_entity(entity)
        if not external_ids:
            raise EntityMatchingError(
                f'Expected external_ids to be non-empty for entity [{entity}] with class [{cls.__name__}]',
                entity.get_class_id_name())
        ids.update(external_ids)
    return ids


def get_external_ids_from_entity(entity: DatabaseEntity):
    external_ids = []
    if isinstance(entity, schema.StatePerson):
        for external_id in entity.external_ids:
            if external_id:
                external_ids.append(external_id.external_id)
    else:
        if entity.get_external_id():
            external_ids.append(entity.get_external_id())
    return external_ids


def get_multiparent_classes() -> List[Type[DatabaseEntity]]:
    cls_list: List[Type[DatabaseEntity]] = [
        schema.StateCharge,
        schema.StateCourtCase,
        schema.StateIncarcerationPeriod,
        schema.StateParoleDecision,
        schema.StateSupervisionPeriod,
        schema.StateSupervisionContact,
        schema.StateSupervisionViolation,
        schema.StateSupervisionViolationResponse,
        schema.StateProgramAssignment,
        schema.StateAgent]
    direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
    direction_checker.assert_sorted(cls_list)
    return cls_list


def get_all_entities_of_cls(sources: Sequence[DatabaseEntity], cls: Type[DatabaseEntity]):
    """Returns all entities found in the provided |sources| of type |cls|."""
    seen_entities: List[DatabaseEntity] = []
    for tree in get_all_entity_trees_of_cls(sources, cls):
        seen_entities.append(tree.entity)
    return seen_entities


def get_all_entity_trees_of_cls(sources: Sequence[DatabaseEntity], cls: Type[DatabaseEntity]) -> List[EntityTree]:
    """Finds all unique entities of type |cls| in the provided |sources|, and returns their corresponding EntityTrees.
    """
    seen_ids: Set[int] = set()
    seen_trees: List[EntityTree] = []
    direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
    for source in sources:
        tree = EntityTree(entity=source, ancestor_chain=[])
        _get_all_entity_trees_of_cls_helper(tree, cls, seen_ids, seen_trees, direction_checker)
    return seen_trees


def _get_all_entity_trees_of_cls_helper(
        tree: EntityTree,
        cls: Type[DatabaseEntity],
        seen_ids: Set[int],
        seen_trees: List[EntityTree],
        direction_checker: SchemaEdgeDirectionChecker):
    """
    Finds all objects in the provided |tree| graph which have the type |cls|. When an object of type |cls| is found,
    updates the provided |seen_ids| and |seen_trees| with the object's id and EntityTree respectively.
    """
    entity = tree.entity
    entity_cls = entity.__class__

    # If |cls| is higher ranked than |entity_cls|, it is impossible to reach an object of type |cls| from the current
    # entity.
    if direction_checker.is_higher_ranked(cls, entity_cls):
        return
    if entity_cls == cls and id(entity) not in seen_ids:
        seen_ids.add(id(entity))
        seen_trees.append(tree)
        return
    for child_field_name in get_set_entity_field_names(entity, EntityFieldType.FORWARD_EDGE):
        child_trees = tree.generate_child_trees(entity.get_field_as_list(child_field_name))
        for child_tree in child_trees:
            _get_all_entity_trees_of_cls_helper(child_tree, cls, seen_ids, seen_trees, direction_checker)


def get_root_entity_cls(
        ingested_persons: List[schema.StatePerson]) -> Type[DatabaseEntity]:
    """
    Attempts to find the highest entity class within the |ingested_persons| for
    which objects are not placeholders. Returns the class if found, otherwise
    raises.

    Note: This should only be used with persons ingested from a region directly
    (and not with persons post entity matching), as this function uses DFS to
    find the root entity cls. This therefore assumes that a) the passed in
    StatePersons are trees and not DAGs (one parent per entity) and b) that the
    structure of the passed in graph is symmetrical.
    """
    check_all_objs_have_type(ingested_persons, schema.StatePerson)

    root_cls = None
    if ingested_persons:
        root_cls = _get_root_entity_helper(ingested_persons[0])
    if root_cls is None:
        raise EntityMatchingError(
            "Could not find root class for ingested persons", 'state_person')
    return root_cls


def _get_root_entity_helper(
        entity: DatabaseEntity) -> Optional[Type[DatabaseEntity]]:
    if not is_placeholder(entity):
        return entity.__class__

    for field_name in get_set_entity_field_names(
            entity, EntityFieldType.FORWARD_EDGE):
        field = entity.get_field_as_list(field_name)[0]
        result = _get_root_entity_helper(field)
        if result is not None:
            return result
    return None


def db_id_or_object_id(entity: DatabaseEntity) -> int:
    """If present, returns the primary key field from the provided |entity|,
    otherwise provides the object id.
    """
    return entity.get_id() if entity.get_id() else id(entity)


def read_persons(
        session: Session,
        region: str,
        ingested_people: List[schema.StatePerson]
) -> List[schema.StatePerson]:
    """Looks up all people necessary for entity matching based on the provided
    |region| and |ingested_people|.
    """
    check_all_objs_have_type(ingested_people, schema.StatePerson)

    # TODO(#1868): more specific query
    db_people = dao.read_people(session)
    logging.info("Read [%d] people from DB in region [%s]",
                 len(db_people), region)
    return db_people


def read_db_entity_trees_of_cls_to_merge(
        session: Session,
        state_code: str,
        schema_cls: Type[StateBase]
) -> List[List[EntityTree]]:
    """
    Returns a list of lists of EntityTree where each inner list is a group
    of EntityTrees with entities of class |schema_cls| that need to be merged
    because their entities have the same external_id.

    Will assert if schema_cls does not have a person_id or external_id field.
    """
    external_ids = dao.read_external_ids_of_cls_with_external_id_match(
        session, state_code, schema_cls)
    people = dao.read_people_by_cls_external_ids(
        session, state_code, schema_cls, external_ids)
    all_cls_trees = get_all_entity_trees_of_cls(people, schema_cls)

    external_ids_map: Dict[str, List[EntityTree]] = defaultdict(list)
    for tree in all_cls_trees:
        if not isinstance(tree.entity, schema_cls):
            raise ValueError(f'Unexpected entity type [{type(tree.entity)}]')

        if tree.entity.external_id in external_ids:
            external_ids_map[tree.entity.external_id].append(tree)

    return [tree_list for _, tree_list in external_ids_map.items()]


def read_persons_by_root_entity_cls(
        session: Session,
        region: str,
        ingested_people: List[schema.StatePerson],
        allowed_root_entity_classes: Optional[List[Type[DatabaseEntity]]],
) -> List[schema.StatePerson]:
    """Looks up all people necessary for entity matching based on the provided
    |region| and |ingested_people|.

    If |allowed_root_entity_classes| is provided, throw an error if any
    unexpected root entity class is found.
    """
    root_entity_cls = get_root_entity_cls(ingested_people)
    if allowed_root_entity_classes and root_entity_cls not in allowed_root_entity_classes:
        raise ValueError(f'For region [{region}] found unexpected root_entity_cls: [{root_entity_cls.__name__}]. '
                         f'Allowed classes: [{allowed_root_entity_classes}]')
    root_external_ids = get_external_ids_of_cls(
        ingested_people, root_entity_cls)
    logging.info("[Entity Matching] Reading [%s] external ids of class [%s]",
                 len(root_external_ids), root_entity_cls.__name__)
    persons_by_root_entity = dao.read_people_by_cls_external_ids(
        session, region, root_entity_cls, root_external_ids)
    placeholder_persons = dao.read_placeholder_persons(session, region)

    # When the |root_entity_cls| is not StatePerson, it is possible for both
    # persons_by_root_entity and placeholder_persons to contain the same
    # placeholder person(s). For this reason, we dedup people across both lists
    # before returning.
    deduped_people = []
    seen_person_ids: Set[int] = set()
    for person in persons_by_root_entity + placeholder_persons:
        if person.person_id not in seen_person_ids:
            deduped_people.append(person)
            seen_person_ids.add(person.person_id)

    return deduped_people


def get_or_create_placeholder_child(
        parent_entity: DatabaseEntity, child_field_name: str, child_class: Type[DatabaseEntity], **child_kwargs):
    """Checks all the entities in the |parent_entity|'s field |child_field_name|. If there is a placeholder entity,
    returns that. Otherwise creates a new placeholder entity of type |child_class| on the parent's |child_field_name|
    using |child_kw_args|.
    """
    children = parent_entity.get_field_as_list(child_field_name)
    placeholder_children = [c for c in children if is_placeholder(c)]

    if placeholder_children:
        return placeholder_children[0]

    logging.info(
        'No placeholder children on entity with id [%s] of type [%s] exist on field [%s]. Have to create one.',
        parent_entity.get_external_id(), parent_entity.get_entity_name(), child_field_name)
    new_child = child_class(**child_kwargs)
    if not is_placeholder(new_child):
        raise EntityMatchingError(
            f'Child created with kwargs is not a placeholder [{child_kwargs}]. Parent entity: [{parent_entity}]. Child '
            f'field name: [{child_field_name}]. Child class: [{child_class}].', parent_entity.get_entity_name())

    children.append(new_child)
    parent_entity.set_field_from_list(child_field_name, children)
    return new_child
