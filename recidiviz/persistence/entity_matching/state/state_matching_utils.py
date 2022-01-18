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
from enum import Enum
from typing import List, Optional, Sequence, Set, Type, cast

from recidiviz.common.common_utils import check_all_objs_have_type
from recidiviz.common.constants import enum_canonical_strings
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_court_case import StateCourtType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import dao, schema
from recidiviz.persistence.database.schema_utils import (
    get_non_history_state_database_entities,
)
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.base_entity import EnumEntity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    EntityFieldType,
    SchemaEdgeDirectionChecker,
    get_explicilty_set_flat_fields,
    is_placeholder,
    is_reference_only_entity,
)
from recidiviz.persistence.entity_matching.entity_matching_types import EntityTree
from recidiviz.persistence.errors import EntityMatchingError


def is_match(
    ingested_entity: EntityTree,
    db_entity: EntityTree,
    field_index: CoreEntityFieldIndex,
) -> bool:
    """Returns true if the provided |ingested_entity| matches the provided
    |db_entity|. Otherwise returns False.
    """
    return _is_match(
        ingested_entity=ingested_entity.entity,
        db_entity=db_entity.entity,
        field_index=field_index,
    )


def _is_match(
    *,
    ingested_entity: DatabaseEntity,
    db_entity: DatabaseEntity,
    field_index: CoreEntityFieldIndex,
) -> bool:
    """Returns true if the provided |ingested_entity| matches the provided
    |db_entity|. Otherwise returns False.
    """
    if not ingested_entity or not db_entity:
        return ingested_entity == db_entity

    if ingested_entity.__class__ != db_entity.__class__:
        raise ValueError(
            f"is_match received entities of two different classes: ingested entity [{ingested_entity}] and db_entity "
            f"[{db_entity}]"
        )

    if not isinstance(ingested_entity, DatabaseEntity):
        raise ValueError(
            f"Unexpected type for ingested entity [{ingested_entity}]: [{type(ingested_entity)}]"
        )
    if not isinstance(db_entity, DatabaseEntity):
        raise ValueError(
            f"Unexpected type for db entity [{db_entity}]: [{type(db_entity)}]"
        )

    if isinstance(ingested_entity, schema.StatePerson):
        db_entity = cast(schema.StatePerson, db_entity)
        for ingested_external_id in ingested_entity.external_ids:
            for db_external_id in db_entity.external_ids:
                if _is_match(
                    ingested_entity=ingested_external_id,
                    db_entity=db_external_id,
                    field_index=field_index,
                ):
                    return True
        return False

    # Aside from people, all entities are state specific.
    if ingested_entity.get_field("state_code") != db_entity.get_field("state_code"):
        return False

    # TODO(#2671): Update all person attributes below to use complete entity
    # equality instead of just comparing individual fields.
    if isinstance(ingested_entity, schema.StatePersonExternalId):
        db_entity = cast(schema.StatePersonExternalId, db_entity)
        return (
            ingested_entity.external_id == db_entity.external_id
            and ingested_entity.id_type == db_entity.id_type
        )

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

    if isinstance(
        ingested_entity,
        (
            schema.StateSupervisionViolationResponseDecisionEntry,
            schema.StateSupervisionViolatedConditionEntry,
            schema.StateSupervisionViolationTypeEntry,
            schema.StateSupervisionCaseTypeEntry,
        ),
    ):
        return _base_entity_match(
            ingested_entity, db_entity, skip_fields=set(), field_index=field_index
        )

    # Placeholders entities are considered equal
    if (
        ingested_entity.get_external_id() is None
        and db_entity.get_external_id() is None
    ):
        return is_placeholder(ingested_entity, field_index) and is_placeholder(
            db_entity, field_index
        )
    return ingested_entity.get_external_id() == db_entity.get_external_id()


def nonnull_fields_entity_match(
    ingested_entity: EntityTree,
    db_entity: EntityTree,
    field_index: CoreEntityFieldIndex,
    skip_fields: Optional[Set[str]] = None,
) -> bool:
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
    return _base_entity_match(
        a, b, skip_fields=skip_fields, allow_null_mismatch=True, field_index=field_index
    )


def _base_entity_match(
    a: DatabaseEntity,
    b: DatabaseEntity,
    skip_fields: Set[str],
    field_index: CoreEntityFieldIndex,
    allow_null_mismatch: bool = False,
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
    if is_placeholder(a, field_index) or is_placeholder(b, field_index):
        return False

    # Compare external ids if one is present
    if a.get_external_id() or b.get_external_id():
        return a.get_external_id() == b.get_external_id()

    # Compare all flat fields of the two entities
    all_set_flat_field_names = field_index.get_fields_with_non_empty_values(
        a, EntityFieldType.FLAT_FIELD
    ) | field_index.get_fields_with_non_empty_values(b, EntityFieldType.FLAT_FIELD)
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


def is_multiple_id_entity(entity: DatabaseEntity) -> bool:
    """Returns True if the given entity can have multiple external ids."""
    return entity.__class__ in get_multiple_id_classes()


def get_multiple_id_classes() -> List[Type[DatabaseEntity]]:
    """Returns a list of all classes that have multiple external ids."""
    to_return: List[Type[DatabaseEntity]] = []
    for cls in get_non_history_state_database_entities():
        if "external_ids" in cls.get_relationship_property_names():
            if cls != schema.StatePerson:
                raise ValueError(
                    f"Untested multiple id class. To remove this, please add "
                    f"test coverage to ensure entities of class {cls} are "
                    f"merged properly when there are 2 ingested entities and "
                    f"when there are 2 database entities. See tests for state"
                    f"person for examples."
                )
            to_return.append(cls)
    return to_return


def remove_child_from_entity(
    *, entity: DatabaseEntity, child_field_name: str, child_to_remove: DatabaseEntity
) -> None:
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
    *, entity: DatabaseEntity, child_field_name: str, child_to_add: DatabaseEntity
) -> None:
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
                f"already had different value [{child_field}]",
                entity.get_entity_name(),
            )
        child_field = child_to_add
        entity.set_field(child_field_name, child_field)


# TODO(#2244): Create general approach for required fields/default values
def convert_to_placeholder(
    entity: DatabaseEntity, field_index: CoreEntityFieldIndex
) -> None:
    for field_name in field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FLAT_FIELD
    ):
        if field_name == entity.get_class_id_name():
            continue
        if field_name == "state_code":
            continue
        if field_name == "status":
            entity.set_field(field_name, enum_canonical_strings.present_without_info)
            continue
        if field_name == "incarceration_type":
            entity.set_field(field_name, StateIncarcerationType.STATE_PRISON.value)
            continue
        if field_name == "court_type":
            entity.set_field(field_name, StateCourtType.PRESENT_WITHOUT_INFO.value)
            continue
        if field_name == "agent_type":
            entity.set_field(field_name, StateAgentType.PRESENT_WITHOUT_INFO.value)
            continue
        entity.clear_field(field_name)


def can_atomically_merge_entity(
    new_entity: DatabaseEntity, field_index: CoreEntityFieldIndex
) -> bool:
    """If True, then when merging this entity onto an existing database entity, all the
    values from this new entity should be taken wholesale, even if some of those values
    are null.
    """

    # We often get data about a person from multiple sources - do not overwrite person
    # information with new updates to the person.
    if isinstance(new_entity, schema.StatePerson):
        return False

    # If this entity only contains external id information - do not merge atomically
    # onto database.
    if is_reference_only_entity(new_entity, field_index) or is_placeholder(
        new_entity, field_index
    ):
        return False

    # Enum-like objects with no external id should always be merged atomically.
    if not hasattr(new_entity, "external_id"):
        return True

    # Entities were added to this list by default, with exemptions (below) selected
    # explicitly where issues arose because we were writing to them from multiple
    # sources.
    if isinstance(
        new_entity,
        (
            schema.StateAssessment,
            schema.StateCharge,
            schema.StateCourtCase,
            schema.StateIncarcerationIncident,
            schema.StateIncarcerationIncidentOutcome,
            schema.StateIncarcerationPeriod,
            schema.StatePersonExternalId,
            schema.StateProgramAssignment,
            schema.StateSupervisionContact,
            schema.StateSupervisionCaseTypeEntry,
            schema.StateSupervisionPeriod,
            schema.StateSupervisionSentence,
            schema.StateSupervisionViolation,
            schema.StateSupervisionViolationResponse,
        ),
    ):
        return True

    if isinstance(
        new_entity,
        (
            schema.StateAgent,
            # We update this entity from multiple views in ND
            schema.StateIncarcerationSentence,
            # We update this entity from four views in US_ID:
            # early_discharge_supervision_sentence
            # early_discharge_incarceration_sentence
            # early_discharge_supervision_sentence_deleted_rows
            # early_discharge_incarceration_sentence_deleted_rows
            schema.StateEarlyDischarge,
        ),
    ):
        return False

    raise ValueError(
        f"Cannot determine whether we can merge entity [{new_entity}] atomically "
        f"onto the existing database match."
    )


# TODO(#2244): Create general approach for required fields/default values
def merge_flat_fields(
    *,
    new_entity: DatabaseEntity,
    old_entity: DatabaseEntity,
    field_index: CoreEntityFieldIndex,
) -> DatabaseEntity:
    """Merges all set non-relationship fields on the |new_entity| onto the |old_entity|.
    Returns the newly merged entity."""

    if can_atomically_merge_entity(new_entity, field_index):
        fields = field_index.get_all_core_entity_fields(
            new_entity, EntityFieldType.FLAT_FIELD
        )
    else:
        fields = get_explicilty_set_flat_fields(new_entity, field_index)

        # If an enum field is updated, always update the corresponding raw text field
        # (and vice versa), even if one of the values is null.
        new_fields = set()
        for field_name in fields:
            if isinstance(new_entity.get_field(field_name), Enum):
                new_fields.add(f"{field_name}{EnumEntity.RAW_TEXT_FIELD_SUFFIX}")
            if field_name.endswith(EnumEntity.RAW_TEXT_FIELD_SUFFIX):
                new_fields.add(field_name[: -len(EnumEntity.RAW_TEXT_FIELD_SUFFIX)])
        fields.update(new_fields)

    for child_field_name in fields:
        if child_field_name == old_entity.get_class_id_name():
            continue

        old_entity.set_field(child_field_name, new_entity.get_field(child_field_name))

    return old_entity


def get_all_person_external_ids(
    persons: List[schema.StatePerson],
) -> Set[str]:
    """Returns the external ids of all entities of type |cls| found in the
    provided |persons| trees.
    """
    check_all_objs_have_type(persons, schema.StatePerson)

    ids: Set[str] = set()
    for person in persons:
        external_ids = get_person_external_ids(person)
        ids.update(external_ids)
    return ids


def get_person_external_ids(db_person: schema.StatePerson) -> List[str]:
    external_ids = []
    if not db_person.external_ids:
        raise EntityMatchingError(
            f"Expected external_ids to be non-empty for [{db_person}]",
            db_person.get_class_id_name(),
        )
    for external_id in db_person.external_ids:
        external_ids.append(external_id.external_id)

    return external_ids


def get_multiparent_classes() -> List[Type[DatabaseEntity]]:
    cls_list: List[Type[DatabaseEntity]] = [
        schema.StateCharge,
        schema.StateCourtCase,
        schema.StateAgent,
    ]
    direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
    direction_checker.assert_sorted(cls_list)
    return cls_list


def get_all_entities_of_cls(
    sources: Sequence[DatabaseEntity],
    cls: Type[DatabaseEntity],
    field_index: CoreEntityFieldIndex,
) -> List[DatabaseEntity]:
    """Returns all entities found in the provided |sources| of type |cls|."""
    seen_entities: List[DatabaseEntity] = []
    for tree in get_all_entity_trees_of_cls(sources, cls, field_index):
        seen_entities.append(tree.entity)
    return seen_entities


def get_all_entity_trees_of_cls(
    sources: Sequence[DatabaseEntity],
    cls: Type[DatabaseEntity],
    field_index: CoreEntityFieldIndex,
) -> List[EntityTree]:
    """Finds all unique entities of type |cls| in the provided |sources|,
    and returns their corresponding EntityTrees."""
    seen_ids: Set[int] = set()
    seen_trees: List[EntityTree] = []
    direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
    for source in sources:
        tree = EntityTree(entity=source, ancestor_chain=[])
        _get_all_entity_trees_of_cls_helper(
            tree, cls, seen_ids, seen_trees, direction_checker, field_index
        )
    return seen_trees


def _get_all_entity_trees_of_cls_helper(
    tree: EntityTree,
    cls: Type[DatabaseEntity],
    seen_ids: Set[int],
    seen_trees: List[EntityTree],
    direction_checker: SchemaEdgeDirectionChecker,
    field_index: CoreEntityFieldIndex,
) -> None:
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
    for child_field_name in field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FORWARD_EDGE
    ):
        child_trees = tree.generate_child_trees(
            entity.get_field_as_list(child_field_name)
        )
        for child_tree in child_trees:
            _get_all_entity_trees_of_cls_helper(
                child_tree, cls, seen_ids, seen_trees, direction_checker, field_index
            )


def db_id_or_object_id(entity: DatabaseEntity) -> int:
    """If present, returns the primary key field from the provided |entity|,
    otherwise provides the object id.
    """
    return entity.get_id() if entity.get_id() else id(entity)


def read_potential_match_db_persons(
    session: Session,
    region_code: str,
    ingested_persons: List[schema.StatePerson],
) -> List[schema.StatePerson]:
    """Reads and returns all persons from the DB that are needed for entity matching,
    given the |ingested_persons|.
    """
    root_external_ids = get_all_person_external_ids(ingested_persons)
    logging.info(
        "[Entity Matching] Reading entities of class schema.StatePerson using [%s] "
        "external ids",
        len(root_external_ids),
    )
    persons_by_root_entity = dao.read_people_by_cls_external_ids(
        session, region_code, schema.StatePerson, root_external_ids
    )

    deduped_people = []
    seen_person_ids: Set[int] = set()
    for person in persons_by_root_entity:
        if person.person_id not in seen_person_ids:
            deduped_people.append(person)
            seen_person_ids.add(person.person_id)

    return deduped_people
