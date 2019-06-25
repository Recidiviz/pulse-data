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

"""Contains logic to match state-level database entities with state-level
ingested entities.
"""
import logging

from typing import List, Dict, Optional, cast, Sequence, Mapping

from recidiviz import Session
from recidiviz.persistence.entity.entities import EntityPersonType
from recidiviz.persistence.entity.entity_utils import \
     EntityFieldType, get_set_entity_field_names
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.database.schema.state import dao
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import StatePerson
from recidiviz.persistence.entity_matching import entity_matching_utils
from recidiviz.persistence.entity_matching.base_entity_matcher import \
    BaseEntityMatcher, MatchedEntities, increment_error
from recidiviz.persistence.entity_matching.state import state_matching_utils
from recidiviz.persistence.entity_matching.state.state_matching_utils import \
    merge_flat_fields, remove_back_edges, \
    add_person_to_entity_graph

from recidiviz.persistence.errors import EntityMatchingError, \
    MatchedMultipleIngestedEntitiesError


class StateEntityMatcher(BaseEntityMatcher[entities.StatePerson]):
    """Base class for all entity matchers."""

    # TODO(1907): Rename base class variable to ingested_persons
    def run_match(self, session: Session, region: str,
                  ingested_people: List[EntityPersonType]) \
            -> MatchedEntities:
        """Attempts to match all persons from |ingested_persons| with
        corresponding persons in our database for the given |region|. Returns a
        MatchedEntities object that contains the results of matching.
        """
        ingested_persons = ingested_people
        merged_persons: List[StatePerson] = []
        error_count = 0

        # TODO(1868): more specific query
        db_persons = dao.read_people(session)

        # Remove all back edges before entity matching. All entities in the
        # state schema have edges both to their children and their parents. We
        # remove these for simplicity as entity matching does not depend on
        # these parent references (back edges), and as SqlAlchemy can infer a
        # child->parent edge from the existence of the parent->child edge. If
        # we did not remove these back edges, any time an entity relationship
        # changes, we would have to update edges both on the parent and child,
        # instead of just on the parent.
        #
        # The one type of back edge that is not bidirectional, and therefore
        # will not be automatically populated on write are the person references
        # on all entities in the graph that are not direct children of the
        # person. To preserve those references on write, we manually add them
        # after entity matching is complete (see add_person_to_entity_graph)
        for db_person in db_persons:
            remove_back_edges(db_person)

        matched_persons_by_db_ids: Dict[int, StatePerson] = {}
        for ingested_person in ingested_persons:
            try:
                merged_person = _match_entity(
                    ingested_entity=ingested_person, db_entities=db_persons,
                    matched_entities_by_db_ids=matched_persons_by_db_ids)
                merged_person = cast(StatePerson, merged_person)
                merged_persons.append(merged_person)
            except EntityMatchingError as e:
                logging.exception(
                    "Found error while matching ingested person. \nPerson: %s",
                    ingested_person)
                increment_error(e.entity_name)
                error_count += 1
        # TODO(1868): Merge entities that could have been created twice
        # (those with multiple parent branches)
        add_person_to_entity_graph(merged_persons)
        return MatchedEntities(people=merged_persons, error_count=error_count)


def _match_entities(
        *, ingested_entities: Sequence[Entity],
        db_entities: Sequence[Entity]) -> List[Entity]:
    """Attempts to match all entities from |ingested_entities| with one entity
    in the provided |db_entities|. For all ingested_entities with a matching
    db_entity, merges the ingested_entity information into the db_entity, and
    continues entity matching for all children entities.

    Returns a list containing all merged and new entities.
    """
    merged_entities = []
    matched_entities_by_db_id: Dict[int, Entity] = {}

    for ingested_entity in ingested_entities:
        merged_db_entity = _match_entity(
            ingested_entity=ingested_entity,
            db_entities=db_entities,
            matched_entities_by_db_ids=matched_entities_by_db_id)

        merged_entities.append(merged_db_entity)

    # Keep track of even untouched DB entities, as the parent of this entity
    # layer must know about all of its children (even the untouched ones). If
    # we exclude the untouched database entities from this list, on write,
    # SQLAlchemy will treat the incomplete child list as an update, and attempt
    # to remove any children with links to the parent in our database, but not
    # in the provided list.
    for db_entity in db_entities:
        if db_entity.get_id() not in matched_entities_by_db_id.keys():
            merged_entities.append(db_entity)

    return merged_entities


def _match_entity(
        *,
        ingested_entity: Entity, db_entities: Sequence[Entity],
        matched_entities_by_db_ids: Mapping[int, Entity]) -> Entity:
    """
    Attempts to match the provided |ingested_entity| to one of the provided
    |database_entities|. If a successful match is found, merges the
    |ingested_entity| onto the matching database entity, performs entity
    matching on all children of the ingested and db entities, and returns the
    merged entity. If no match is found, the provided |ingested_entity| is
    returned.
    """
    matched_entities_by_db_ids = cast(Dict[int, Entity],
                                      matched_entities_by_db_ids)
    # Currently only support one to one matches, but could transition to
    # get_best_match
    db_match = entity_matching_utils.get_only_match(
        ingested_entity, db_entities, state_matching_utils.is_match)
    if not db_match:
        return ingested_entity

    matched_db_id = db_match.get_id()
    if matched_db_id in matched_entities_by_db_ids:
        matches = [ingested_entity, matched_entities_by_db_ids[matched_db_id]]
        raise MatchedMultipleIngestedEntitiesError(db_match, matches)
    matched_entities_by_db_ids[matched_db_id] = ingested_entity

    return _merge_entities(ingested_entity=ingested_entity, db_match=db_match)


def _merge_entities(
        *, ingested_entity: Entity, db_match: Entity) -> Entity:
    """ Given an |ingested_entity| and its corresponding |db_match|, this method
    merges all flat fields from the ingested entity onto the db_entity and
    continues entity matching for all children of provided objects.

    Returns a merged_entity, which is the |db_match| object updated with all
    the new information from the |ingested_entity|.
    """
    child_field_names = \
        get_set_entity_field_names(db_match, EntityFieldType.FORWARD_EDGE) \
        | get_set_entity_field_names(
            ingested_entity, EntityFieldType.FORWARD_EDGE)

    for child_field_name in child_field_names:
        db_field = getattr(db_match, child_field_name)
        ing_field = getattr(ingested_entity, child_field_name)

        if isinstance(db_field, list):
            setattr(db_match, child_field_name,
                    _match_entities(ingested_entities=ing_field,
                                    db_entities=db_field))
        else:
            setattr(
                db_match, child_field_name,
                _match_entity_one_to_one(
                    ingested_entity=ing_field, db_entity=db_field))

    merged_entity = merge_flat_fields(
        ingested_entity=ingested_entity, db_entity=db_match)
    return merged_entity


def _match_entity_one_to_one(
        *, ingested_entity: Optional[Entity], db_entity: Optional[Entity]) \
        -> Optional[Entity]:
    if not ingested_entity:
        return db_entity
    if not db_entity:
        return ingested_entity

    if state_matching_utils.is_match(
            ingested_entity=ingested_entity, db_entity=db_entity):
        return _merge_entities(
            ingested_entity=ingested_entity, db_match=db_entity)

    raise EntityMatchingError(
        f"Singular entity field should match, but the entities do not."
        f" Ingested {ingested_entity} and found {db_entity} in db",
        ingested_entity.get_entity_name())
