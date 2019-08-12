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

from typing import List, Dict, Tuple, Optional, cast, Type

from more_itertools import one

from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.entity_utils import \
    get_set_entity_field_names, get_field_as_list, get_field, set_field, \
    set_field_from_list, is_placeholder
from recidiviz.persistence.database.schema.state import dao
from recidiviz.persistence.entity.base_entity import Entity, ExternalIdEntity
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StateIncarcerationPeriod, StateCharge, StateCourtCase, StateAgent, \
    StateIncarcerationSentence, StateAssessment, StateSupervisionPeriod, \
    StateSupervisionViolation, StateSupervisionViolationResponse
from recidiviz.persistence.entity_matching import entity_matching_utils
from recidiviz.persistence.entity_matching.base_entity_matcher import \
    BaseEntityMatcher, increment_error
from recidiviz.persistence.entity_matching.state.state_matching_utils import \
    remove_back_edges, \
    add_person_to_entity_graph, EntityFieldType, generate_child_entity_trees, \
    remove_child_from_entity, \
    add_child_to_entity, merge_incarceration_periods, \
    merge_flat_fields, is_match, move_incidents_onto_periods, \
    get_root_entity_cls, get_total_entities_of_cls, \
    associate_revocation_svrs_with_ips, base_entity_match, \
    nd_get_incomplete_incarceration_period_match
from recidiviz.persistence.entity_matching.entity_matching_types import \
    EntityTree, IndividualMatchResult, MatchResults, MatchedEntities

from recidiviz.persistence.errors import EntityMatchingError, \
    MatchedMultipleIngestedEntitiesError


class StateEntityMatcher(BaseEntityMatcher[StatePerson]):
    """Base class for all entity matchers."""

    def run_match(self, session: Session, region: str,
                  ingested_people: List[StatePerson]) \
            -> MatchedEntities:
        """Attempts to match all persons from |ingested_persons| with
        corresponding persons in our database for the given |region|. Returns a
        MatchedEntities object that contains the results of matching.
        """

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
        # will not be automatically populated on write is the person references
        # on all entities in the graph that are not direct children of the
        # person. To preserve those references on write, we manually add them
        # after entity matching is complete (see add_person_to_entity_graph)
        for db_person in db_persons:
            remove_back_edges(db_person)

        matched_entities = _run_match(ingested_people, db_persons)
        add_person_to_entity_graph(matched_entities.people)
        return matched_entities


def _run_match(ingested_persons: List[StatePerson],
               db_persons: List[StatePerson]) -> MatchedEntities:
    """Attempts to match |ingested_persons| with people in |db_persons|.
    Assumes no backedges are present in either |ingested_persons| or
    |db_persons|.
    """
    _perform_preprocessing(ingested_persons)

    matched_entities = _match_persons(
        ingested_persons=ingested_persons, db_persons=db_persons)

    # TODO(1868): Remove any placeholders in graph without children after
    # write
    merge_multiparent_entities(matched_entities.people)
    move_incidents_onto_periods(matched_entities.people)
    associate_revocation_svrs_with_ips(
        matched_entities.people)
    return matched_entities


# TODO(2037): Move state specific logic into its own file.
def _perform_preprocessing(ingested_persons: List[StatePerson]):
    """Performs state specific preprocessing on the provided |ingested_persons|.
    """
    merge_incarceration_periods(ingested_persons)


def _match_persons(
        *, ingested_persons: List[StatePerson], db_persons: List[StatePerson]) \
        -> MatchedEntities:
    """Attempts to match all persons from |ingested_persons| with the provided
    |db_persons|. Results are returned in the MatchedEntities object which
    contains all successfully matched and merged persons as well as an error
    count that is incremented every time an error is raised matching an
    ingested person.
    """
    db_person_trees = [
        EntityTree(entity=db_person, ancestor_chain=[])
        for db_person in db_persons]
    ingested_person_trees = [
        EntityTree(entity=ingested_person, ancestor_chain=[])
        for ingested_person in ingested_persons]

    root_entity_cls = get_root_entity_cls(ingested_persons)
    total_root_entities = get_total_entities_of_cls(
        ingested_persons, root_entity_cls)
    persons_match_results = _match_entity_trees(
        ingested_entity_trees=ingested_person_trees,
        db_entity_trees=db_person_trees,
        root_entity_cls=root_entity_cls)

    updated_persons = []
    for match_result in persons_match_results.individual_match_results:
        if not match_result.merged_entity_trees:
            updated_persons.append(match_result.ingested_entity_tree.entity)
        else:
            # It is possible that multiple ingested people match to the same
            # DB person, in which case we should only keep one reference to
            # that object.
            for merged_person_tree in match_result.merged_entity_trees:
                if merged_person_tree.entity not in updated_persons:
                    updated_persons.append(merged_person_tree.entity)

    # The only database persons that are unmatched that we potentially want to
    # update are placeholder persons. These may have had children removed as
    # a part of the matching process and therefore would need updating.
    for db_person in persons_match_results.unmatched_db_entities:
        if is_placeholder(db_person):
            updated_persons.append(db_person)

    return MatchedEntities(
        people=updated_persons, error_count=persons_match_results.error_count,
        total_root_entities=total_root_entities)


def _match_entity_trees(
        *, ingested_entity_trees: List[EntityTree],
        db_entity_trees: List[EntityTree],
        root_entity_cls: Type) -> MatchResults:
    """Attempts to match all of the |ingested_entity_trees| with one of the
    provided |db_entity_trees|. For all matches, merges the ingested entity
    information into the db entity, and continues entity matching for all
    child entities.

    If the provided |root_entity_cls| corresponds to the class of the provided
    |ingested_entity_trees|, increments an error count rather than raising when
    one is encountered.

    Returns a MatchResults object which contains IndividualMatchResults for each
    ingested tree, a list of unmatched DB entities, and the number of errors
    encountered while matching these trees.
    """
    individual_match_results: List[IndividualMatchResult] = []
    matched_entities_by_db_id: Dict[int, Entity] = {}
    error_count = 0

    for ingested_entity_tree in ingested_entity_trees:
        try:
            match_result = _match_entity_tree(
                ingested_entity_tree=ingested_entity_tree,
                db_entity_trees=db_entity_trees,
                matched_entities_by_db_ids=matched_entities_by_db_id,
                root_entity_cls=root_entity_cls)
            individual_match_results.append(match_result)
            error_count += match_result.error_count
        except EntityMatchingError as e:
            if isinstance(ingested_entity_tree.entity, root_entity_cls):
                ingested_entity = cast(Entity, ingested_entity_tree.entity)
                logging.exception(
                    "Found error while matching ingested %s. \nEntity: %s",
                    ingested_entity.get_entity_name(),
                    ingested_entity)
                increment_error(e.entity_name)
                error_count += 1
            else:
                raise e

    # Keep track of even unmatched DB entities, as the parent of this entity
    # layer must know about all of its children (even the unmatched ones). If
    # we exclude the unmatched database entities from this list, on write,
    # SQLAlchemy will treat the incomplete child list as an update, and attempt
    # to remove any children with links to the parent in our database but not
    # in the provided list.
    unmatched_db_entities: List[Entity] = []
    for db_entity_tree in db_entity_trees:
        db_entity = db_entity_tree.entity
        if db_entity.get_id() not in matched_entities_by_db_id.keys():
            unmatched_db_entities.append(db_entity)

    return MatchResults(
        individual_match_results, unmatched_db_entities, error_count)


def _match_entity_tree(
        *, ingested_entity_tree: EntityTree, db_entity_trees: List[EntityTree],
        matched_entities_by_db_ids: Dict[int, Entity],
        root_entity_cls: Type) -> IndividualMatchResult:
    """Attempts to match the provided |ingested_entity_tree| to one of the
    provided |db_entity_trees|. If a successful match is found, merges the
    ingested entity onto the matching database entity and performs entity
    matching on all children of the matched entities.

    Returns the results of matching as an IndividualMatchResult.
    """

    if is_placeholder(ingested_entity_tree.entity):
        return _match_placeholder_tree(
            ingested_placeholder_tree=ingested_entity_tree,
            db_entity_trees=db_entity_trees,
            matched_entities_by_db_ids=matched_entities_by_db_ids,
            root_entity_cls=root_entity_cls)

    db_match_tree = _get_match(ingested_entity_tree, db_entity_trees)

    if not db_match_tree:
        return _match_unmatched_tree(
            ingested_unmatched_entity_tree=ingested_entity_tree,
            db_entity_trees=db_entity_trees,
            root_entity_cls=root_entity_cls)

    return _match_matched_tree(
        ingested_entity_tree=ingested_entity_tree,
        db_match_tree=db_match_tree,
        matched_entities_by_db_ids=matched_entities_by_db_ids,
        root_entity_cls=root_entity_cls)


def _match_placeholder_tree(
        *, ingested_placeholder_tree: EntityTree,
        db_entity_trees: List[EntityTree],
        matched_entities_by_db_ids: Dict[int, Entity],
        root_entity_cls) \
        -> IndividualMatchResult:
    """Attempts to match the provided |ingested_placeholder_tree| to entities in
    the provided |db_entity_trees| based off any child matches. When such a
    match is found, the child is moved off of the ingested entity and onto the
    matched db entity.

    Returns the results of matching as an IndividualMatchResult.
    """
    updated_entity_trees: List[EntityTree] = []
    error_count = 0
    match_results_by_child = _get_match_results_for_all_children(
        ingested_entity_tree=ingested_placeholder_tree,
        db_entity_trees=db_entity_trees,
        root_entity_cls=root_entity_cls)

    # Initialize so pylint doesn't yell.
    child_field_name = None
    child_match_result = None
    placeholder_children: List[Entity] = []

    def resolve_child_match_result():
        """Resolves any child matches by removing the child from the ingested
        placeholder entity and adding the child onto the corresponding DB
        entity.
        """

        if not child_field_name or not child_match_result:
            raise EntityMatchingError(
                f"Expected child_field_name and child_match_result to be set, "
                f"but instead got {child_field_name} and {child_match_result} "
                f"respectively.",
                ingested_placeholder_tree.entity.get_entity_name())

        # If the child wasn't matched, leave it on the placeholder object.
        if not child_match_result.merged_entity_trees:
            placeholder_children.append(
                child_match_result.ingested_entity_tree.entity)
            return

        # Ensure the merged children are on the correct entity
        for merged_child_tree in child_match_result.merged_entity_trees:
            merged_parent_tree = merged_child_tree.generate_parent_tree()

            # If one of the merged parents is the ingested placeholder entity,
            # simply keep track of the child in placeholder_children.
            if merged_parent_tree.entity == ingested_placeholder_tree.entity:
                placeholder_children.append(
                    child_match_result.ingested_entity_tree.entity)
                continue

            add_child_to_entity(
                entity=merged_parent_tree.entity,
                child_field_name=child_field_name,
                child_to_add=merged_child_tree.entity)

            # Keep track of all db parents of the merged children.
            updated_entities = [m.entity for m in updated_entity_trees]
            if merged_parent_tree.entity not in updated_entities:
                _add_match_to_matched_entities_cache(
                    db_entity_match=merged_parent_tree.entity,
                    ingested_entity=ingested_placeholder_tree.entity,
                    matched_entities_by_db_ids=matched_entities_by_db_ids)
                updated_entity_trees.append(merged_parent_tree)

    for child_field_name, match_results in match_results_by_child:
        placeholder_children = []
        error_count += match_results.error_count
        for child_match_result in match_results.individual_match_results:
            resolve_child_match_result()
        set_field_from_list(ingested_placeholder_tree.entity, child_field_name,
                            placeholder_children)

    # If we updated any of the entity trees, check to see if the placeholder
    # tree still has any children. If it doesn't have any children, it doesn't
    # need to be committed into our DB.
    if updated_entity_trees:
        set_child_fields = get_set_entity_field_names(
            ingested_placeholder_tree.entity,
            entity_field_type=EntityFieldType.FORWARD_EDGE)
        if set_child_fields:
            updated_entity_trees.append(ingested_placeholder_tree)

    return IndividualMatchResult(
        ingested_entity_tree=ingested_placeholder_tree,
        merged_entity_trees=updated_entity_trees,
        error_count=error_count)


def _match_unmatched_tree(
        ingested_unmatched_entity_tree: EntityTree,
        db_entity_trees: List[EntityTree],
        root_entity_cls) \
        -> IndividualMatchResult:
    """
    Attempts to match the provided |ingested_unmatched_entity_tree| to any
    placeholder DB trees in the provided |db_entity_trees| based off of any
    child matches. When such a match is found, the merged child is moved off of
    the placeholder DB entity and onto the ingested entity.

    Returns the results of matching as an IndividualMatchResult.
    """
    db_placeholder_trees = [
        tree for tree in db_entity_trees if is_placeholder(tree.entity)]

    error_count = 0
    match_results_by_child = _get_match_results_for_all_children(
        ingested_entity_tree=ingested_unmatched_entity_tree,
        db_entity_trees=db_placeholder_trees, root_entity_cls=root_entity_cls)

    # If the ingested entity is updated because of a child entity match, we
    # should update our ingested entity's ancestor chain to reflect that of it's
    # counterpart DB. This is necessary for above layers of entity matching
    # which rely on knowing the parent of any merged entities.
    ancestor_chain_updated: List[Entity] = []

    # Initialize so pylint doesn't yell.
    child_match_result = None
    child_field_name = None

    def resolve_child_match_result():
        """Resolves any child matches by moving matched children off of their DB
        placeholder parent and onto the ingested, unmatched entity.
        """
        if not child_field_name or not child_match_result:
            raise EntityMatchingError(
                f"Expected child_field_name and child_match_result to be set, "
                f"but instead got {child_field_name} and {child_match_result} "
                f"respectively.",
                ingested_unmatched_entity_tree.entity.get_entity_name())

        # If child is unmatched, keep track of unchanged child
        if not child_match_result.merged_entity_trees:
            updated_child_trees.append(child_match_result.ingested_entity_tree)
        else:
            # For each matched child, remove child from the DB placeholder and
            # keep track of merged child(ren).
            for merged_child_tree in child_match_result.merged_entity_trees:
                updated_child_trees.append(merged_child_tree)
                placeholder_tree = merged_child_tree.generate_parent_tree()
                remove_child_from_entity(
                    entity=placeholder_tree.entity,
                    child_field_name=child_field_name,
                    child_to_remove=merged_child_tree.entity)

                # For now we only handle the case where all placeholders with
                # matched children have the same parent chain. If they do not,
                # we throw an error.
                if ancestor_chain_updated:
                    if ancestor_chain_updated != \
                            placeholder_tree.ancestor_chain:
                        raise EntityMatchingError(
                            f"Expected all placeholder DB entities matched to "
                            f"an ingested unmatched entity to have the same "
                            f"ancestor chain, but they did not. Found "
                            f"conflicting ancestor chains: "
                            f"{ancestor_chain_updated} and "
                            f"{placeholder_tree.ancestor_chain}",
                            ingested_entity.get_entity_name())
                else:
                    ancestor_chain_updated.extend(
                        placeholder_tree.ancestor_chain)

    ingested_entity = ingested_unmatched_entity_tree.entity
    for child_field_name, match_results in match_results_by_child:
        error_count += match_results.error_count
        ingested_child_field = get_field(ingested_entity, child_field_name)
        updated_child_trees: List[EntityTree] = []
        for child_match_result in match_results.individual_match_results:
            resolve_child_match_result()

        # Update the ingested entity with the updated child(ren).
        updated_children = [mc.entity for mc in updated_child_trees]
        if isinstance(ingested_child_field, list):
            set_field(ingested_entity, child_field_name, updated_children)
        else:
            set_field(ingested_entity, child_field_name, one(updated_children))

    updated_entities = []
    if ancestor_chain_updated:
        updated_entities.append(EntityTree(
            entity=ingested_entity, ancestor_chain=ancestor_chain_updated))

    return IndividualMatchResult(
        ingested_entity_tree=ingested_unmatched_entity_tree,
        merged_entity_trees=updated_entities, error_count=error_count)


def _match_matched_tree(
        *, ingested_entity_tree: EntityTree, db_match_tree: EntityTree,
        matched_entities_by_db_ids: Dict[int, Entity],
        root_entity_cls) \
        -> IndividualMatchResult:
    """Given an |ingested_entity_tree| and it's matched |db_match_tree|, this
    method merges any updated information from teh ingested entity onto the DB
    entity and then continues entity matching for all children of the provided
    objects.

    Returns the results of matching as an IndividualMatchResult.
    """
    ingested_entity = ingested_entity_tree.entity
    db_entity = db_match_tree.entity

    _add_match_to_matched_entities_cache(
        db_entity_match=db_entity, ingested_entity=ingested_entity,
        matched_entities_by_db_ids=matched_entities_by_db_ids)
    error_count = 0
    match_results_by_child = _get_match_results_for_all_children(
        ingested_entity_tree=ingested_entity_tree,
        db_entity_trees=[db_match_tree], root_entity_cls=root_entity_cls)

    # Initialize so pylint doesn't yell
    child_match_result = None

    def resolve_child_match_result():
        """Keeps track of all matched and unmatched children."""
        if not child_match_result:
            raise EntityMatchingError(
                f"Expected child_match_result to be set, but instead got "
                f"{child_match_result}",
                ingested_entity_tree.entity.get_entity_name())

        if not child_match_result.merged_entity_trees:
            updated_child_trees.append(child_match_result.ingested_entity_tree)
        else:
            updated_child_trees.extend(child_match_result.merged_entity_trees)

    for child_field_name, match_results in match_results_by_child:
        error_count += match_results.error_count
        ingested_child_field = getattr(ingested_entity, child_field_name)
        updated_child_trees: List[EntityTree] = []
        for child_match_result in match_results.individual_match_results:
            resolve_child_match_result()

        # Update the db_entity with the updated child(ren).
        updated_children = [c.entity for c in updated_child_trees]
        if isinstance(ingested_child_field, list):
            updated_children.extend(match_results.unmatched_db_entities)
            set_field(db_entity, child_field_name, updated_children)
        else:
            if match_results.unmatched_db_entities:
                raise EntityMatchingError(
                    f"Singular ingested entity field {child_field_name} "
                    f"with value: {ingested_child_field} should "
                    f"match one of the provided db options, but it does not. "
                    f"Found unmatched db entities: "
                    f"{match_results.unmatched_db_entities}",
                    ingested_entity.get_entity_name())
            set_field(db_entity, child_field_name, one(updated_children))

    merged_entity = merge_flat_fields(
        new_entity=ingested_entity, old_entity=db_entity)
    merged_entity_tree = EntityTree(
        entity=merged_entity, ancestor_chain=db_match_tree.ancestor_chain)
    return IndividualMatchResult(ingested_entity_tree=ingested_entity_tree,
                                 merged_entity_trees=[merged_entity_tree],
                                 error_count=error_count)


def _get_match_results_for_all_children(
        ingested_entity_tree: EntityTree, db_entity_trees: List[EntityTree],
        root_entity_cls) \
        -> List[Tuple[str, MatchResults]]:
    """Attempts to match all children of the |ingested_entity_tree| to children
    of the |db_entity_trees|. Matching for each child is independent and can
    match to different DB parents.

    Returns a list of tuples with the following values:
    - str: the string name of the child field
    - MatchResult: the result of matching this child field to children of the
        provided |db_entity_trees|
    """
    results = []
    ingested_entity = ingested_entity_tree.entity
    set_child_fields = get_set_entity_field_names(ingested_entity,
                                                  EntityFieldType.FORWARD_EDGE)

    for child_field_name in set_child_fields:
        ingested_child_field = get_field(ingested_entity, child_field_name)
        db_child_trees = generate_child_entity_trees(child_field_name,
                                                     db_entity_trees)
        if isinstance(ingested_child_field, list):
            ingested_child_list = ingested_child_field
        else:
            ingested_child_list = [ingested_child_field]

        ingested_child_trees = \
            ingested_entity_tree.generate_child_trees(ingested_child_list)
        match_results = _match_entity_trees(
            ingested_entity_trees=ingested_child_trees,
            db_entity_trees=db_child_trees,
            root_entity_cls=root_entity_cls)
        results.append((child_field_name, match_results))
    return results


def _add_match_to_matched_entities_cache(
        *, db_entity_match: Entity, ingested_entity: Entity,
        matched_entities_by_db_ids: Dict[int, Entity]):
    """Records a new ingested_entity/db_entity match. If the DB entity has
    already been matched to a different ingested_entity, it raises an error.
    """
    matched_db_id = db_entity_match.get_id()

    if matched_db_id in matched_entities_by_db_ids:
        if ingested_entity != matched_entities_by_db_ids[matched_db_id]:
            matches = [ingested_entity,
                       matched_entities_by_db_ids[matched_db_id]]
            # It's ok for a DB object to match multiple ingested placeholders.
            if is_placeholder(matches[0]) and is_placeholder(matches[1]):
                return
            raise MatchedMultipleIngestedEntitiesError(db_entity_match, matches)
    else:
        matched_entities_by_db_ids[matched_db_id] = ingested_entity


def _get_match(ingested_entity_tree: EntityTree,
               db_entity_trees: List[EntityTree]) -> Optional[EntityTree]:
    """With the provided |ingested_entity_tree|, this attempts to find a match
    among the provided |db_entity_trees|. If a match is found, it is returned.
    """
    exact_match = entity_matching_utils.get_only_match(
        ingested_entity_tree, db_entity_trees, is_match)

    if not exact_match:
        if isinstance(ingested_entity_tree.entity, StateIncarcerationPeriod):
            return nd_get_incomplete_incarceration_period_match(
                ingested_entity_tree, db_entity_trees)
        if isinstance(ingested_entity_tree.entity,
                      (StateAgent, StateIncarcerationSentence, StateAssessment,
                       StateSupervisionPeriod, StateSupervisionViolation,
                       StateSupervisionViolationResponse)):
            return entity_matching_utils.get_only_match(
                ingested_entity_tree, db_entity_trees, base_entity_match)
    return exact_match


# TODO(2037): Move this into a post processing file.
def merge_multiparent_entities(persons: List[StatePerson]):
    """For each person in the provided |persons|, looks at all of the child
    entities that can have multiple parents, and merges these entities where
    possible.
    """
    for person in persons:
        charge_map: Dict[str, List[_EntityWithParents]] = {}
        _populate_multiparent_map(person, StateCharge, charge_map)
        _merge_multiparent_entities_from_map(charge_map)

        court_case_map: Dict[str, List[_EntityWithParents]] = {}
        _populate_multiparent_map(person, StateCourtCase, court_case_map)
        _merge_multiparent_entities_from_map(court_case_map)

        agent_map: Dict[str, List[_EntityWithParents]] = {}
        _populate_multiparent_map(person, StateAgent, agent_map)
        _merge_multiparent_entities_from_map(agent_map)


class _LinkedParents:
    def __init__(self, parent: Entity, field_name: str):
        self.parent = parent
        self.field_name = field_name


class _EntityWithParents:
    def __init__(self, entity: Entity, linked_parents: List[_LinkedParents]):
        self.entity = entity
        self.linked_parents = linked_parents


def _merge_multiparent_entities_from_map(
        multiparent_map: Dict[str, List[_EntityWithParents]]):
    """Merges entities from the provided |multiparent_map|."""
    for entities_with_parents in multiparent_map.values():
        merged_entity_with_parents = None
        for entity_with_parents in entities_with_parents:
            if not merged_entity_with_parents:
                merged_entity_with_parents = entity_with_parents
                continue

            # Keep track of which one is a DB entity for matching below. If
            # both are ingested, then order does not matter for matching.
            if entity_with_parents.entity.get_id():
                db_with_parents = entity_with_parents
                ing_with_parents = merged_entity_with_parents
            else:
                db_with_parents = merged_entity_with_parents
                ing_with_parents = entity_with_parents

            # Merge the two objects via entity matching
            db_tree = EntityTree(db_with_parents.entity, [])
            ing_tree = EntityTree(ing_with_parents.entity, [])
            match_result = _match_matched_tree(
                ingested_entity_tree=ing_tree, db_match_tree=db_tree,
                matched_entities_by_db_ids={}, root_entity_cls=StatePerson)
            updated_entity = one(match_result.merged_entity_trees).entity

            # As entity matching automatically updates the input db entity, we
            # only have to replace ing_with_parents.entity.
            _replace_entity(
                entity=updated_entity, to_replace=ing_with_parents.entity,
                linked_parents=ing_with_parents.linked_parents)
            db_with_parents.linked_parents.extend(
                ing_with_parents.linked_parents)
            merged_entity_with_parents = db_with_parents


def _is_subset(entity: Entity, subset: Entity) -> bool:
    """Checks if all fields on the provided |subset| are present in the provided
    |entity|. Returns True if so, otherwise False.
    """
    for field_name in get_set_entity_field_names(
            subset, EntityFieldType.FLAT_FIELD):
        if get_field(entity, field_name) != get_field(subset, field_name):
            return False
    for field_name in get_set_entity_field_names(
            subset, EntityFieldType.FORWARD_EDGE):
        for field in get_field_as_list(subset, field_name):
            if field not in get_field_as_list(entity, field_name):
                return False
    return True


def _replace_entity(
        *, entity: Entity, to_replace: Entity,
        linked_parents: List[_LinkedParents]):
    """For all parent entities in |to_replace_parents|, replaces |to_replace|
    with |entity|.
    """
    for linked_parent in linked_parents:
        remove_child_from_entity(
            entity=linked_parent.parent,
            child_field_name=linked_parent.field_name,
            child_to_remove=to_replace)
        add_child_to_entity(entity=linked_parent.parent,
                            child_field_name=linked_parent.field_name,
                            child_to_add=entity)


def _populate_multiparent_map(
        entity: Entity, entity_cls: Type,
        multiparent_map: Dict[str, List[_EntityWithParents]]):
    """Looks through all children in the provided |entity|, and if they are of
    type |entity_cls|, adds an entry to the provided |multiparent_map|.
    """
    for child_field_name in get_set_entity_field_names(
            entity, EntityFieldType.FORWARD_EDGE):
        linked_parent = _LinkedParents(entity, child_field_name)
        for child in get_field_as_list(entity, child_field_name):
            _populate_multiparent_map(child, entity_cls, multiparent_map)

            if not isinstance(child, entity_cls):
                continue

            # All persistence entities are ExternalIdEntities
            child = cast(ExternalIdEntity, child)
            external_id = child.external_id

            # We're only matching entities if they have the same
            # external_id.
            if not external_id:
                continue

            if external_id in multiparent_map.keys():
                entities_with_parents = multiparent_map[external_id]
                found_entity = False

                # If the child object itself has already been seen, simply add
                # the |entity| parent to the list of linked parents
                for entity_with_parents in entities_with_parents:
                    if id(entity_with_parents.entity) == id(child):
                        found_entity = True
                        entity_with_parents.linked_parents.append(linked_parent)

                # If the child object has not been seen, create a new
                # _EntityWithParents object for this external_id
                if not found_entity:
                    entity_with_parents = \
                        _EntityWithParents(child, [linked_parent])
                    entities_with_parents.append(entity_with_parents)

            # If the external_id has never been seen before, create a new
            # entry for it.
            else:
                entity_with_parents = _EntityWithParents(
                    child, [linked_parent])
                multiparent_map[external_id] = [entity_with_parents]
