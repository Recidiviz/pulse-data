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
import datetime
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Set, Tuple, Type, cast

from more_itertools import one

from recidiviz.common.common_utils import check_all_objs_have_type
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema.state.dao import check_not_dirty
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_entity_people_to_schema_people,
)
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.entity_utils import (
    get_all_core_entity_field_names,
    get_all_db_objs_from_tree,
    get_all_db_objs_from_trees,
    get_set_entity_field_names,
    is_placeholder,
    is_standalone_class,
    is_standalone_entity,
    log_entity_count,
)
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity_matching import entity_matching_utils
from recidiviz.persistence.entity_matching.base_entity_matcher import (
    BaseEntityMatcher,
    increment_error,
)
from recidiviz.persistence.entity_matching.entity_matching_types import (
    EntityTree,
    IndividualMatchResult,
    MatchedEntities,
    MatchResults,
)
from recidiviz.persistence.entity_matching.state.base_state_matching_delegate import (
    BaseStateMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.state_matching_utils import (
    EntityFieldType,
    add_child_to_entity,
    convert_to_placeholder,
    db_id_or_object_id,
    generate_child_entity_trees,
    get_all_entity_trees_of_cls,
    get_external_id_keys_from_multiple_id_entity,
    get_external_ids_from_entity,
    get_multiparent_classes,
    get_multiple_id_classes,
    get_root_entity_cls,
    get_total_entities_of_cls,
    is_match,
    is_multiple_id_entity,
    read_db_entity_trees_of_cls_to_merge,
    remove_child_from_entity,
)
from recidiviz.persistence.errors import (
    EntityMatchingError,
    MatchedMultipleIngestedEntitiesError,
)

# How many person trees to search to fill the non_placeholder_ingest_types set.
MAX_NUM_TREES_TO_SEARCH_FOR_NON_PLACEHOLDER_TYPES = 20


class _ParentInfo:
    """
    Information about a parent entity of a child entity, with the name of
    the field on the parent that links to its child.
    """

    def __init__(self, parent: DatabaseEntity, field_name: str):
        self.parent = parent
        self.field_name = field_name


class _EntityWithParentInfo:
    """
    Struct containing an child entity and information about all the parent
    objects currently pointing to that entity object.
    """

    def __init__(self, entity: DatabaseEntity, linked_parents: List[_ParentInfo]):
        self.entity = entity
        self.linked_parents = linked_parents


# TODO(#2504): Rename `ingested` and `db` entities to something more generic that
# still accurately describes that one is being merged onto the other.
class StateEntityMatcher(BaseEntityMatcher[entities.StatePerson]):
    """Class that handles entity matching for all state data."""

    def __init__(self, state_matching_delegate: BaseStateMatchingDelegate):
        self.all_ingested_db_objs: Set[DatabaseEntity] = set()
        self.ingest_obj_id_to_person_id: Dict[int, int] = defaultdict()
        self.person_id_to_ingest_objs: Dict[int, Set[DatabaseEntity]] = defaultdict(set)

        # The first class found in our ingested object graph for which we have
        # information (i.e. non-placeholder). We make the assumption that all
        # ingested object graphs have the same root entity class and that this
        # root entity class can be matched via external_id. Defaults to
        # schema.StatePerson
        self.root_entity_cls: Type[DatabaseEntity] = schema.StatePerson

        # Cache of DB objects of type self.root_entity_cls (above) keyed by
        # their external ids.
        self.root_entity_cache: Dict[str, List[EntityTree]] = defaultdict(list)

        # Set of class types in the ingested objects that are not placeholders
        self.non_placeholder_ingest_types: Set[Type[DatabaseEntity]] = set()

        # Set this to True if you want to run with consistency checking
        self.do_ingest_obj_consistency_check = False

        # Set this to True if you want counts on all entities read from the
        # DB to be logged.
        self.log_entity_counts = False

        self.entities_to_convert_to_placeholder_or_expunge: List[DatabaseEntity] = []

        # Delegate object with all state specific logic
        self.state_matching_delegate = state_matching_delegate

        self.session: Optional[Session] = None

    def set_session(self, session: Session):
        if self.session:
            raise ValueError(
                "Attempting to set self.session, " "but self.session is already set"
            )
        self.session = session

    def get_session(self) -> Session:
        if self.session is None:
            raise ValueError(
                "Attempting to get self.session, " "but self.session is not set"
            )
        return self.session

    def set_root_entity_cache(self, db_persons: List[schema.StatePerson]) -> None:
        if not db_persons:
            return

        trees = get_all_entity_trees_of_cls(db_persons, self.root_entity_cls)
        for tree in trees:
            external_ids = get_external_ids_from_entity(tree.entity)
            for external_id in external_ids:
                self.root_entity_cache[external_id].append(tree)

    def _is_placeholder(self, entity: DatabaseEntity) -> bool:
        """Entity matching-specific wrapper around is_placeholder() that checks that
        we didn't fail to add any types to the non_placeholder_ingest_types set.
        """
        entity_is_placeholder = is_placeholder(entity)
        entity_cls = type(entity)
        if (
            not entity_is_placeholder
            # If the primary key is set, this is an entity that has been committed to
            # the DB already and is not an ingested entity.
            and entity.get_primary_key() is None
            and entity_cls not in self.non_placeholder_ingest_types
        ):
            error_message = (
                f"Failed to identify one of the non-placeholder ingest types: "
                f"[{entity_cls.__name__}]. Entity: [{entity}]"
            )
            if hasattr(entity, "external_id") or isinstance(entity, schema.StatePerson):
                # TODO(#7908): This was changed from raising a value error to just logging
                # an error. This should probably be re-visited to see if we can re-enable
                # raising the error.
                logging.error(error_message)
            else:
                # This is an enum-like entity that we will not do any interesting
                # post-processing to. Downgrade to a warning and continue.
                logging.warning(
                    "%s. Adding non-external_id class [%s] to set now.",
                    error_message,
                    entity_cls,
                )
                self.non_placeholder_ingest_types.add(entity_cls)

        return entity_is_placeholder

    @staticmethod
    def get_non_placeholder_ingest_types_indices_to_search(
        num_total_trees: int,
    ) -> List[int]:
        """
        We search through 20 trees (evenly distributed across the list) to find all
        applicable non-placeholder types.
        """
        if num_total_trees == 0:
            return []

        num_trees_to_search = min(
            MAX_NUM_TREES_TO_SEARCH_FOR_NON_PLACEHOLDER_TYPES,
            num_total_trees,
        )
        indices_to_search = list(range(num_total_trees))[
            0 :: num_total_trees // num_trees_to_search
        ]
        return indices_to_search[0:num_trees_to_search]

    def get_non_placeholder_ingest_types(
        self, ingested_db_persons: List[schema.StatePerson]
    ) -> Set[Type[DatabaseEntity]]:
        """Returns set of class types in the ingested persons that are not
        placeholders.

        NOTE: This assumes that ingested trees are largely similar and all
        non-placeholder types will show up in a selected set of 20 person trees. If the
        first 20 do not surface all types, we will log an error later in the
        _is_placeholder() helper and entity matching might not work correctly.
        """
        non_placeholder_ingest_types: Set[Type[DatabaseEntity]] = set()

        indices_to_search = self.get_non_placeholder_ingest_types_indices_to_search(
            num_total_trees=len(ingested_db_persons)
        )

        for i in indices_to_search:
            ingested_person = ingested_db_persons[i]
            for obj in get_all_db_objs_from_tree(ingested_person):
                if not is_placeholder(obj):
                    non_placeholder_ingest_types.add(type(obj))
        return non_placeholder_ingest_types

    def set_ingest_objs_for_persons(
        self, ingested_db_persons: List[schema.StatePerson]
    ) -> None:
        if self.do_ingest_obj_consistency_check:
            logging.info(
                "[Entity matching] Setting ingest object mappings for [%s] "
                "persons trees",
                len(ingested_db_persons),
            )
            for person in ingested_db_persons:
                person_ingest_objs = get_all_db_objs_from_tree(person)
                for obj in person_ingest_objs:
                    self.ingest_obj_id_to_person_id[id(obj)] = id(person)
                    self.person_id_to_ingest_objs[id(person)].add(obj)
                    self.all_ingested_db_objs.add(obj)
            logging.info(
                "[Entity matching] Done setting ingest object mappings for [%s]"
                " persons trees",
                len(ingested_db_persons),
            )

    def check_no_ingest_objs(self, db_objs: List[DatabaseEntity]) -> None:
        if self.do_ingest_obj_consistency_check:
            ingest_objs = self.get_ingest_objs_from_list(db_objs)
            if ingest_objs:
                logging.warning(
                    "Found [%s] unexpected object from ingested entity tree",
                    len(ingest_objs),
                )

                objs_by_person_id = self.generate_ingest_objs_by_person_id(ingest_objs)
                for person_id, found_objs in objs_by_person_id.items():
                    self.log_found_ingest_obj_info_for_person(person_id, found_objs)

                raise ValueError(
                    f"Found unexpected ingest object. First unexpected object: "
                    f"{self.describe_db_entity(next(iter(ingest_objs)))}"
                )

    def generate_ingest_objs_by_person_id(self, found_objs: Set[DatabaseEntity]):
        person_id_to_ingest_objs: Dict[int, Set[DatabaseEntity]] = defaultdict(set)
        for obj in found_objs:
            person_id = self.ingest_obj_id_to_person_id[id(obj)]
            person_id_to_ingest_objs[person_id].add(obj)
        return person_id_to_ingest_objs

    def log_found_ingest_obj_info_for_person(
        self, person_id: int, found_objs: Set[DatabaseEntity]
    ) -> None:
        logging.warning("For person [%s]:", str(person_id))
        logging.warning("  Found:")
        for found_obj in found_objs:
            logging.warning("    %s", self.describe_db_entity(found_obj))

        logging.warning("  All:")
        for obj in self.person_id_to_ingest_objs[person_id]:
            logging.warning("    %s", self.describe_db_entity(obj))

    def get_ingest_objs_from_list(
        self, db_objs: List[DatabaseEntity]
    ) -> Set[DatabaseEntity]:
        all_objs = get_all_db_objs_from_trees(db_objs)
        intersection = self.all_ingested_db_objs.intersection(all_objs)

        return intersection

    @staticmethod
    def describe_db_entity(entity) -> str:
        external_id = (
            entity.get_external_id() if hasattr(entity, "external_id") else "N/A"
        )

        return (
            f"{entity.get_entity_name()}({id(entity)}) "
            f"external_id={external_id} placeholder={is_placeholder(entity)}"
        )

    def run_match(
        self,
        session: Session,
        _,  # region_code, instead taken from state_matching_delegate
        ingested_people: List[entities.StatePerson],
    ) -> MatchedEntities:
        """Attempts to match all persons from |ingested_persons| with
        corresponding persons in our database. Returns a MatchedEntities object
        that contains the results of matching.
        """
        self.set_session(session)
        logging.info(
            "[Entity matching] Converting ingested entities to DB entities "
            "at time [%s].",
            datetime.datetime.now().isoformat(),
        )
        ingested_db_persons = convert_entity_people_to_schema_people(
            ingested_people, populate_back_edges=False
        )
        self.non_placeholder_ingest_types = self.get_non_placeholder_ingest_types(
            ingested_db_persons
        )
        self.set_ingest_objs_for_persons(ingested_db_persons)
        self.root_entity_cls = get_root_entity_cls(ingested_db_persons)

        logging.info(
            "[Entity matching] Starting reading and converting persons "
            "at time [%s].",
            datetime.datetime.now().isoformat(),
        )

        check_all_objs_have_type(ingested_db_persons, schema.StatePerson)
        db_persons = self.state_matching_delegate.read_potential_match_db_persons(
            session=session, ingested_persons=ingested_db_persons
        )

        if self.log_entity_counts:
            logging.info("Entity counts for all people read from the DB:")
            log_entity_count(db_persons)

        logging.info(
            "Read [%d] persons from DB in region [%s]",
            len(db_persons),
            self.state_matching_delegate.get_region_code(),
        )

        self.set_root_entity_cache(db_persons)

        logging.info(
            "[Entity matching] Completed DB read at time [%s].",
            datetime.datetime.now().isoformat(),
        )
        matched_entities_builder = self._run_match(ingested_db_persons, db_persons)

        # In order to maintain the invariant that all objects are properly
        # added to the Session when we return from entity_matching we
        # add new persons to the session here. Adding a person to the session
        # that is already in-session (i.e. not new) has no effect.
        # pylint:disable=not-an-iterable
        for match_person in matched_entities_builder.people:
            session.add(match_person)

        logging.info("[Entity matching] Session flush")
        session.flush()

        logging.info("[Entity matching] Database clean up")
        # TODO(#2578): Database cleanup errors should be surfaced properly in
        # their own error count.
        matched_entities_builder.database_cleanup_error_count = (
            self._perform_database_cleanup(matched_entities_builder.people)
        )

        # Assert that all entities have been flushed before returning from
        # entity matching
        check_not_dirty(session)

        return matched_entities_builder.build()

    def _run_match(
        self,
        ingested_persons: List[schema.StatePerson],
        db_persons: List[schema.StatePerson],
    ) -> MatchedEntities.Builder:
        """Attempts to match |ingested_persons| with persons in |db_persons|.
        Assumes no backedges are present in either |ingested_persons| or
        |db_persons|.
        """
        # Prevent automatic flushing of any entities associated with this
        # session until we have explicitly finished matching. This ensures
        # errors aren't thrown due to unexpected flushing of incomplete entity
        # relationships.
        session = self.get_session()
        with session.no_autoflush:
            logging.info("[Entity matching] Pre-processing")
            ingested_persons = self._perform_match_preprocessing(ingested_persons)

            logging.info("[Entity matching] Matching persons")
            matched_entities_builder = self._match_persons(
                ingested_persons=ingested_persons, db_persons=db_persons
            )
            logging.info(
                "[Entity matching] Matching persons returned [%s] " "matched persons",
                len(matched_entities_builder.people),
            )

            logging.info("[Entity matching] Matching post-processing")
            self._perform_match_postprocessing(
                matched_entities_builder.people, db_persons
            )
        return matched_entities_builder

    def _perform_match_preprocessing(
        self, ingested_persons: List[schema.StatePerson]
    ) -> List[schema.StatePerson]:
        """Performs state specific preprocessing on the provided
        |ingested_persons|.

        After post processing, we guarantee that all backedges are properly
        set on entities within the |ingested_persons| trees.
        """
        logging.info("[Entity matching] Pre-processing: Merge multi-id " "entities")
        preprocessed_persons = self.merge_multiple_id_entities(ingested_persons)
        self.state_matching_delegate.perform_match_preprocessing(preprocessed_persons)
        return preprocessed_persons

    def _perform_match_postprocessing(
        self,
        matched_persons: List[schema.StatePerson],
        db_persons: List[schema.StatePerson],
    ) -> None:
        # TODO(#1868): Remove any placeholders in graph without children after
        # write
        logging.info("[Entity matching] Merge multi-parent entities")
        self.merge_multiparent_entities(matched_persons)

        self.state_matching_delegate.perform_match_postprocessing(matched_persons)

        # TODO(#2894): Create long term strategy for dealing with/rolling back partially updated DB entities.
        # Make sure person ids are added to all entities in the matched persons and also to all read DB people. We
        # populate back edges on DB people as a precaution in case entity matching failed for any of these people
        # after some updates had already taken place.
        logging.info("[Entity matching] Populate indirect person backedges")
        new_people = [p for p in matched_persons if p.person_id is None]
        self._populate_person_backedges(new_people)
        self._populate_person_backedges(db_persons)

    def _perform_database_cleanup(
        self, matched_persons: List[schema.StatePerson]
    ) -> int:
        """Home to all cleanup functions that require that database ids exist
        on all entities, including newly created ones. Returns the number of
        errors encountered while performing clean up.

        NOTE: This method expects that all updated entities have been flushed
        in the session.
        """
        check_not_dirty(self.get_session())
        error_count = 0

        logging.info("[Entity matching] Merge new parent-child links")
        error_count += self._merge_new_parent_child_links(matched_persons)

        return error_count

    def _merge_new_parent_child_links(
        self, matched_persons: List[schema.StatePerson]
    ) -> int:
        """Due to the way in which entity matching limits scope on potential
        matches, we can accidentally create multiple entities with the same
        external_id. This method queries from the DB and attempts to find
        all newly created entities of this type (if any exist), and then merges
        those duplicate entities.

        All affected persons are added to matched_persons, if not already
        present.

        #TODO(#2578): Currently this method does not handle merging of duplicate
        entities of multi_parent classes or multi_id classess aside from
        StatePerson. It also always merges the duplicate entity itself, and does
        not perform merging of any parent chains, which could be useful when
        both entities have distinct non-placeholder ancestor chains. We have yet
        to have use for this extra functionality.
        """
        region_code = self.state_matching_delegate.get_region_code()
        session = self.get_session()
        error_count = 0
        seen_persons_ids: Set[int] = set()
        for person in matched_persons:
            seen_persons_ids.add(person.person_id)

        classes_to_merge = []
        for cls in self.non_placeholder_ingest_types:
            if self._can_merge_child_cls(cls):
                classes_to_merge.append(cls)

        for cls in classes_to_merge:
            tree_groups = read_db_entity_trees_of_cls_to_merge(
                session, region_code, cls
            )

            for tree_group in tree_groups:
                merged_tree = tree_group[0]
                for next_tree in tree_group[1:]:

                    # Add all persons affected to the list of matched persons
                    merged_tree_person = cast(
                        schema.StatePerson, merged_tree.get_earliest_ancestor()
                    )
                    next_tree_person = cast(
                        schema.StatePerson, next_tree.get_earliest_ancestor()
                    )
                    if merged_tree_person.person_id not in seen_persons_ids:
                        seen_persons_ids.add(merged_tree_person.person_id)
                        matched_persons.append(merged_tree_person)
                    if next_tree_person.person_id not in seen_persons_ids:
                        seen_persons_ids.add(next_tree_person.person_id)
                        matched_persons.append(next_tree_person)

                    # Merge duplicate entities
                    (
                        merge_from_tree,
                        merge_onto_tree,
                    ) = self._order_merge_from_and_onto_trees(merged_tree, next_tree)

                    if not self._check_parent_chains_match(
                        merge_onto_tree, merge_from_tree
                    ):
                        error_count += 1
                        continue
                    logging.info(
                        "Discovered new parent child link with entity type "
                        "[%s]. Merging entity with id [%s] onto entity "
                        "with id [%s]",
                        cls.__name__,
                        merge_from_tree.entity.get_id(),
                        merge_onto_tree.entity.get_id(),
                    )
                    match_result = self._match_entity_trees(
                        ingested_entity_trees=[merge_from_tree],
                        db_entity_trees=[merge_onto_tree],
                        root_entity_cls=cls,
                    )
                    error_count += match_result.error_count
                    merged_tree = merge_onto_tree

            # Manually flush changes before next query.
            session.flush()
        return error_count

    def _can_merge_child_cls(self, cls):
        if not hasattr(cls, "external_id"):
            return False
        if is_standalone_class(cls):
            return False

        # TODO(#2578): Add functionality to merge multi_id and multi_parent
        # classes in this case.
        if cls == schema.StatePersonExternalId:
            return False
        if cls in get_multiparent_classes():
            return False
        return True

    def _order_merge_from_and_onto_trees(
        self, a: EntityTree, b: EntityTree
    ) -> Tuple[EntityTree, EntityTree]:
        """Looks at the passed in trees |a| and |b| and returns a tuple of the
        two ordered so that the first element is the tree to merge from and the
        second is the tree to merge onto.
        """
        a_ancestor_tree = a.generate_parent_tree()
        if self._is_placeholder(a_ancestor_tree.entity):
            return a, b
        return b, a

    def _check_parent_chains_match(self, a: EntityTree, b: EntityTree) -> bool:
        """Returns True if the parent chains of the provided entity trees |a|
        and |b| match. Otherwise returns false.
        """
        entity_cls_name = a.entity.get_entity_name()
        # TODO(#2578): Support merging when entities have multiple ancestor
        # ancestor chains.
        if len(a.ancestor_chain) != len(b.ancestor_chain):
            logging.error(
                "Ancestor chains are different lengths for entities of type "
                "[%s].\nEntity a id: [%s]\nEntity b id: [%s]",
                entity_cls_name,
                a.entity.get_id(),
                b.entity.get_id(),
            )
            return False

        if not a.ancestor_chain:
            return True

        a_ancestor_tree = a
        b_ancestor_tree = b
        while a_ancestor_tree.ancestor_chain:
            a_ancestor_tree = a_ancestor_tree.generate_parent_tree()
            b_ancestor_tree = b_ancestor_tree.generate_parent_tree()
            # ignore placeholders in the chain.
            if self._is_placeholder(a_ancestor_tree.entity) or self._is_placeholder(
                b_ancestor_tree.entity
            ):
                pass
            elif not is_match(a_ancestor_tree, b_ancestor_tree):
                logging.error(
                    "Ancestor chains do not match for entities to merge of type"
                    " [%s] with ids [%s] and [%s].",
                    entity_cls_name,
                    a.entity.get_id(),
                    b.entity.get_id(),
                )
                return False
        return True

    def merge_multiple_id_entities(
        self, ingested_persons: List[schema.StatePerson]
    ) -> List[schema.StatePerson]:
        """Merges all entities in the list of |ingested_persons| that can have
        multiple external ids (Currently just StatePerson).

        Returns the list of unique StatePeople after this merging.
        """
        persons: List[schema.StatePerson] = []
        for cls in get_multiple_id_classes():
            merged_entities = self._merge_multiple_id_entities_helper(
                ingested_person_trees=ingested_persons, multiple_id_cls=cls
            )
            if cls == schema.StatePerson:
                persons = cast(List[schema.StatePerson], merged_entities)

        return persons

    def _merge_multiple_id_entities_helper(
        self,
        ingested_person_trees: List[schema.StatePerson],
        multiple_id_cls: Type[DatabaseEntity],
    ) -> Sequence[DatabaseEntity]:
        """Helper method to merge all entities of type |multiple_id_cls| with
        colliding ids. Returns a list of unique entities after this merging
        is complete.
        """
        to_return = []
        processed_entities_by_external_id_key: Dict[str, EntityTree] = {}
        ingested_entity_trees = get_all_entity_trees_of_cls(
            ingested_person_trees, multiple_id_cls
        )

        for ingested_entity_tree in ingested_entity_trees:
            ingested_entity = ingested_entity_tree.entity
            external_id_keys = get_external_id_keys_from_multiple_id_entity(
                ingested_entity
            )

            # If no external_ids, don't worry about merging them with other
            # ingested entities
            if not external_id_keys:
                to_return.append(ingested_entity)
                continue

            # Merge entities when external_ids collide
            self._merge_ingested_entity_with_duplicates(
                ingested_entity_tree,
                external_id_keys,
                processed_entities_by_external_id_key,
            )

            updated_external_id_keys = get_external_id_keys_from_multiple_id_entity(
                ingested_entity
            )
            for external_id_key in updated_external_id_keys:
                processed_entities_by_external_id_key[
                    external_id_key
                ] = ingested_entity_tree

        # Only return unique entities
        to_return.extend(
            self._get_unique_entities(processed_entities_by_external_id_key)
        )

        return to_return

    def _merge_ingested_entity_with_duplicates(
        self,
        entity_tree: EntityTree,
        external_id_keys: List[str],
        processed_entities_by_external_id_key: Dict[str, EntityTree],
    ):
        """Helper function that merges the provided |entity_tree| with all of
        the colliding entities in |processed_entities_by_external_id_key|.
        The provided |entity_tree| is modified in place as colliding entities
        are merged onto the |entity_tree|.
        """
        trees_to_merge_into = []

        for external_id_key in external_id_keys:
            if external_id_key in processed_entities_by_external_id_key:
                trees_to_merge_into.append(
                    processed_entities_by_external_id_key[external_id_key]
                )

        for tree_to_merge in trees_to_merge_into:
            self._match_matched_tree(
                ingested_entity_tree=tree_to_merge,
                db_match_tree=entity_tree,
                matched_entities_by_db_ids={},
                root_entity_cls=schema.StatePerson,
            )

    def _get_unique_entities(self, d: Dict[str, EntityTree]):
        """Returns all unique entities found in the provided dict |d|."""
        seen_map: Set[int] = set()
        to_return: List[DatabaseEntity] = []

        for entity_tree in d.values():
            entity_id = id(entity_tree.entity)
            if entity_id not in seen_map:
                seen_map.add(entity_id)
                to_return.append(entity_tree.entity)
        return to_return

    def _match_persons(
        self,
        *,
        ingested_persons: List[schema.StatePerson],
        db_persons: List[schema.StatePerson],
    ) -> MatchedEntities.Builder:
        """Attempts to match all persons from |ingested_persons| with the
        provided |db_persons|. Results are returned in the MatchedEntities
        object which contains all successfully matched and merged persons as
        well as an error count that is incremented every time an error is raised
        matching an ingested person.

        All returned persons have direct and indirect backedges set.
        """
        db_person_trees = [
            EntityTree(entity=db_person, ancestor_chain=[]) for db_person in db_persons
        ]
        ingested_person_trees = [
            EntityTree(entity=ingested_person, ancestor_chain=[])
            for ingested_person in ingested_persons
        ]

        root_entity_cls = get_root_entity_cls(ingested_persons)
        total_root_entities = get_total_entities_of_cls(
            ingested_persons, root_entity_cls
        )
        persons_match_results = self._match_entity_trees(
            ingested_entity_trees=ingested_person_trees,
            db_entity_trees=db_person_trees,
            root_entity_cls=root_entity_cls,
        )

        updated_persons: List[schema.StatePerson] = []
        for match_result in persons_match_results.individual_match_results:
            if not match_result.merged_entity_trees:
                raise EntityMatchingError(
                    "Expected match_result.merged_entity_trees to not be empty.",
                    "state_person",
                )

            # It is possible that multiple ingested persons match to the same
            # DB person, in which case we should only keep one reference to
            # that object.
            for merged_person_tree in match_result.merged_entity_trees:
                if not isinstance(merged_person_tree.entity, schema.StatePerson):
                    raise EntityMatchingError(
                        f"Expected merged_person_tree.entity [{merged_person_tree.entity}] to "
                        f"have type schema.StatePerson.",
                        "state_person",
                    )

                if merged_person_tree.entity not in updated_persons:
                    updated_persons.append(merged_person_tree.entity)

        # The only database persons that are unmatched that we potentially want
        # to update are placeholder persons. These may have had children removed
        # as a part of the matching process and therefore would need updating.
        for db_person in persons_match_results.unmatched_db_entities:
            if not isinstance(db_person, schema.StatePerson):
                raise EntityMatchingError(
                    f"Expected db_person [{db_person}] to have type schema.StatePerson.",
                    "state_person",
                )

            if self._is_placeholder(db_person):
                updated_persons.append(db_person)

        self._populate_person_backedges(updated_persons)

        matched_entities_builder = MatchedEntities.builder()
        matched_entities_builder.people = updated_persons
        matched_entities_builder.error_count = persons_match_results.error_count
        matched_entities_builder.total_root_entities = total_root_entities
        return matched_entities_builder

    def _populate_person_backedges(self, updated_persons: List[schema.StatePerson]):
        for person in updated_persons:
            children = get_all_db_objs_from_tree(person)
            self.check_no_ingest_objs(list(children))
            for child in children:
                if child is not person and not is_standalone_entity(child):
                    child.set_field("person", person)

    def _match_entity_trees(
        self,
        *,
        ingested_entity_trees: List[EntityTree],
        db_entity_trees: List[EntityTree],
        root_entity_cls: Type[DatabaseEntity],
    ) -> MatchResults:
        """Attempts to match all of the |ingested_entity_trees| with one of the
        provided |db_entity_trees|. For all matches, merges the ingested entity
        information into the db entity, and continues entity matching for all
        child entities.
        If the provided |root_entity_cls| corresponds to the class of the
        provided |ingested_entity_trees|, increments an error count rather than
        raising when one is encountered.
        Returns a MatchResults object which contains IndividualMatchResults for
        each ingested tree, a list of unmatched DB entities, and the number of
        errors encountered while matching these trees.
        """
        individual_match_results: List[IndividualMatchResult] = []
        matched_entities_by_db_id: Dict[int, List[DatabaseEntity]] = {}
        error_count = 0
        for ingested_entity_tree in ingested_entity_trees:
            try:
                match_result = self._match_entity_tree(
                    ingested_entity_tree=ingested_entity_tree,
                    db_entity_trees=db_entity_trees,
                    matched_entities_by_db_ids=matched_entities_by_db_id,
                    root_entity_cls=root_entity_cls,
                )
                individual_match_results.append(match_result)
                error_count += match_result.error_count
            except EntityMatchingError as e:
                if isinstance(ingested_entity_tree.entity, root_entity_cls):
                    ingested_entity = ingested_entity_tree.entity
                    logging.exception(
                        "Found error while matching ingested entity %s with root entity class %s.",
                        e.entity_name,
                        ingested_entity.get_entity_name(),
                    )
                    increment_error(e.entity_name)
                    error_count += 1
                else:
                    raise e

        # Keep track of even unmatched DB entities, as the parent of this entity
        # layer must know about all of its children (even the unmatched ones).
        # If we exclude the unmatched database entities from this list, on
        # write, SQLAlchemy will treat the incomplete child list as an update,
        # and attempt to remove any children with links to the parent in our
        # database but not in the provided list.
        unmatched_db_entities: List[DatabaseEntity] = []
        for db_entity_tree in db_entity_trees:
            db_entity = db_entity_tree.entity
            db_id = db_id_or_object_id(db_entity)
            if db_id not in matched_entities_by_db_id.keys():
                unmatched_db_entities.append(db_entity)

        for entity in self.entities_to_convert_to_placeholder_or_expunge:
            self._convert_to_placeholder_or_expunge(entity)
        self.entities_to_convert_to_placeholder_or_expunge.clear()

        return MatchResults(
            individual_match_results, unmatched_db_entities, error_count
        )

    def _convert_to_placeholder_or_expunge(self, entity: DatabaseEntity):
        """
        Depending on whether or not the provided |entity| has already been
        committed to the db, this function either converts the |entity| into a
        placeholder object or removes it from the session altogether.
        """
        if entity.get_id():
            convert_to_placeholder(entity)
        else:
            session = self.get_session()
            if entity in session:
                session.expunge(entity)

    def _match_entity_tree(
        self,
        *,
        ingested_entity_tree: EntityTree,
        db_entity_trees: List[EntityTree],
        matched_entities_by_db_ids: Dict[int, List[DatabaseEntity]],
        root_entity_cls: Type,
    ) -> IndividualMatchResult:
        """Attempts to match the provided |ingested_entity_tree| to one of the
        provided |db_entity_trees|. If a successful match is found, merges the
        ingested entity onto the matching database entity and performs entity
        matching on all children of the matched entities.
        Returns the results of matching as an IndividualMatchResult.
        """

        if self._is_placeholder(ingested_entity_tree.entity):
            return self._match_placeholder_tree(
                ingested_placeholder_tree=ingested_entity_tree,
                db_entity_trees=db_entity_trees,
                matched_entities_by_db_ids=matched_entities_by_db_ids,
                root_entity_cls=root_entity_cls,
            )

        db_match_tree = self._get_match(ingested_entity_tree, db_entity_trees)

        if not db_match_tree:
            return self._match_unmatched_tree(
                ingested_unmatched_entity_tree=ingested_entity_tree,
                db_entity_trees=db_entity_trees,
                root_entity_cls=root_entity_cls,
            )

        return self._match_matched_tree(
            ingested_entity_tree=ingested_entity_tree,
            db_match_tree=db_match_tree,
            matched_entities_by_db_ids=matched_entities_by_db_ids,
            root_entity_cls=root_entity_cls,
        )

    def _match_placeholder_tree(
        self,
        *,
        ingested_placeholder_tree: EntityTree,
        db_entity_trees: List[EntityTree],
        matched_entities_by_db_ids: Dict[int, List[DatabaseEntity]],
        root_entity_cls,
    ) -> IndividualMatchResult:
        """Attempts to match the provided |ingested_placeholder_tree| to
        entities in the provided |db_entity_trees| based off any child matches.
        When such a match is found, the child is moved off of the ingested
        entity and onto the matched db entity.
        Returns the results of matching as an IndividualMatchResult.
        """
        error_count = 0
        match_results_by_child = self._get_match_results_for_all_children(
            ingested_entity_tree=ingested_placeholder_tree,
            db_entity_trees=db_entity_trees,
            root_entity_cls=root_entity_cls,
        )

        # Initialize so pylint doesn't yell.
        child_field_name = None
        child_match_result = None
        placeholder_children: List[DatabaseEntity] = []
        updated_entity_trees: List[EntityTree] = []

        def resolve_child_placeholder_match_result():
            """Resolves any child matches by removing the child from the
            ingested placeholder entity and adding the child onto the
            corresponding DB entity.
            """

            if not child_field_name or not child_match_result:
                raise EntityMatchingError(
                    f"Expected child_field_name and child_match_result to be set, but instead got [{child_field_name}] "
                    f"and [{child_match_result}] respectively for entity [{ingested_placeholder_tree.entity}].",
                    ingested_placeholder_tree.entity.get_entity_name(),
                )

            if not child_match_result.merged_entity_trees:
                raise EntityMatchingError(
                    f"Expected child_match_result.merged_entity_trees to not be empty for "
                    f"[{ingested_placeholder_tree.entity.get_entity_name()}.{child_field_name}] on entity "
                    f"[{ingested_placeholder_tree.entity}].",
                    ingested_placeholder_tree.entity.get_entity_name(),
                )

            for merged_child_tree in child_match_result.merged_entity_trees:
                self.check_no_ingest_objs([merged_child_tree.entity])

            # Ensure the merged children are on the correct entity
            for merged_child_tree in child_match_result.merged_entity_trees:
                merged_parent_tree = merged_child_tree.generate_parent_tree()

                # If one of the merged parents is the ingested placeholder
                # entity, simply keep track of the child in placeholder_
                # children.
                if merged_parent_tree.entity == ingested_placeholder_tree.entity:
                    placeholder_children.append(merged_child_tree.entity)
                    continue

                add_child_to_entity(
                    entity=merged_parent_tree.entity,
                    child_field_name=child_field_name,
                    child_to_add=merged_child_tree.entity,
                )

                # Keep track of all db parents of the merged children.
                updated_entities = [m.entity for m in updated_entity_trees]
                if merged_parent_tree.entity not in updated_entities:
                    self._add_match_to_matched_entities_cache(
                        db_entity_match=merged_parent_tree.entity,
                        ingested_entity=ingested_placeholder_tree.entity,
                        matched_entities_by_db_ids=matched_entities_by_db_ids,
                    )
                    updated_entity_trees.append(merged_parent_tree)

        db_placeholder = self.get_or_create_db_entity_to_persist(
            ingested_placeholder_tree.entity
        )
        db_placeholder_tree = EntityTree(
            entity=db_placeholder,
            ancestor_chain=ingested_placeholder_tree.ancestor_chain,
        )

        for child_field_name, match_results in match_results_by_child:
            placeholder_children = []
            error_count += match_results.error_count
            for child_match_result in match_results.individual_match_results:
                resolve_child_placeholder_match_result()

            self.check_no_ingest_objs(placeholder_children)

            db_placeholder_tree.entity.set_field_from_list(
                child_field_name, placeholder_children
            )

        if db_placeholder_tree.entity.get_id():
            # If this object has already been committed to the DB, we always return it
            # as a result.
            updated_entity_trees.append(db_placeholder_tree)
        else:
            # If we updated any of the entity trees, check to see if the placeholder
            # tree still has any children. If it doesn't have any children, it
            # doesn't need to be committed into our DB.
            set_child_fields = get_set_entity_field_names(
                db_placeholder_tree.entity,
                entity_field_type=EntityFieldType.FORWARD_EDGE,
            )
            if set_child_fields:
                updated_entity_trees.append(db_placeholder_tree)

        return IndividualMatchResult(
            merged_entity_trees=updated_entity_trees, error_count=error_count
        )

    def get_or_create_db_entity_to_persist(
        self, entity: DatabaseEntity
    ) -> DatabaseEntity:
        """If the provided |entity| has already been persisted to our DB,
        returns the entity itself with all relationships preserved.
        Otherwise returns a shallow copy of the entity with no relationship
        fields set.
        """
        if entity.get_id() is not None:
            return entity

        # Perform shallow copy
        ent_cls = entity.__class__
        new_entity = ent_cls()
        for field in get_all_core_entity_field_names(
            entity, EntityFieldType.FLAT_FIELD
        ):
            new_entity.set_field(field, entity.get_field(field))

        return new_entity

    def _match_unmatched_tree(
        self,
        ingested_unmatched_entity_tree: EntityTree,
        db_entity_trees: List[EntityTree],
        root_entity_cls,
    ) -> IndividualMatchResult:
        """
        Attempts to match the provided |ingested_unmatched_entity_tree| to any
        placeholder DB trees in the provided |db_entity_trees| based off of any
        child matches. When such a match is found, the merged child is moved off
        of the placeholder DB entity and onto the ingested entity.
        Returns the results of matching as an IndividualMatchResult.
        """

        db_placeholder_trees = [
            tree for tree in db_entity_trees if self._is_placeholder(tree.entity)
        ]

        error_count = 0
        match_results_by_child = self._get_match_results_for_all_children(
            ingested_entity_tree=ingested_unmatched_entity_tree,
            db_entity_trees=db_placeholder_trees,
            root_entity_cls=root_entity_cls,
        )

        # If the ingested entity is updated because of a child entity match, we
        # should update our ingested entity's ancestor chain to reflect that of
        # it's counterpart DB. This is necessary for above layers of entity
        # matching which rely on knowing the parent of any merged entities.
        ancestor_chain_updated: List[DatabaseEntity] = []

        # Initialize so pylint doesn't yell.
        child_match_result = None
        child_field_name = None

        def resolve_child_match_result():
            """Resolves any child matches by moving matched children off of
            their DB placeholder parent and onto the ingested, unmatched entity.
            """
            if not child_field_name or not child_match_result:
                raise EntityMatchingError(
                    f"Expected child_field_name and child_match_result to be set, but instead got [{child_field_name}] "
                    f"and [{child_match_result}] respectively for entity [{ingested_unmatched_entity_tree.entity}].",
                    ingested_unmatched_entity_tree.entity.get_entity_name(),
                )

            if not child_match_result.merged_entity_trees:
                raise EntityMatchingError(
                    f"Expected child_match_result.merged_entity_trees to not be empty for "
                    f"[{ingested_unmatched_entity_tree.entity.get_entity_name()}.{child_field_name}] on entity "
                    f"[{ingested_unmatched_entity_tree.entity}].",
                    ingested_unmatched_entity_tree.entity.get_entity_name(),
                )

            # For each matched child, remove child from the DB placeholder and
            # keep track of merged child(ren).
            for merged_child_tree in child_match_result.merged_entity_trees:
                updated_child_trees.append(merged_child_tree)
                placeholder_tree = merged_child_tree.generate_parent_tree()
                remove_child_from_entity(
                    entity=placeholder_tree.entity,
                    child_field_name=child_field_name,
                    child_to_remove=merged_child_tree.entity,
                )

                # TODO(#2505): Avoid changing ancestor chains if possible.
                # For now we only handle the case where all placeholders with
                # matched children have the same parent chain. If they do not,
                # we throw an error.
                if ancestor_chain_updated:
                    if ancestor_chain_updated != placeholder_tree.ancestor_chain:
                        raise EntityMatchingError(
                            f"Expected all placeholder DB entities matched to an ingested unmatched entity to have the "
                            f"same ancestor chain, but they did not for entity [{ingested_entity}]. "
                            f"Found conflicting ancestor chains: "
                            f"{ancestor_chain_updated} and {placeholder_tree.ancestor_chain}",
                            ingested_entity.get_entity_name(),
                        )
                else:
                    ancestor_chain_updated.extend(placeholder_tree.ancestor_chain)

        ingested_entity = ingested_unmatched_entity_tree.entity

        new_db_entity = self.get_or_create_db_entity_to_persist(
            ingested_unmatched_entity_tree.entity
        )
        for child_field_name, match_results in match_results_by_child:
            error_count += match_results.error_count
            ingested_child_field = ingested_entity.get_field(child_field_name)
            updated_child_trees: List[EntityTree] = []
            for child_match_result in match_results.individual_match_results:
                resolve_child_match_result()

            # Update the ingested entity copy with the updated child(ren).
            updated_children = [mc.entity for mc in updated_child_trees]
            self.check_no_ingest_objs(updated_children)
            if isinstance(ingested_child_field, list):
                new_db_entity.set_field(child_field_name, updated_children)
            else:
                new_db_entity.set_field(child_field_name, one(updated_children))

        updated_entities = []
        if ancestor_chain_updated:
            updated_entities.append(
                EntityTree(entity=new_db_entity, ancestor_chain=ancestor_chain_updated)
            )
        else:
            updated_entities.append(
                EntityTree(
                    entity=new_db_entity,
                    ancestor_chain=ingested_unmatched_entity_tree.ancestor_chain,
                )
            )

        return IndividualMatchResult(
            merged_entity_trees=updated_entities, error_count=error_count
        )

    def _match_matched_tree(
        self,
        *,
        ingested_entity_tree: EntityTree,
        db_match_tree: EntityTree,
        matched_entities_by_db_ids: Dict[int, List[DatabaseEntity]],
        root_entity_cls,
    ) -> IndividualMatchResult:
        """Given an |ingested_entity_tree| and it's matched |db_match_tree|,
        this method merges any updated information from teh ingested entity
        onto the DB entity and then continues entity matching for all children
        of the provided objects.
        Returns the results of matching as an IndividualMatchResult.
        """
        ingested_entity = ingested_entity_tree.entity
        db_entity = db_match_tree.entity

        self._add_match_to_matched_entities_cache(
            db_entity_match=db_entity,
            ingested_entity=ingested_entity,
            matched_entities_by_db_ids=matched_entities_by_db_ids,
        )
        error_count = 0
        match_results_by_child = self._get_match_results_for_all_children(
            ingested_entity_tree=ingested_entity_tree,
            db_entity_trees=[db_match_tree],
            root_entity_cls=root_entity_cls,
        )

        # Initialize so pylint doesn't yell
        child_match_result = None
        child_field_name = None

        def resolve_child_match_result():
            """Keeps track of all matched and unmatched children."""
            if not child_match_result:
                raise EntityMatchingError(
                    f"Expected child_match_result to be set for "
                    f"{ingested_entity_tree.entity.get_entity_name()}.{child_field_name} on entity "
                    f"[{ingested_entity_tree.entity}], but instead got {child_match_result}",
                    ingested_entity_tree.entity.get_entity_name(),
                )

            if not child_match_result.merged_entity_trees:
                raise EntityMatchingError(
                    f"Expected child_match_result.merged_entity_trees on entity [{ingested_entity_tree.entity}] to not "
                    f"be empty for [{ingested_entity_tree.entity.get_entity_name()}.{child_field_name}].",
                    ingested_entity_tree.entity.get_entity_name(),
                )

            for merged_child_tree in child_match_result.merged_entity_trees:
                updated_child_trees.append(merged_child_tree)
                remove_child_from_entity(
                    child_to_remove=merged_child_tree.entity,
                    child_field_name=child_field_name,
                    entity=ingested_entity,
                )

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
                self.check_no_ingest_objs(updated_children)
                db_entity.set_field(child_field_name, updated_children)
            else:
                if match_results.unmatched_db_entities and not is_standalone_entity(
                    ingested_child_field
                ):
                    raise EntityMatchingError(
                        f"Singular ingested entity field {ingested_entity.get_entity_name()}.{child_field_name} "
                        f"with value: {ingested_child_field} should match one of the provided db options, but it does "
                        f"not for entity [{ingested_entity}]. Found unmatched db entities: "
                        f"{match_results.unmatched_db_entities}",
                        ingested_entity.get_entity_name(),
                    )
                self.check_no_ingest_objs(updated_children)
                db_entity.set_field(child_field_name, one(updated_children))

        merged_entity = self.state_matching_delegate.merge_flat_fields(
            from_entity=ingested_entity, to_entity=db_entity
        )
        merged_entity_tree = EntityTree(
            entity=merged_entity, ancestor_chain=db_match_tree.ancestor_chain
        )

        # Clear flat fields off of the ingested entity after entity matching
        # is complete. This ensures that if this `ingested_entity` has
        # multiple parents, each time we reach this entity, we'll correctly
        # match it to the correct `db_entity`. It is possible that the ingested and db
        # entities are the same object (when `match_trees` is called on entities within
        # the same person tree in post processing). In this case, do not clear flat fields.
        if id(ingested_entity) != id(db_entity):
            self.entities_to_convert_to_placeholder_or_expunge.append(ingested_entity)

        return IndividualMatchResult(
            merged_entity_trees=[merged_entity_tree], error_count=error_count
        )

    def _get_match_results_for_all_children(
        self,
        ingested_entity_tree: EntityTree,
        db_entity_trees: List[EntityTree],
        root_entity_cls,
    ) -> List[Tuple[str, MatchResults]]:
        """Attempts to match all children of the |ingested_entity_tree| to
        children of the |db_entity_trees|. Matching for each child is
        independent and can match to different DB parents.
        Returns a list of tuples with the following values:
        - str: the string name of the child field
        - MatchResult: the result of matching this child field to children of
            the provided |db_entity_trees|
        """
        results = []
        ingested_entity = ingested_entity_tree.entity
        set_child_fields = get_set_entity_field_names(
            ingested_entity, EntityFieldType.FORWARD_EDGE
        )

        for child_field_name in set_child_fields:
            ingested_child_field = ingested_entity.get_field(child_field_name)
            db_child_trees = generate_child_entity_trees(
                child_field_name, db_entity_trees
            )
            if isinstance(ingested_child_field, list):
                ingested_child_list = ingested_child_field
            else:
                ingested_child_list = [ingested_child_field]

            ingested_child_trees = ingested_entity_tree.generate_child_trees(
                ingested_child_list
            )
            match_results = self._match_entity_trees(
                ingested_entity_trees=ingested_child_trees,
                db_entity_trees=db_child_trees,
                root_entity_cls=root_entity_cls,
            )
            results.append((child_field_name, match_results))
        return results

    def _add_match_to_matched_entities_cache(
        self,
        *,
        db_entity_match: DatabaseEntity,
        ingested_entity: DatabaseEntity,
        matched_entities_by_db_ids: Dict[int, List[DatabaseEntity]],
    ) -> None:
        """If the provided |db_entity_match| and |ingested_entity| represent a
        new match, records the match in |matched_entities_by_db_ids|. Throws an
        error if this new match is disallowed.
        """
        matched_db_id = db_id_or_object_id(db_entity_match)
        if matched_db_id not in matched_entities_by_db_ids:
            matched_entities_by_db_ids[matched_db_id] = [ingested_entity]
            return

        # Don't add the same entity twice.
        ingested_matches = matched_entities_by_db_ids[matched_db_id]
        if ingested_entity in ingested_matches:
            return

        ingested_matches.append(ingested_entity)
        # It's ok for a DB object to match multiple ingested placeholders.
        if all(self._is_placeholder(im) for im in ingested_matches):
            return

        # Entities with multiple external ids can be matched multiple times.
        if is_multiple_id_entity(db_entity_match):
            return

        raise MatchedMultipleIngestedEntitiesError(db_entity_match, ingested_matches)

    def get_cached_matches(self, entity: DatabaseEntity) -> List[EntityTree]:
        external_ids = get_external_ids_from_entity(entity)
        cached_matches = []
        cached_match_ids: Set[int] = set()

        for external_id in external_ids:
            cached_trees = self.root_entity_cache[external_id]
            for cached_tree in cached_trees:
                if cached_tree.entity.get_id() not in cached_match_ids:
                    cached_match_ids.add(cached_tree.entity.get_id())
                    cached_matches.append(cached_tree)
        return cached_matches

    def _get_match(
        self, ingested_entity_tree: EntityTree, db_entity_trees: List[EntityTree]
    ) -> Optional[EntityTree]:
        """With the provided |ingested_entity_tree|, this attempts to find a
        match among the provided |db_entity_trees|. If a match is found, it is
        returned.
        """
        db_match_candidates = db_entity_trees
        if isinstance(ingested_entity_tree.entity, self.root_entity_cls):
            db_match_candidates = self.get_cached_matches(ingested_entity_tree.entity)

        # Entities that can have multiple external IDs need special casing to
        # handle the fact that multiple DB entities could match the provided
        # ingested entity.
        if is_multiple_id_entity(ingested_entity_tree.entity):
            exact_match = self._get_only_match_for_multiple_id_entity(
                ingested_entity_tree=ingested_entity_tree,
                db_entity_trees=db_match_candidates,
            )
        else:
            exact_match = entity_matching_utils.get_only_match(
                ingested_entity_tree, db_match_candidates, is_match
            )

        if not exact_match:
            exact_match = self.state_matching_delegate.get_non_external_id_match(
                ingested_entity_tree, db_entity_trees
            )

        if exact_match is None:
            return exact_match

        if not isinstance(exact_match, EntityTree):
            raise ValueError(
                f"Bad return value of type [{type(exact_match)}] for exact match [{exact_match}]."
            )

        return exact_match

    def _get_only_match_for_multiple_id_entity(
        self, ingested_entity_tree: EntityTree, db_entity_trees: List[EntityTree]
    ) -> Optional[EntityTree]:
        """Returns the DB EntityTree from |db_entity_trees| that matches
        the provided |ingested_entity_tree|, if one exists.

        - If no match exists, returns None.
        - If one match exists, returns the singular match
        - If multiple DB entities match, this method squashes all information
          from the matches onto one of the DB matches and returns that DB match.
          The other, non-returned matches are transformed into placeholders
          (all fields removed save primary keys).
        """

        db_matches = entity_matching_utils.get_all_matches(
            ingested_entity_tree, db_entity_trees, is_match
        )

        if not db_matches:
            return None

        # Merge all duplicate persons into the one db match
        match_tree = db_matches[0]
        for duplicate_match_tree in db_matches[1:]:
            self._match_matched_tree(
                ingested_entity_tree=duplicate_match_tree,
                db_match_tree=match_tree,
                matched_entities_by_db_ids={},
                root_entity_cls=ingested_entity_tree.entity.__class__,
            )
        return match_tree

    def merge_multiparent_entities(self, persons: List[schema.StatePerson]) -> None:
        """For each person in the provided |persons|, looks at all of the child
        entities that can have multiple parents, and merges these entities where
        possible.
        """
        for person in persons:
            for cls in get_multiparent_classes():
                if cls not in self.non_placeholder_ingest_types:
                    continue
                entity_map: Dict[str, List[_EntityWithParentInfo]] = {}
                self._populate_multiparent_map(person, cls, entity_map)
                self._merge_multiparent_entities_from_map(entity_map)

    def _merge_multiparent_entities_from_map(
        self, multiparent_map: Dict[str, List[_EntityWithParentInfo]]
    ) -> None:
        """Merges entities from the provided |multiparent_map|."""
        for _external_id, entities_with_parents in multiparent_map.items():
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
                elif merged_entity_with_parents.entity.get_id():
                    db_with_parents = merged_entity_with_parents
                    ing_with_parents = entity_with_parents
                else:
                    db_with_parents = merged_entity_with_parents
                    ing_with_parents = entity_with_parents

                # Merge the two objects via entity matching
                db_tree = EntityTree(db_with_parents.entity, [])
                ing_tree = EntityTree(ing_with_parents.entity, [])

                match_results = self._match_entity_trees(
                    ingested_entity_trees=[ing_tree],
                    db_entity_trees=[db_tree],
                    root_entity_cls=ing_tree.entity.__class__,
                )
                # TODO(#2760): Report post processing error counts
                if match_results.error_count:
                    logging.error(
                        "Attempting to merge multiparent entities of type "
                        "[%s], with external_ids [%s] and [%s] but encountered "
                        "[%s] entity matching errors",
                        ing_tree.entity.__class__.__name__,
                        ing_tree.entity.get_external_id(),
                        db_tree.entity.get_external_id(),
                        str(match_results.error_count),
                    )
                updated_entity = db_tree.entity

                # As entity matching automatically updates the input db entity,
                # we only have to replace ing_with_parents.entity.
                self._replace_entity(
                    entity=updated_entity,
                    to_replace=ing_with_parents.entity,
                    linked_parents=ing_with_parents.linked_parents,
                )
                db_with_parents.linked_parents.extend(ing_with_parents.linked_parents)
                merged_entity_with_parents = db_with_parents

    def _is_subset(self, entity: DatabaseEntity, subset: DatabaseEntity) -> bool:
        """Checks if all fields on the provided |subset| are present in the
        provided |entity|. Returns True if so, otherwise False.
        """
        for field_name in get_set_entity_field_names(
            subset, EntityFieldType.FLAT_FIELD
        ):
            if entity.get_field(field_name) != entity.get_field(field_name):
                return False
        for field_name in get_set_entity_field_names(
            subset, EntityFieldType.FORWARD_EDGE
        ):
            for field in subset.get_field_as_list(field_name):
                if field not in entity.get_field_as_list(field_name):
                    return False
        return True

    def _replace_entity(
        self,
        *,
        entity: DatabaseEntity,
        to_replace: DatabaseEntity,
        linked_parents: List[_ParentInfo],
    ) -> None:
        """For all parent entities in |to_replace_parents|, replaces
        |to_replace| with |entity|.
        """
        for linked_parent in linked_parents:
            remove_child_from_entity(
                entity=linked_parent.parent,
                child_field_name=linked_parent.field_name,
                child_to_remove=to_replace,
            )
            add_child_to_entity(
                entity=linked_parent.parent,
                child_field_name=linked_parent.field_name,
                child_to_add=entity,
            )

    def _populate_multiparent_map(
        self,
        entity: DatabaseEntity,
        entity_cls: Type[DatabaseEntity],
        multiparent_map: Dict[str, List[_EntityWithParentInfo]],
    ) -> None:
        """Looks through all children in the provided |entity|, and if they are
        of type |entity_cls|, adds an entry to the provided |multiparent_map|.
        """
        for child_field_name in get_set_entity_field_names(
            entity, EntityFieldType.FORWARD_EDGE
        ):
            linked_parent = _ParentInfo(entity, child_field_name)
            for child in entity.get_field_as_list(child_field_name):
                self._populate_multiparent_map(child, entity_cls, multiparent_map)

                if not isinstance(child, entity_cls):
                    continue

                # All persistence entities have external ids
                external_id = child.get_external_id()

                # We're only matching entities if they have the same
                # external_id.
                if not external_id:
                    continue

                if external_id in multiparent_map.keys():
                    entities_with_parents = multiparent_map[external_id]
                    found_entity = False

                    # If the child object itself has already been seen, simply
                    # add the |entity| parent to the list of linked parents
                    for entity_with_parents in entities_with_parents:
                        if id(entity_with_parents.entity) == id(child):
                            found_entity = True
                            entity_with_parents.linked_parents.append(linked_parent)

                    # If the child object has not been seen, create a new
                    # _EntityWithParentInfo object for this external_id
                    if not found_entity:
                        entity_with_parents = _EntityWithParentInfo(
                            child, [linked_parent]
                        )
                        entities_with_parents.append(entity_with_parents)

                # If the external_id has never been seen before, create a new
                # entry for it.
                else:
                    entity_with_parents = _EntityWithParentInfo(child, [linked_parent])
                    multiparent_map[external_id] = [entity_with_parents]
