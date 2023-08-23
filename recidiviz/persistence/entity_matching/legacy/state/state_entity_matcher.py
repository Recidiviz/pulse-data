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
from typing import Dict, Generic, List, Optional, Set, Tuple, Type

from more_itertools import one

from recidiviz.common.str_field_utils import to_snake_case
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state.dao import (
    check_not_dirty,
    read_root_entities_by_external_ids,
)
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_entities_to_schema,
)
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    get_all_db_objs_from_tree,
    get_all_db_objs_from_trees,
    is_placeholder,
    is_standalone_entity,
)
from recidiviz.persistence.entity_matching.ingest_view_tree_merger import (
    IngestViewTreeMerger,
)
from recidiviz.persistence.entity_matching.legacy import entity_matching_utils
from recidiviz.persistence.entity_matching.legacy.entity_matching_types import (
    EntityTree,
    IndividualMatchResult,
    MatchedEntities,
    MatchResults,
)
from recidiviz.persistence.entity_matching.legacy.monitoring import increment_error
from recidiviz.persistence.entity_matching.legacy.state.state_matching_utils import (
    EntityFieldType,
    add_child_to_entity,
    db_id_or_object_id,
    generate_child_entity_trees,
    get_all_root_entity_external_ids,
    get_multiparent_classes,
    get_root_entity_external_ids,
    is_match,
    is_multiple_id_entity,
    merge_flat_fields,
    remove_child_from_entity,
)
from recidiviz.persistence.entity_matching.legacy.state.state_specific_entity_matching_delegate import (
    StateSpecificEntityMatchingDelegate,
)
from recidiviz.persistence.errors import (
    EntityMatchingError,
    MatchedMultipleIngestedEntitiesError,
)
from recidiviz.persistence.persistence_utils import RootEntityT, SchemaRootEntityT
from recidiviz.utils.types import assert_type

# How many root entity trees to search to fill the non_placeholder_ingest_types set.
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
class StateEntityMatcher(Generic[RootEntityT, SchemaRootEntityT]):
    """Class that handles entity matching for all state data."""

    def __init__(
        self,
        root_entity_cls: Type[RootEntityT],
        schema_root_entity_cls: Type[SchemaRootEntityT],
        state_specific_logic_delegate: StateSpecificEntityMatchingDelegate,
    ) -> None:
        self.root_entity_cls: Type[RootEntityT] = root_entity_cls
        self.schema_root_entity_cls: Type[SchemaRootEntityT] = schema_root_entity_cls
        self.all_ingested_db_objs: Set[DatabaseEntity] = set()

        # Cache of root entity DB objects (e.g. objs of type schema.StatePerson)
        # keyed by their external ids.
        self.db_root_entities_by_external_id: Dict[str, List[EntityTree]] = defaultdict(
            list
        )

        # Set of class types in the ingested objects that are not placeholders
        self.non_placeholder_ingest_types: Set[Type[DatabaseEntity]] = set()

        # A list of entities that we want to either expunge from the session or delete
        # before we commit the session.
        self.entities_to_delete_or_expunge: List[DatabaseEntity] = []

        # Delegate object with all state specific logic
        self.state_specific_logic_delegate = state_specific_logic_delegate

        self.session: Optional[Session] = None

        self.field_index = CoreEntityFieldIndex()
        self.tree_merger = IngestViewTreeMerger(self.field_index)

    def set_session(self, session: Session) -> None:
        if self.session:
            raise ValueError(
                "Attempting to set self.session, but self.session is already set"
            )
        self.session = session

    def get_session(self) -> Session:
        if self.session is None:
            raise ValueError(
                "Attempting to get self.session, but self.session is not set"
            )
        return self.session

    def set_db_root_entity_cache(
        self, db_root_entities: List[SchemaRootEntityT]
    ) -> None:
        if not db_root_entities:
            return

        for db_root_entity in db_root_entities:
            external_ids = get_root_entity_external_ids(db_root_entity)
            tree = EntityTree(entity=db_root_entity, ancestor_chain=[])
            for external_id in external_ids:
                self.db_root_entities_by_external_id[external_id].append(tree)

    def _is_placeholder(self, entity: DatabaseEntity) -> bool:
        """Entity matching-specific wrapper around is_placeholder() that checks that
        we didn't fail to add any types to the non_placeholder_ingest_types set.
        """
        entity_is_placeholder = is_placeholder(entity, self.field_index)
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
            if hasattr(entity, "external_id") or isinstance(
                entity, self.schema_root_entity_cls
            ):
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
        self, ingested_db_root_entities: List[SchemaRootEntityT]
    ) -> Set[Type[DatabaseEntity]]:
        """Returns set of class types in the ingested root entities that are not
        placeholders.

        NOTE: This assumes that ingested trees are largely similar and all
        non-placeholder types will show up in a selected set of 20 root entity trees. If
         the first 20 do not surface all types, we will log an error later in the
        _is_placeholder() helper and entity matching might not work correctly.
        """
        non_placeholder_ingest_types: Set[Type[DatabaseEntity]] = set()

        indices_to_search = self.get_non_placeholder_ingest_types_indices_to_search(
            num_total_trees=len(ingested_db_root_entities)
        )

        for i in indices_to_search:
            ingested_root_entity = ingested_db_root_entities[i]
            for obj in get_all_db_objs_from_tree(
                ingested_root_entity, self.field_index
            ):
                if not is_placeholder(obj, self.field_index):
                    non_placeholder_ingest_types.add(type(obj))
        return non_placeholder_ingest_types

    def get_ingest_objs_from_list(
        self, db_objs: List[DatabaseEntity]
    ) -> Set[DatabaseEntity]:
        all_objs = get_all_db_objs_from_trees(db_objs, self.field_index)
        intersection = self.all_ingested_db_objs.intersection(all_objs)

        return intersection

    def describe_db_entity(self, entity: DatabaseEntity) -> str:
        external_id = (
            entity.get_external_id() if hasattr(entity, "external_id") else "N/A"
        )

        return (
            f"{entity.get_entity_name()}({id(entity)}) "
            f"external_id={external_id} placeholder={is_placeholder(entity, self.field_index)}"
        )

    def run_match(
        self,
        session: Session,
        ingested_root_entities: List[RootEntityT],
    ) -> MatchedEntities[SchemaRootEntityT]:
        """Attempts to match all root entities from |ingested_root_entities| with
        corresponding root entities in our database. Returns a MatchedEntities object
        that contains the results of matching.
        """

        ingested_root_entities = self.tree_merger.merge(ingested_root_entities)

        self.set_session(session)
        logging.info(
            "[Entity matching] Converting ingested entities to DB entities "
            "at time [%s].",
            datetime.datetime.now().isoformat(),
        )
        ingested_db_root_entities = [
            assert_type(p, self.schema_root_entity_cls)
            for p in convert_entities_to_schema(
                ingested_root_entities, populate_back_edges=False
            )
        ]
        self.non_placeholder_ingest_types = self.get_non_placeholder_ingest_types(
            ingested_db_root_entities
        )

        logging.info(
            "[Entity matching] Starting reading and converting root entities "
            "at time [%s].",
            datetime.datetime.now().isoformat(),
        )

        root_external_ids = get_all_root_entity_external_ids(ingested_db_root_entities)
        logging.info(
            "[Entity Matching] Reading entities of class schema.StatePerson using [%s] "
            "external ids",
            len(root_external_ids),
        )
        db_root_entities = read_root_entities_by_external_ids(
            session,
            schema_root_entity_cls=self.schema_root_entity_cls,
            cls_external_ids=root_external_ids,
            state_code=self.state_specific_logic_delegate.get_region_code(),
        )

        logging.info(
            "Read [%d] root entities from DB in region [%s]",
            len(db_root_entities),
            self.state_specific_logic_delegate.get_region_code(),
        )

        self.set_db_root_entity_cache(db_root_entities)

        logging.info(
            "[Entity matching] Completed DB read at time [%s].",
            datetime.datetime.now().isoformat(),
        )
        matched_entities_builder = self._run_match(
            ingested_db_root_entities, db_root_entities
        )

        # In order to maintain the invariant that all objects are properly
        # added to the Session when we return from entity_matching we
        # add new root entities to the session here. Adding a root entities to the
        # session that is already in-session (i.e. not new) has no effect.
        # pylint:disable=not-an-iterable
        for match_root_entity in matched_entities_builder.root_entities:
            session.add(match_root_entity)

        logging.info("[Entity matching] Session flush")
        session.flush()

        # Assert that all entities have been flushed before returning from
        # entity matching
        check_not_dirty(session)

        matched_entities: MatchedEntities[
            SchemaRootEntityT
        ] = matched_entities_builder.build()
        return matched_entities

    def _run_match(
        self,
        ingested_root_entities: List[SchemaRootEntityT],
        db_root_entities: List[SchemaRootEntityT],
    ) -> MatchedEntities.Builder:
        """Attempts to match |ingested_root_entities| with root entities in
        |db_root_entities|.Assumes no backedges are present in either
        |ingested_root_entities| or |db_root_entities|.
        """
        # Prevent automatic flushing of any entities associated with this
        # session until we have explicitly finished matching. This ensures
        # errors aren't thrown due to unexpected flushing of incomplete entity
        # relationships.
        session = self.get_session()
        with session.no_autoflush:
            logging.info("[Entity matching] Matching root entities")
            matched_entities_builder = self._match_root_entities(
                ingested_root_entities=ingested_root_entities,
                db_root_entities=db_root_entities,
            )
            logging.info(
                "[Entity matching] Matching root entities returned [%s] matched entities",
                len(matched_entities_builder.root_entities),
            )

            logging.info("[Entity matching] Matching post-processing")
            self._perform_match_postprocessing(
                matched_entities_builder.root_entities, db_root_entities
            )
        return matched_entities_builder

    def _perform_match_postprocessing(
        self,
        matched_root_entities: List[SchemaRootEntityT],
        db_root_entities: List[SchemaRootEntityT],
    ) -> None:
        logging.info("[Entity matching] Merge multi-parent entities")
        self.merge_multiparent_entities(matched_root_entities)

        # TODO(#2894): Create long term strategy for dealing with/rolling back partially
        #  updated DB entities.
        # Make sure root entity ids are added to all entities in the matched root
        # entities and also to all read DB root entities. We populate back edges on DB root
        # entities as a precaution in case entity matching failed for any of these root
        # entities after some updates had already taken place.
        logging.info("[Entity matching] Populate indirect root entity backedges")
        new_root_entities = [
            p for p in matched_root_entities if p.get_primary_key() is None
        ]
        self._populate_root_entity_backedges(new_root_entities)
        self._populate_root_entity_backedges(db_root_entities)

    def _match_root_entities(
        self,
        *,
        ingested_root_entities: List[SchemaRootEntityT],
        db_root_entities: List[SchemaRootEntityT],
    ) -> MatchedEntities.Builder:
        """Attempts to match all root entities from |ingested_root_entities| with the
        provided |db_root_entities|. Results are returned in the MatchedEntities
        object which contains all successfully matched and merged root entities as
        well as an error count that is incremented every time an error is raised
        matching an ingested root entity.

        All returned root entities have direct and indirect backedges set.
        """
        schema_root_entity_cls = self.schema_root_entity_cls
        root_entity_cls_name = to_snake_case(schema_root_entity_cls.__name__)
        db_root_entity_trees = [
            EntityTree(entity=db_root_entity, ancestor_chain=[])
            for db_root_entity in db_root_entities
        ]
        ingested_root_entity_trees = [
            EntityTree(entity=ingested_root_entity, ancestor_chain=[])
            for ingested_root_entity in ingested_root_entities
        ]

        total_root_entities = len(ingested_root_entities)
        root_entity_match_results = self._match_entity_trees(
            ingested_entity_trees=ingested_root_entity_trees,
            db_entity_trees=db_root_entity_trees,
            root_entity_cls=schema_root_entity_cls,
        )

        updated_root_entities: List[SchemaRootEntityT] = []
        for match_result in root_entity_match_results.individual_match_results:
            if not match_result.merged_entity_trees:
                raise EntityMatchingError(
                    "Expected match_result.merged_entity_trees to not be empty.",
                    root_entity_cls_name,
                )

            # It is possible that multiple ingested root entities match to the same
            # DB root entity, in which case we should only keep one reference to
            # that object.
            for merged_root_entity_tree in match_result.merged_entity_trees:
                if not isinstance(
                    merged_root_entity_tree.entity, schema_root_entity_cls
                ):
                    raise EntityMatchingError(
                        f"Expected merged_root_entity_tree.entity [{merged_root_entity_tree.entity}] to "
                        f"have type [{schema_root_entity_cls.__name__}].",
                        root_entity_cls_name,
                    )

                if merged_root_entity_tree.entity not in updated_root_entities:
                    updated_root_entities.append(merged_root_entity_tree.entity)

        self._populate_root_entity_backedges(updated_root_entities)

        matched_entities_builder = MatchedEntities.builder()
        matched_entities_builder.root_entities = updated_root_entities
        matched_entities_builder.error_count = root_entity_match_results.error_count
        matched_entities_builder.total_root_entities = total_root_entities
        return matched_entities_builder

    def _populate_root_entity_backedges(
        self, root_entities: List[SchemaRootEntityT]
    ) -> None:
        back_edge_field_name = self.root_entity_cls.back_edge_field_name()
        for root_entity in root_entities:
            children = get_all_db_objs_from_tree(root_entity, self.field_index)
            for child in children:
                if child is not root_entity and not is_standalone_entity(child):
                    child.set_field(back_edge_field_name, root_entity)

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
            if db_id not in matched_entities_by_db_id:
                unmatched_db_entities.append(db_entity)

        for entity in self.entities_to_delete_or_expunge:
            self._delete_or_expunge_entity(entity)
        self.entities_to_delete_or_expunge.clear()

        return MatchResults(
            individual_match_results, unmatched_db_entities, error_count
        )

    def _delete_or_expunge_entity(self, entity: DatabaseEntity) -> None:
        """If the provided entity is committed to the DB already, then deletes it from
        the session so it will be deleted at commit time. Otherwise, expunges it from
        the session so it is not committed.
        """
        session = self.get_session()
        if entity.get_id():
            session.delete(entity)
        else:
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
        root_entity_cls: Type[DatabaseEntity],
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

        def resolve_child_placeholder_match_result() -> None:
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
            set_child_fields = self.field_index.get_fields_with_non_empty_values(
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
        for field in self.field_index.get_all_core_entity_fields(
            type(entity), EntityFieldType.FLAT_FIELD
        ):
            new_entity.set_field(field, entity.get_field(field))

        return new_entity

    def _match_unmatched_tree(
        self,
        ingested_unmatched_entity_tree: EntityTree,
        db_entity_trees: List[EntityTree],
        root_entity_cls: Type[DatabaseEntity],
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

        def resolve_child_match_result() -> None:
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
        root_entity_cls: Type[DatabaseEntity],
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

        def resolve_child_match_result() -> None:
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
                db_entity.set_field(child_field_name, one(updated_children))

        merged_entity = merge_flat_fields(
            new_entity=ingested_entity,
            old_entity=db_entity,
            field_index=self.field_index,
        )

        merged_entity_tree = EntityTree(
            entity=merged_entity, ancestor_chain=db_match_tree.ancestor_chain
        )

        # We have merged the source (ingested) entity onto the destination (DB) entity,
        # making the destination entity the source of truth. If source and destination
        # are not the same object, we want to either a) delete the source
        # entity out of the database if it has already been committed, or b) expunge it
        # from the session so it does not get committed.
        if id(ingested_entity) != id(db_entity):
            self.entities_to_delete_or_expunge.append(ingested_entity)

        return IndividualMatchResult(
            merged_entity_trees=[merged_entity_tree], error_count=error_count
        )

    def _get_match_results_for_all_children(
        self,
        ingested_entity_tree: EntityTree,
        db_entity_trees: List[EntityTree],
        root_entity_cls: Type[DatabaseEntity],
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
        set_child_fields = self.field_index.get_fields_with_non_empty_values(
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
        if is_multiple_id_entity(db_entity_match.__class__):
            return

        raise MatchedMultipleIngestedEntitiesError(db_entity_match, ingested_matches)

    def get_cached_matches(self, db_root_entity: SchemaRootEntityT) -> List[EntityTree]:
        external_ids = get_root_entity_external_ids(db_root_entity)
        cached_matches = []
        cached_match_ids: Set[int] = set()

        for external_id in external_ids:
            cached_trees = self.db_root_entities_by_external_id[external_id]
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
        if isinstance(
            ingested_entity_tree.entity,
            self.schema_root_entity_cls,
        ):
            db_match_candidates = self.get_cached_matches(ingested_entity_tree.entity)

        # Entities that can have multiple external IDs need special casing to
        # handle the fact that multiple DB entities could match the provided
        # ingested entity.
        if is_multiple_id_entity(ingested_entity_tree.entity.__class__):
            exact_match = self._get_only_match_for_multiple_id_entity(
                ingested_entity_tree=ingested_entity_tree,
                db_entity_trees=db_match_candidates,
            )
        else:
            exact_match = entity_matching_utils.get_only_match(
                ingested_entity_tree, db_match_candidates, self.field_index, is_match
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
            ingested_entity_tree, db_entity_trees, self.field_index, is_match
        )

        if not db_matches:
            return None

        # Merge all duplicate root entities into the one db match
        match_tree = db_matches[0]
        for duplicate_match_tree in db_matches[1:]:
            result = self._match_matched_tree(
                ingested_entity_tree=duplicate_match_tree,
                db_match_tree=match_tree,
                matched_entities_by_db_ids={},
                root_entity_cls=ingested_entity_tree.entity.__class__,
            )
            match_tree = one(result.merged_entity_trees)
        return match_tree

    def merge_multiparent_entities(
        self, root_entities: List[SchemaRootEntityT]
    ) -> None:
        """For each entity in the provided |root_entities|, looks at all of the child
        entities that can have multiple parents, and merges these entities where
        possible.
        """
        for cls in get_multiparent_classes():
            if cls not in self.non_placeholder_ingest_types:
                continue
            logging.info(
                "[Entity matching] Merging multi-parent entities of class [%s]",
                cls.__name__,
            )
            for root_entity in root_entities:
                entity_map: Dict[str, List[_EntityWithParentInfo]] = {}
                self._populate_multiparent_map(root_entity, cls, entity_map)
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
        for field_name in self.field_index.get_fields_with_non_empty_values(
            subset, EntityFieldType.FLAT_FIELD
        ):
            if entity.get_field(field_name) != entity.get_field(field_name):
                return False
        for field_name in self.field_index.get_fields_with_non_empty_values(
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
        for child_field_name in self.field_index.get_fields_with_non_empty_values(
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

                if external_id in multiparent_map:
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
