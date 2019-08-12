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
"""Contains types used throughout state and county entity matching."""
from typing import List, Generic

import attr

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities import EntityPersonType
from recidiviz.persistence.errors import EntityMatchingError


# TODO(1907): Rename people -> persons
@attr.s(frozen=True, kw_only=True)
class MatchedEntities(Generic[EntityPersonType]):
    """
    Object that contains output for entity matching
    - people: List of all successfully matched and unmatched people.
        This list does NOT include any people for which Entity Matching raised
        an Exception.
    - orphaned entities: All entities that were orphaned during matching.
        These will need to be added to the session separately from the
        returned people.
    - error count: The number of errors raised while processing a root entity
    - total root entities: The total number of root entities processed during
        matching.
    """
    people: List[EntityPersonType] = attr.ib(factory=list)
    orphaned_entities: List[Entity] = attr.ib(factory=list)
    error_count: int = attr.ib(default=0)
    total_root_entities: int = attr.ib(default=0)

    def __add__(self, other):
        return MatchedEntities(
            people=self.people + other.people,
            orphaned_entities=self.orphaned_entities + other.orphaned_entities,
            error_count=self.error_count + other.error_count,
            total_root_entities=self.total_root_entities
            + other.total_root_entities)


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
                 merged_entity_trees: List[EntityTree], error_count: int):
        if not ingested_entity_tree:
            raise EntityMatchingError(
                "When creating IndividualMatchResult object, "
                "ingested_entity_tree field must be set",
                'individual_match_result')

        # The initial EntityTree to be matched to DB EntityTrees.
        self.ingested_entity_tree = ingested_entity_tree

        # If matching was successful, these are results of merging the
        # ingested_entity_tree with any of its DB matches.
        self.merged_entity_trees = merged_entity_trees

        # The number of errors encountered while matching this entity.
        self.error_count = error_count


class MatchResults:
    """Object that represents the results of a match attempt for a group of
    ingested and database EntityTree objects"""

    def __init__(self, individual_match_results: List[IndividualMatchResult],
                 unmatched_db_entities: List[Entity], error_count: int):
        # Results for each individual ingested EntityTree.
        self.individual_match_results = individual_match_results

        # List of db entities that were unmatched.
        self.unmatched_db_entities = unmatched_db_entities

        # The number of errors encountered while matching these entities.
        self.error_count = error_count
