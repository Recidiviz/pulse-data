# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Class responsible for merging ingested entity trees into as few trees with as
few placeholder nodes as possible.
"""

from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from recidiviz.persistence.entity.entity_deserialize import Entity, EntityT
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    EntityFieldType,
    get_flat_fields_json_str,
    is_placeholder,
)
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.errors import EntityMatchingError


class StateIngestedTreeMerger:
    """Class responsible for merging ingested entity trees into as few trees with as
    few placeholder nodes as possible.
    """

    def __init__(self, field_index: CoreEntityFieldIndex) -> None:
        self.field_index = field_index

    def merge(
        self, ingested_persons: List[entities.StatePerson]
    ) -> List[entities.StatePerson]:
        """Merges all ingested StatePeople trees that can be connected via external_id.

        Returns the list of unique StatePeople after this merging.
        """

        buckets = self.bucket_ingested_persons(ingested_persons)

        unique_persons: List[entities.StatePerson] = []

        # Merge each bucket into one person
        for people_to_merge in buckets:
            unique_person, _ = self._merge_matched_tree_group(people_to_merge)
            if unique_person:
                unique_persons.append(unique_person)

        return unique_persons

    @classmethod
    def bucket_ingested_persons(
        cls,
        ingested_persons: List[entities.StatePerson],
    ) -> List[List[entities.StatePerson]]:
        """Buckets the list of ingested persons into groups that all should be merged
        into the same person, based on their external ids. Each inner list in the
        returned list should be merged into one person.
        """

        result_buckets: List[List[entities.StatePerson]] = []

        # First bucket all the people that should be merged
        bucketed_persons_dict: Dict[str, List[entities.StatePerson]] = defaultdict(list)
        external_id_key_to_primary: Dict[str, str] = {}
        for person in ingested_persons:
            external_id_keys = cls._person_external_id_keys(person)
            if len(external_id_keys) == 0:
                # We don't merge placeholder people
                result_buckets.append([person])
                continue

            # Find all the people who should be related to this person based on their
            # external_ids and merge them into one bucket.
            merged_bucket = [person]
            primary_buckets_to_merge = set()
            for external_id_key in external_id_keys:
                if external_id_key in external_id_key_to_primary:
                    primary_id_for_id = external_id_key_to_primary[external_id_key]
                    primary_buckets_to_merge.add(primary_id_for_id)

            for external_id_key in primary_buckets_to_merge:
                if external_id_key in bucketed_persons_dict:
                    merged_bucket.extend(bucketed_persons_dict.pop(external_id_key))

            # Deterministically pick one of the ids to be the new primary id for this
            # merged bucket.
            all_primary_id_candidates = primary_buckets_to_merge.union(external_id_keys)
            primary_id = min(all_primary_id_candidates)

            for bucket_person in merged_bucket:
                for external_id_key in cls._person_external_id_keys(bucket_person):
                    external_id_key_to_primary[external_id_key] = primary_id

            bucketed_persons_dict[primary_id] = merged_bucket

        for bucket in bucketed_persons_dict.values():
            result_buckets.append(bucket)

        return result_buckets

    def _merge_matched_tree_group(
        self, entity_group: List[EntityT]
    ) -> Tuple[Optional[EntityT], Set[int]]:
        """Recursively merge the list of entities into a single entity and returns a
        tuple containing a) entity, or None if the list is empty and b) a set of all
        python object ids present in the entity group.
        """
        if not entity_group:
            return None, set()

        seen_objects: Set[int] = set()
        flat_field_reprs: Set[str] = set()
        for entity in entity_group:
            seen_objects.add(id(entity))
            flat_field_reprs.add(get_flat_fields_json_str(entity, self.field_index))

        # Get the entity that will become the merged entity
        primary_entity = entity_group[0]

        # Make sure all the root entities have the exact same flat field values (e.g.
        # everything other than relationship fields) as the others so we are justified
        # in merging.
        if len(flat_field_reprs) > 1:
            # If there is more than one string representation of the flat fields, then
            # we have objects with conflicting info that we are trying to merge.
            raise EntityMatchingError(
                f"Found multiple different ingested entities of type "
                f"[{primary_entity.__class__.__name__}] with conflicting "
                f"information: {[e.limited_pii_repr() for e in entity_group]}",
                entity_name=primary_entity.get_entity_name(),
            )

        children_by_field = self._get_children_grouped_by_field(entity_group)

        # Merge each child group into a set of merged children and attach to primary
        # entity.
        for field, child_list in children_by_field.items():
            groups = self._bucket_ingested_single_id_entities(child_list)
            merged_children = []
            for group in groups:
                merged_child, group_seen_objects = self._merge_matched_tree_group(group)

                if seen_objects.intersection(group_seen_objects):
                    # If we have made it here, there are multiple paths to one or more
                    # objects so the input is not a tree.
                    raise ValueError(
                        f"Already have seen one of the objects in [{merged_child}] "
                        f"- input is not a tree."
                    )
                seen_objects.update(group_seen_objects)

                if merged_child:
                    merged_children.append(merged_child)
            primary_entity.set_field_from_list(field, merged_children)

        # TODO(#7908): Collect non_placeholder_ingest_types here so we have a
        #  comprehensive list.
        return primary_entity, seen_objects

    def _bucket_ingested_single_id_entities(
        self, entity_list: List[EntityT]
    ) -> List[List[EntityT]]:
        """Buckets the list of ingested entities into groups that should be merged
        into the same entity, based on their external ids or the contents of their flat
        fields. This function assumes each entity in the list has an identical parent
        chain that will be merged.
        """

        placeholders = []
        non_placeholder_buckets_dict: Dict[str, List[EntityT]] = defaultdict(list)
        for entity in entity_list:
            if is_placeholder(entity, self.field_index):
                placeholders.append(entity)
            else:
                non_placeholder_buckets_dict[
                    self._get_non_placeholder_ingested_entity_key(entity)
                ].append(entity)

        buckets = self._bucket_ingested_placeholder_entities(placeholders)
        buckets.extend([list(b) for b in non_placeholder_buckets_dict.values()])
        return buckets

    def _bucket_ingested_placeholder_entities(
        self, placeholders: List[EntityT]
    ) -> List[List[EntityT]]:
        """Buckets the list of ingested placeholder entities into groups that should be
        merged into the same entity, based on their direct children. If two placeholders
        have hydrated children on a 1:1 relationship field (e.g. StateCourtCase <->
        StateAgent) then they cannot be merged because their children cannot be merged.

        This function assumes each entity in the list has an identical parent chain that
        will be merged.
        """
        requires_exact_match_child_keys: Dict[int, Dict[str, Set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        for placeholder in placeholders:
            for child_field in self.field_index.get_fields_with_non_empty_values(
                placeholder, EntityFieldType.FORWARD_EDGE
            ):
                child = placeholder.get_field(child_field)
                if child and not isinstance(child, list):
                    if not is_placeholder(child, self.field_index):
                        requires_exact_match_child_keys[id(placeholder)][
                            child_field
                        ].add(self._get_non_placeholder_ingested_entity_key(child))

        placeholder_buckets: List[List[EntityT]] = []

        def _add_placeholder_to_buckets(p: EntityT) -> None:
            """Finds the bucket this placeholder object should belong to
            and adds it to that bucket, or creates a new one if no bucket exists.
            """
            child_keys_by_field = requires_exact_match_child_keys[id(p)]
            for placeholder_bucket in placeholder_buckets:
                if not placeholder_bucket:
                    raise ValueError("Expected only buckets with non-zero length.")
                bucket_child_keys_by_field = requires_exact_match_child_keys[
                    id(placeholder_bucket[0])
                ]

                if bucket_child_keys_by_field == child_keys_by_field:
                    placeholder_bucket.append(p)
                    return

            placeholder_buckets.append([p])

        for placeholder in placeholders:
            _add_placeholder_to_buckets(placeholder)
        return placeholder_buckets

    def _get_children_grouped_by_field(
        self, entity_group: List[EntityT]
    ) -> Dict[str, List[EntityT]]:
        """Find all direct children (following a single edge) across all entities and
        group them by field they're attached to.
        """

        children_by_field = defaultdict(list)
        for entity in entity_group:
            if non_empty_backedges := self.field_index.get_fields_with_non_empty_values(
                entity, EntityFieldType.BACK_EDGE
            ):
                raise ValueError(
                    f"Found non-empty backedge fields on entity with class "
                    f"[{entity.__class__.__name__}]: {non_empty_backedges}. Backedges "
                    f"are set automatically through entity matching and should not be "
                    f"set in parsing."
                )

            for field in self.field_index.get_fields_with_non_empty_values(
                entity, EntityFieldType.FORWARD_EDGE
            ):
                children_by_field[field].extend(entity.get_field_as_list(field))
        return children_by_field

    @staticmethod
    def _person_external_id_keys(person: entities.StatePerson) -> Set[str]:
        """Generates a set of unique string keys for a person's StatePersonExternalIds."""
        return {f"{e.external_id}|{e.id_type}" for e in person.external_ids}

    def _get_non_placeholder_ingested_entity_key(self, entity: Entity) -> str:
        """Returns a string key that can be used to bucket this non-placeholder entity."""
        external_id = entity.get_external_id()
        if not external_id or isinstance(entity, entities.StatePersonExternalId):
            return get_flat_fields_json_str(entity, self.field_index)

        return external_id
