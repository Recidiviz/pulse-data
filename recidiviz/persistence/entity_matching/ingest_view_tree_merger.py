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
"""Class responsible for merging hydrated entity trees from a *single ingest view
on a single day* into as few trees as possible.
"""
import json
import logging
from collections import defaultdict
from types import ModuleType
from typing import Any, Dict, List, Optional, Set, Tuple

from recidiviz.persistence.entity.base_entity import (
    EntityT,
    ExternalIdEntity,
    HasMultipleExternalIdsEntityT,
)
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_deserialize import Entity
from recidiviz.persistence.entity.entity_field_index import EntityFieldType
from recidiviz.persistence.entity.entity_utils import get_flat_fields_json_str
from recidiviz.persistence.entity_matching.entity_merger_utils import (
    root_entity_external_id_keys,
)


class EntityMergingError(Exception):
    """Raised when an error with entity merging is encountered."""

    def __init__(self, msg: str, entity_name: str):
        self.entity_name = entity_name
        super().__init__(msg)


class IngestViewTreeMerger:
    """Class responsible for merging hydrated entity trees from a *single ingest view
    on a single day* into as few trees as possible.
    """

    def __init__(self, entities_module: ModuleType) -> None:
        self.entities_module_context = entities_module_context_for_module(
            entities_module
        )
        self.field_index = self.entities_module_context.field_index()

    def merge(
        self,
        ingested_root_entities: List[HasMultipleExternalIdsEntityT],
        # TODO(#24679): Delete this argument entirely once
        #  INGEST_VIEW_TREE_MERGER_ERROR_EXEMPTIONS is empty and deleted.
        should_throw_on_conflicts: bool = True,
    ) -> List[HasMultipleExternalIdsEntityT]:
        """Merges all ingested root entity trees that can be connected via external_id.

        Returns the list of unique root entities after this merging.

        Throws if two merged trees provide conflicting information on the same entity.
        """

        buckets = self.bucket_ingested_root_entities(ingested_root_entities)

        unique_root_entities: List[HasMultipleExternalIdsEntityT] = []

        # Merge each bucket into one entity
        for root_entities_to_merge in buckets:
            unique_root_entity, _ = self._merge_matched_tree_group(
                root_entities_to_merge, should_throw_on_conflicts
            )
            if unique_root_entity:
                unique_root_entities.append(unique_root_entity)

        return unique_root_entities

    @classmethod
    def bucket_ingested_root_entities(
        cls,
        ingested_root_entities: List[HasMultipleExternalIdsEntityT],
    ) -> List[List[HasMultipleExternalIdsEntityT]]:
        """Buckets the list of ingested root entities into groups that all should be
        merged into the same root entity, based on their external ids. Each inner list
        in the returned list should be merged into one root entity.
        """

        result_buckets: List[List[HasMultipleExternalIdsEntityT]] = []

        # First bucket all the root entities that should be merged
        bucketed_root_entities_dict: Dict[
            str, List[HasMultipleExternalIdsEntityT]
        ] = defaultdict(list)
        external_id_key_to_primary: Dict[str, str] = {}
        for root_entity in ingested_root_entities:
            external_id_keys = root_entity_external_id_keys(root_entity)
            if len(external_id_keys) == 0:
                raise ValueError(
                    "Ingested root entity objects must have one or more assigned external ids."
                )

            # Find all the other root entities who should be related to this root entity
            # based on their external_ids and merge them into one bucket.
            merged_bucket = [root_entity]
            primary_buckets_to_merge = set()
            for external_id_key in external_id_keys:
                if external_id_key in external_id_key_to_primary:
                    primary_id_for_id = external_id_key_to_primary[external_id_key]
                    primary_buckets_to_merge.add(primary_id_for_id)

            for external_id_key in primary_buckets_to_merge:
                if external_id_key in bucketed_root_entities_dict:
                    merged_bucket.extend(
                        bucketed_root_entities_dict.pop(external_id_key)
                    )

            # Deterministically pick one of the ids to be the new primary id for this
            # merged bucket.
            all_primary_id_candidates = primary_buckets_to_merge.union(external_id_keys)
            primary_id = min(all_primary_id_candidates)

            for bucket_root_entity in merged_bucket:
                for external_id_key in root_entity_external_id_keys(bucket_root_entity):
                    external_id_key_to_primary[external_id_key] = primary_id

            bucketed_root_entities_dict[primary_id] = merged_bucket

        for bucket in bucketed_root_entities_dict.values():
            result_buckets.append(bucket)

        return result_buckets

    def _get_conflicting_fields(self, flat_field_reprs: set[str]) -> Set[str]:
        """Returns the set of field names that have conflicting values by comparing
        the JSON representations of flat fields.
        """
        if len(flat_field_reprs) <= 1:
            raise ValueError(
                f"Expected multiple flat_field_reprs, found [{len(flat_field_reprs)}]."
            )
        parsed_reprs = [json.loads(repr_str) for repr_str in flat_field_reprs]

        # Assume all flat field repr maps have the same keys
        all_fields = set(parsed_reprs[0].keys())

        # Find distinct values for each field
        field_to_values: dict[str, set[Any]] = defaultdict(set)
        for field_name in all_fields:
            for parsed in parsed_reprs:
                field_to_values[field_name].add(parsed[field_name])

        # Filter to fields with multiple values
        return {
            field_name
            for field_name, distinct_values in field_to_values.items()
            if len(distinct_values) > 1
        }

    def _merge_matched_tree_group(
        self, entity_group: List[EntityT], should_throw_on_conflicts: bool = True
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
            flat_field_reprs.add(
                get_flat_fields_json_str(entity, self.entities_module_context)
            )

        primary_entity = entity_group[0]

        self._check_flat_field_conflicts(
            entity_group, flat_field_reprs, should_throw_on_conflicts
        )

        children_by_field = self._get_children_grouped_by_field(entity_group)
        for field, child_list in children_by_field.items():
            self._merge_children_for_field(
                primary_entity,
                field,
                child_list,
                seen_objects,
                should_throw_on_conflicts,
            )

        return primary_entity, seen_objects

    def _check_flat_field_conflicts(
        self,
        entity_group: List[EntityT],
        flat_field_reprs: Set[str],
        should_throw_on_conflicts: bool,
    ) -> None:
        """Raises EntityMergingError (or logs) if entities in the group have
        conflicting flat field values.
        """
        if len(flat_field_reprs) <= 1:
            return

        primary_entity = entity_group[0]
        conflicting_fields = self._get_conflicting_fields(flat_field_reprs)

        entity_type = primary_entity.__class__.__name__
        fields_str = ", ".join(sorted(conflicting_fields))

        error_message_parts = [
            f"Found multiple different ingested entities of type [{entity_type}]",
            f"with conflicting information in fields: {fields_str}",
            "",
            "Entities with conflicts:",
        ]

        for i, entity in enumerate(entity_group, 1):
            error_message_parts.append(f"  Entity {i}: {entity.limited_pii_repr()}")

        error_message = "\n".join(error_message_parts)

        if should_throw_on_conflicts:
            raise EntityMergingError(
                error_message,
                entity_name=primary_entity.get_entity_name(),
            )
        logging.error(error_message)

    def _merge_children_for_field(
        self,
        primary_entity: EntityT,
        field: str,
        child_list: List[EntityT],
        seen_objects: Set[int],
        should_throw_on_conflicts: bool,
    ) -> None:
        """Merges all children for a single forward-edge field and attaches them
        to |primary_entity|. Mutates |seen_objects| to track visited entities.

        For singular (non-list) forward edges, raises EntityMergingError if
        merging produces multiple distinct children. This catches conflicts that
        the flat-field check cannot see because the conflicting values live in
        child entities, not in flat fields on the parent. For example, two
        root entity rows that share an external ID but provide different values
        for a singular child field::

            Row 1 → Parent(child=Child(value="A"))
            Row 2 → Parent(child=Child(value="B"))

        Because ``child`` is a singular forward edge, the two Child objects are
        bucketed separately and both survive merging — producing two children
        for a field that can only hold one.
        """
        groups = self._bucket_ingested_single_id_entities(child_list)
        merged_children: List[EntityT] = []
        for group in groups:
            merged_child, group_seen_objects = self._merge_matched_tree_group(
                group, should_throw_on_conflicts
            )

            if seen_objects.intersection(group_seen_objects):
                raise ValueError(
                    f"Already have seen one of the objects in [{merged_child}] "
                    f"- input is not a tree."
                )
            seen_objects.update(group_seen_objects)

            if merged_child:
                merged_children.append(merged_child)

        if (
            not isinstance(primary_entity.get_field(field), list)
            and len(merged_children) > 1
        ):
            entity_type = primary_entity.__class__.__name__
            child_type = merged_children[0].__class__.__name__
            error_message = (
                f"Found multiple different ingested [{child_type}] entities "
                f"for singular field [{field}] on [{entity_type}]"
            )
            if should_throw_on_conflicts:
                raise EntityMergingError(
                    error_message, entity_name=primary_entity.get_entity_name()
                )
            logging.error(error_message)
            merged_children = merged_children[:1]

        primary_entity.set_field_from_list(field, merged_children)

    def _bucket_ingested_single_id_entities(
        self, entity_list: List[EntityT]
    ) -> List[List[EntityT]]:
        """Buckets the list of ingested entities into groups that should be merged
        into the same entity, based on their external ids or the contents of their flat
        fields. This function assumes each entity in the list has an identical parent
        chain that will be merged.
        """
        root_entity_buckets_dict: Dict[str, List[EntityT]] = defaultdict(list)
        for entity in entity_list:
            key = self._get_root_entity_ingested_entity_key(entity)
            root_entity_buckets_dict[key].append(entity)
        return [list(b) for b in root_entity_buckets_dict.values()]

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

    def _get_root_entity_ingested_entity_key(self, entity: Entity) -> str:
        """Returns a string key that can be used to bucket this non-placeholder entity."""
        external_id = entity.get_external_id()
        if not external_id or isinstance(entity, ExternalIdEntity):
            return get_flat_fields_json_str(entity, self.entities_module_context)

        return external_id
