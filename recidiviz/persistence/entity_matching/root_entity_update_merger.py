# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Class responsible for merging a root entity tree with new / updated child entities
into an existing version of that root entity.
"""
import json
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Type

from more_itertools import one

from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attr_field_type_for_field_name,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.persistence.entity.base_entity import (
    Entity,
    EntityT,
    EnumEntity,
    ExternalIdEntity,
    ExternalIdEntityT,
    HasExternalIdEntity,
    HasExternalIdEntityT,
    RootEntity,
)
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity,
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_field_index import EntityFieldType
from recidiviz.persistence.entity.entity_utils import get_all_entities_from_tree
from recidiviz.persistence.entity.serialization import serialize_entity_into_json
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.entities import (
    StatePersonAddressPeriod,
    StatePersonAlias,
    StatePersonHousingStatusPeriod,
    StatePersonStaffRelationshipPeriod,
)
from recidiviz.persistence.entity.state.state_entity_mixins import (
    LedgerEntityMixin,
    LedgerEntityMixinT,
)
from recidiviz.persistence.entity.walk_entity_dag import EntityDagEdge, walk_entity_dag
from recidiviz.persistence.entity_matching.entity_merger_utils import (
    enum_entity_key,
    external_id_key,
)
from recidiviz.persistence.persistence_utils import RootEntityT
from recidiviz.pipelines.ingest.state.constants import ExternalId
from recidiviz.utils.types import T, assert_type

# Schema classes that can have multiple parents of different types.
_MULTI_PARENT_ENTITY_TYPES = [entities.StateCharge, entities.StateChargeV2]


def state_person_address_period_key(address_period: StatePersonAddressPeriod) -> str:
    return json.dumps(
        serialize_entity_into_json(address_period, entities_module=entities),
        sort_keys=True,
    )


def state_person_housing_status_period_key(
    housing_status_period: StatePersonHousingStatusPeriod,
) -> str:
    return json.dumps(
        serialize_entity_into_json(housing_status_period, entities_module=entities),
        sort_keys=True,
    )


def state_person_staff_relationship_period_key(
    person_staff_relationship_period: StatePersonStaffRelationshipPeriod,
) -> str:
    return json.dumps(
        serialize_entity_into_json(
            person_staff_relationship_period, entities_module=entities
        ),
        sort_keys=True,
    )


def state_person_alias_key(alias: StatePersonAlias) -> str:
    return f"{type(alias)}#{alias.full_name}|{alias.alias_type}"


def ledger_entity_key(ledger: LedgerEntityMixinT) -> str:
    """Returns the ledger's partition key (pylint wouldn't let us store a lambda function)"""
    return ledger.partition_key


def _to_set_assert_no_dupes(item_list: Iterable[T]) -> Set[T]:
    """Throws on the first found duplicate value if the provided iterable yields any
    duplicate values, otherwise, returns a set with the values.
    """
    result = set()
    for item in item_list:
        if item in result:
            raise ValueError(
                f"Found duplicate item [{item}] in list within the same entity tree"
                f"- merging before this step should have eliminated this duplicate "
                f"already."
            )
        result.add(item)
    return result


def is_reference_only_state_entity(entity: Entity) -> bool:
    """Returns true if this object does not contain any meaningful information
    describing the entity, but instead only identifies the entity for reference
    purposes. Concretely, this means the object has an external_id but no other set
    fields (aside from default values).
    """
    set_flat_fields = _get_explicitly_set_flat_fields(entity)
    if isinstance(entity, state_entities.StatePerson):
        if set_flat_fields or any([entity.races, entity.aliases, entity.ethnicities]):
            return False
        return bool(entity.external_ids)

    if isinstance(entity, state_entities.StateStaff):
        if set_flat_fields:
            return False
        return bool(entity.external_ids)

    return set_flat_fields == {"external_id"}


def _get_explicitly_set_flat_fields(entity: Entity) -> Set[str]:
    """Returns the set of field names for fields on the entity that have been set with
    non-default values. The "state_code" field is also excluded, as it is set with the
    same value on every entity for a given ingest run.
    """
    entities_module_context = entities_module_context_for_entity(entity)
    field_index = entities_module_context.field_index()
    set_flat_fields = field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FLAT_FIELD
    )

    primary_key_name = entity.get_primary_key_column_name()
    if primary_key_name in set_flat_fields:
        set_flat_fields.remove(primary_key_name)

    # TODO(#2244): Change this to a general approach so we don't need to check
    # explicit columns
    if "state_code" in set_flat_fields:
        set_flat_fields.remove("state_code")

    default_enum_value_fields = {
        field_name
        for field_name in set_flat_fields
        if entity.is_default_enum(field_name)
    }

    set_flat_fields -= default_enum_value_fields

    if "incarceration_type" in set_flat_fields:
        if entity.is_default_enum(
            "incarceration_type", StateIncarcerationType.STATE_PRISON.value
        ):
            set_flat_fields.remove("incarceration_type")

    return set_flat_fields


class RootEntityUpdateMerger:
    """Class responsible for merging a root entity tree with new / updated child
    entities into an existing version of that root entity.
    """

    def __init__(self) -> None:
        self.entities_module_context = entities_module_context_for_module(
            state_entities
        )
        self.field_index = self.entities_module_context.field_index()

    def merge_root_entity_trees(
        self, old_root_entity: Optional[RootEntityT], root_entity_updates: RootEntityT
    ) -> RootEntityT:
        self._check_root_entity_meets_prerequisites(root_entity_updates)
        all_updated_entity_ids = {
            id(e)
            for e in get_all_entities_from_tree(
                root_entity_updates, self.entities_module_context
            )
        }
        if old_root_entity:
            self._check_root_entity_meets_prerequisites(old_root_entity)
            result = self._merge_matched_entities(old_root_entity, root_entity_updates)
        else:
            result = root_entity_updates

        result = self._merge_multi_parent_entities(result, all_updated_entity_ids)
        return result

    def _check_root_entity_meets_prerequisites(self, root_entity: RootEntityT) -> None:
        """Checks that root entity trees do not have any fields set that should not
        yet be set by this point in processing.
        """
        for e in get_all_entities_from_tree(root_entity, self.entities_module_context):
            if back_edge_fields := self.field_index.get_fields_with_non_empty_values(
                e, EntityFieldType.BACK_EDGE
            ):
                raise ValueError(
                    f"Found set back edges on [{type(e).__name__}] entity: "
                    f"{back_edge_fields}. Back edge fields should not be set at this "
                    f"point."
                )
            if primary_key := e.get_field(e.get_primary_key_column_name()):
                raise ValueError(
                    f"Found set primary key on [{type(e).__name__}] entity: "
                    f"{primary_key}. Primary key fields should not be set at this "
                    f"point."
                )

    def _merge_matched_entities(
        self, old_entity: EntityT, new_or_updated_entity: EntityT
    ) -> EntityT:
        """Given two versions of an entity with the same external_id, recursively merges
        two entity trees, applying updates to already existing entities where
        applicable.
        """
        for child_field in self.field_index.get_all_entity_fields(
            type(old_entity), EntityFieldType.FORWARD_EDGE
        ):
            old_children = old_entity.get_field(child_field)
            new_or_updated_children = new_or_updated_entity.get_field(child_field)
            if not old_children and not new_or_updated_children:
                continue

            child_cls = one(
                {e.__class__ for e in old_children + new_or_updated_children}
            )

            try:
                if not new_or_updated_children:
                    all_children = old_children
                elif issubclass(child_cls, HasExternalIdEntity):
                    all_children = self._merge_has_external_id_entity_children(
                        old_children, new_or_updated_children
                    )
                elif issubclass(child_cls, ExternalIdEntity):
                    all_children = self._merge_external_id_entity_children(
                        old_children, new_or_updated_children
                    )
                else:
                    key_fn: Callable[[Any], str]
                    if issubclass(child_cls, EnumEntity):

                        def _enum_entity_key(entity: EnumEntity) -> str:
                            return enum_entity_key(entity)

                        key_fn = _enum_entity_key
                    elif issubclass(child_cls, StatePersonAlias):
                        key_fn = state_person_alias_key
                    elif issubclass(child_cls, StatePersonAddressPeriod):
                        key_fn = state_person_address_period_key
                    elif issubclass(child_cls, StatePersonHousingStatusPeriod):
                        key_fn = state_person_housing_status_period_key
                    elif issubclass(child_cls, StatePersonStaffRelationshipPeriod):
                        key_fn = state_person_staff_relationship_period_key
                    elif issubclass(child_cls, LedgerEntityMixin):
                        key_fn = ledger_entity_key
                    else:
                        raise ValueError(f"Unexpected leaf node class [{child_cls}]")

                    all_children = self._merge_leaf_node_children_based_on_key(
                        old_children, new_or_updated_children, key_fn
                    )
            except Exception as e:
                raise ValueError(
                    f"Error merging children of type [{child_cls.__name__}] from "
                    f"entity [{new_or_updated_entity.limited_pii_repr()}] onto entity "
                    f"[{new_or_updated_entity.limited_pii_repr()}]"
                ) from e

            old_entity.set_field(child_field, all_children)

        if is_reference_only_state_entity(new_or_updated_entity):
            # At this point, the external_ids between old_entity and
            # new_or_updated_entity are the same, but new_or_updated_entity only has
            # the external_id field set, so we keep old_entity.
            return old_entity

        for child_field in self._flat_fields_to_merge(new_or_updated_entity):
            old_entity.set_field(
                child_field,
                new_or_updated_entity.get_field(child_field),
            )

        return old_entity

    @staticmethod
    def _merge_leaf_node_children_based_on_key(
        old_children: List[EntityT],
        new_or_updated_children: List[EntityT],
        key_fn: Callable[[EntityT], str],
    ) -> List[EntityT]:
        """Given two lists of leaf node entities of the same type, returns a merged
        single list that applies updates. Any entity in `new_or_updated_children` with
        the same |key_fn| result as an entity in `old_children` will replace it in the
        merged list.
        """
        new_keys = _to_set_assert_no_dupes(key_fn(e) for e in new_or_updated_children)
        _to_set_assert_no_dupes(key_fn(e) for e in old_children)
        return [
            *new_or_updated_children,
            *[e for e in old_children if key_fn(e) not in new_keys],
        ]

    def _merge_external_id_entity_children(
        self,
        old_children: List[ExternalIdEntityT],
        new_or_updated_children: List[ExternalIdEntityT],
    ) -> List[ExternalIdEntityT]:
        """Given two lists of ExternalIdEntity entities of the same type, returns a
        merged single list that applies updates. Any external ids with the same |key_fn|
        result will be merged together. When merging, if optional fields are nonnull on
        either entity, we always take the nonnull value. If multiple have conflicting
        nonnull values, we throw.
        """
        old_ids_by_key = defaultdict(list)
        for eid in old_children:
            old_ids_by_key[external_id_key(eid)].append(eid)

        new_ids_by_key = defaultdict(list)
        for eid in new_or_updated_children:
            new_ids_by_key[external_id_key(eid)].append(eid)

        # First collect children with no match between new and old
        merged_children = [
            *[
                one(ids_with_key)
                for key, ids_with_key in new_ids_by_key.items()
                if key not in old_ids_by_key
            ],
            *[
                one(ids_with_key)
                for key, ids_with_key in old_ids_by_key.items()
                if key not in new_ids_by_key
            ],
        ]

        # Next, merge
        keys_with_match = set(old_ids_by_key).intersection(new_ids_by_key)
        for key in keys_with_match:
            merged_eid = self._merge_matched_external_ids(
                one(old_ids_by_key[key]), one(new_ids_by_key[key])
            )
            merged_children.append(merged_eid)
        return merged_children

    def _merge_matched_external_ids(
        self,
        old_external_id: ExternalIdEntityT,
        new_or_updated_external_id: ExternalIdEntityT,
    ) -> ExternalIdEntityT:
        """Merges the two matched (same key function value) external ids into a single
        entity.

        When merging, if optional fields are nonnull on either entity, we always take
        the nonnull value. If multiple have conflicting nonnull values, we throw.
        """

        old_key = external_id_key(old_external_id)
        new_key = external_id_key(new_or_updated_external_id)
        if new_key != old_key:
            raise ValueError(
                f"This function should never be called for external ids with different "
                f"keys. Found {old_key} and {new_key}."
            )

        key_fields = {"external_id", "id_type"}

        for child_field in self._flat_fields_to_merge(new_or_updated_external_id):
            old_field_value = old_external_id.get_field(child_field)
            new_field_value = new_or_updated_external_id.get_field(child_field)

            if old_field_value == new_field_value:
                # No need to update, fields already equal
                continue

            if child_field in key_fields:
                raise ValueError(
                    f"Found two external id entities with the same key ([{old_key}] "
                    f"and [{new_key}]) with different {child_field} values. This "
                    f"should never happen."
                )

            if old_field_value is not None and new_field_value is not None:
                raise ValueError(
                    f"Merging field [{child_field}] with two conflicting nonnull "
                    f"values: [{old_field_value}] != [{new_field_value}]. For ids with "
                    f"key: {new_key}."
                )

            nonnull_value = one(
                field_value
                for field_value in [old_field_value, new_field_value]
                if field_value is not None
            )
            old_external_id.set_field(child_field, nonnull_value)
        return old_external_id

    def _merge_has_external_id_entity_children(
        self,
        old_children: List[HasExternalIdEntityT],
        new_or_updated_children: List[HasExternalIdEntityT],
    ) -> List[HasExternalIdEntityT]:
        """Given two lists of HasExternalIdEntity of the same type, returns a merged
        single list that applies updates.
        """
        for e in old_children + new_or_updated_children:
            if not e.external_id:
                raise ValueError(
                    f"Found null external_id for [{e.__class__.__name__}] entity [{e}]."
                )

        _to_set_assert_no_dupes(e.external_id for e in new_or_updated_children)
        _to_set_assert_no_dupes(e.external_id for e in old_children)

        old_by_external_id = {assert_type(e.external_id, str): e for e in old_children}
        new_by_external_id = {
            assert_type(e.external_id, str): e for e in new_or_updated_children
        }

        matching_ids = set(old_by_external_id).intersection(set(new_by_external_id))

        merged_children = [
            *[e for e in old_children if e.external_id not in matching_ids],
            *[e for e in new_or_updated_children if e.external_id not in matching_ids],
        ]
        for external_id in matching_ids:
            merged_children.append(
                self._merge_matched_entities(
                    old_by_external_id[external_id], new_by_external_id[external_id]
                )
            )
        return merged_children

    def _flat_fields_to_merge(self, new_or_updated_entity: Entity) -> Set[str]:
        """Returns the names of the fields on the new/updated entity which contain
        data that should be merged onto the old version of this entity, if there is
        a match.
        """
        all_fields = self.field_index.get_all_entity_fields(
            type(new_or_updated_entity), EntityFieldType.FLAT_FIELD
        )
        if not issubclass(new_or_updated_entity.__class__, RootEntity):
            return all_fields

        # For root entities, we expect to potentially see updates from multiple
        # different sources, with some fields being hydrated by one source and some
        # fields being hydrated by another source. We don't want to completely overwrite
        # all the flat fields based on one data source if that source does not hydrate
        # all the fields. For example, source 1 may hydrate staff email, but source 2
        # might hydrate full name. We want to pick the most hydrated version of each
        # field.
        fields_to_update = {
            f for f in all_fields if new_or_updated_entity.get_field(f) is not None
        }

        default_enum_value_fields = {
            field_name
            for field_name in fields_to_update
            if new_or_updated_entity.is_default_enum(field_name)
        }

        fields_to_update -= default_enum_value_fields

        # If an enum field is updated, always update the corresponding raw text field
        # (and vice versa), even if one of the values is null.
        new_fields = set()
        for field_name in fields_to_update:
            if (
                attr_field_type_for_field_name(
                    new_or_updated_entity.__class__, field_name
                )
                == BuildableAttrFieldType.ENUM
            ):
                new_fields.add(f"{field_name}{EnumEntity.RAW_TEXT_FIELD_SUFFIX}")
            if field_name.endswith(EnumEntity.RAW_TEXT_FIELD_SUFFIX):
                new_fields.add(field_name[: -len(EnumEntity.RAW_TEXT_FIELD_SUFFIX)])
        fields_to_update.update(new_fields)
        return fields_to_update

    def _merge_multi_parent_entities(
        self, root_entity: RootEntityT, all_updated_entity_ids: Set[int]
    ) -> RootEntityT:
        r"""Scans the whole graph of entities connected to |root_entity| and merges any
        entities together if they share the same external_id but do not have the same
        parent entities.

        For example, this:
                               StatePerson
                             /           \
          StateIncarcerationSentence    StateSupervisionSentence
                external_id="ABC"            external_id="DEF"
                      |                           |
                  StateCharge                StateCharge
               external_id="123"           external_id="123"

        ...would become:

                              StatePerson
                             /           \
          StateIncarcerationSentence    StateSupervisionSentence
                external_id="ABC"            external_id="DEF"
                              \            /
                                StateCharge
                              external_id="123"

        Args:
            root_entity: The root Entity to perform the internal merging within.
            all_updated_entity_ids: The set of Python object ids of entities that were
                present in the |root_entity_updates| entity originally passed to
                merge_root_entity_trees. This is used to inform the merging order, so
                newer updates are preserved.
        """

        entities_module_context = entities_module_context_for_module(entities)
        direction_checker = entities_module_context.direction_checker()
        # Assert the list of multi-parent entity types is listed in order from closest
        # to the root entity to farthest away, so we merge from root downwards.
        direction_checker.assert_sorted(_MULTI_PARENT_ENTITY_TYPES)
        for multi_parent_entity_cls in _MULTI_PARENT_ENTITY_TYPES:
            root_entity = self._merge_multi_parent_entities_of_type(
                root_entity, multi_parent_entity_cls, all_updated_entity_ids
            )

        return root_entity

    def _merge_multi_parent_entities_of_type(
        self,
        root_entity: RootEntityT,
        multi_parent_entity_cls: Type[Entity],
        all_updated_entity_ids: Set[int],
    ) -> RootEntityT:
        """Scans the whole graph of entities connected to |root_entity| and merges any
        entities of type |multi_parent_entity_cls| together if they share the same
        external_id. This function should be used to merge entities where they may not
        have the same parents but still share the same external id.
        """

        edges_by_entity_external_id = self.get_edges_by_child_external_id(
            root_entity, child_cls=multi_parent_entity_cls
        )
        for edges_to_parents in edges_by_entity_external_id.values():
            # Sort edges with children from the original |root_entity_updates| entity
            # last so that any updated field values are preserved.
            edges_to_parents = sorted(
                edges_to_parents,
                key=lambda edge: id(edge.child) in all_updated_entity_ids,
            )

            # Iterate over each edge from parent -> multi-parent entity and merge
            # children into a single child entity (e.g. a single StateCharge).
            merged_multi_parent_entity = edges_to_parents[0].child
            for edge_to_parent in edges_to_parents[1:]:
                merged_multi_parent_entity = self._merge_matched_entities(
                    merged_multi_parent_entity, edge_to_parent.child
                )

            # For every parent, replace the old child with the new, merged child.
            for edge_to_parent in edges_to_parents:
                list_on_parent: List[Entity] = getattr(
                    edge_to_parent.parent,
                    edge_to_parent.parent_reference_to_child_field,
                )
                list_on_parent.remove(edge_to_parent.child)
                list_on_parent.append(merged_multi_parent_entity)

        return root_entity

    def get_edges_by_child_external_id(
        self, root_entity: RootEntityT, child_cls: Type[Entity]
    ) -> Dict[str, List[EntityDagEdge]]:
        """Returns all edges in the |root_entity| relationship graph where the child is
        of type |child_cls|, grouped by the external_id of the child entity.
        """
        edges_by_child_external_id: Dict[ExternalId, List[EntityDagEdge]] = defaultdict(
            list
        )

        def find_multi_parent_entities(
            entity: Entity, path_from_root: List[EntityDagEdge]
        ) -> None:
            if not isinstance(entity, child_cls):
                return
            if not path_from_root:
                raise ValueError(
                    f"Found multi-parent entity of type [{type(entity)}] with empty "
                    f"path to the root entity, i.e. it is the root of the entity graph."
                    f"Multi-parent entities cannot also be root entities."
                )
            direct_parent_edge = path_from_root[-1]
            edges_by_child_external_id[
                assert_type(entity.get_external_id(), str)
            ].append(direct_parent_edge)

        entities_module_context = entities_module_context_for_module(entities)
        walk_entity_dag(
            entities_module_context,
            root_entity,
            find_multi_parent_entities,
            explore_all_paths=True,
        )
        return edges_by_child_external_id
