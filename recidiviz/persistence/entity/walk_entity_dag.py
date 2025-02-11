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
"""A helper function for iterating over all entities in an entity tree / graph."""
from typing import Callable, List, Set, Type

import attr

from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attr_field_name_storing_referenced_cls_name,
    attr_field_type_for_field_name,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context import EntitiesModuleContext
from recidiviz.persistence.entity.entity_field_index import EntityFieldType
from recidiviz.utils.types import T, assert_type


@attr.define(kw_only=True)
class EntityDagEdge:
    """Represents a relationship between two entities in a graph of entity
    relationships.
    """

    parent: Entity
    child: Entity

    # The field on parent that contains a reference to the child entity
    parent_reference_to_child_field: str

    @property
    def child_reference_to_parent_field(self) -> str:
        """The field on |child| that contains a reference to |parent|."""
        return assert_type(
            attr_field_name_storing_referenced_cls_name(
                base_cls=self.child_cls,
                referenced_cls_name=self.parent_cls.__name__,
            ),
            str,
        )

    @property
    def child_reference_to_parent_field_type(self) -> BuildableAttrFieldType:
        """The type of the field on |child| that contains a reference to |parent|.
        (e.g. BuildableAttrFieldType.LIST or BuildableAttrFieldType.FORWARD_REF).
        """
        return attr_field_type_for_field_name(
            self.child_cls, self.child_reference_to_parent_field
        )

    @property
    def child_cls(self) -> Type[Entity]:
        return type(self.child)

    @property
    def parent_cls(self) -> Type[Entity]:
        return type(self.parent)


# A function that will be called with an entity and a list of edges that were traversed
# (in order) to reach that entity from the provided root.
EntityNodeProcessingFn = Callable[[Entity, List[EntityDagEdge]], T]


def walk_entity_dag(
    entities_module_context: EntitiesModuleContext,
    dag_root_entity: Entity,
    node_processing_fn: EntityNodeProcessingFn[T],
    explore_all_paths: bool = False,
) -> List[T]:
    """A helper function for doing a DFS traversal of all entities that can be
    reached from the provided |dag_root_entity|.

    Returns a list with the results of running |node_processing_fn| on each visited
    node, in traversal order.

    If |explore_all_paths| is true, the |node_processing_fn| will be called once per
    distinct path to a given node. Otherwise, will only be called the first time we
    visit that node.
    """
    return _walk_entity_dag_inner(
        entities_module_context=entities_module_context,
        entity=dag_root_entity,
        node_processing_fn=node_processing_fn,
        ancestor_chain=[],
        visited_entity_ids=set(),
        explore_all_paths=explore_all_paths,
    )


def _walk_entity_dag_inner(
    *,
    entities_module_context: EntitiesModuleContext,
    entity: Entity,
    node_processing_fn: EntityNodeProcessingFn,
    ancestor_chain: List[EntityDagEdge],
    visited_entity_ids: Set[int],
    explore_all_paths: bool,
) -> List[T]:
    """Private recursive helper for walk_entity_dag()."""
    results = [node_processing_fn(entity, ancestor_chain)]
    visited_entity_ids.add(id(entity))

    field_index = entities_module_context.field_index()

    for field in field_index.get_all_entity_fields(
        type(entity), EntityFieldType.FORWARD_EDGE
    ):
        for child in entity.get_field_as_list(field):
            parent_info = EntityDagEdge(
                parent=entity,
                child=child,
                parent_reference_to_child_field=field,
            )
            if not explore_all_paths and id(child) in visited_entity_ids:
                continue
            results.extend(
                _walk_entity_dag_inner(
                    entities_module_context=entities_module_context,
                    entity=child,
                    node_processing_fn=node_processing_fn,
                    ancestor_chain=ancestor_chain + [parent_info],
                    visited_entity_ids=visited_entity_ids,
                    explore_all_paths=explore_all_paths,
                )
            )
    return results
