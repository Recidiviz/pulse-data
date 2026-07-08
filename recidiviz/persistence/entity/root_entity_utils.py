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
"""Utils for managing root entities."""
from functools import cache
from types import ModuleType
from typing import Dict, Set, Type

from more_itertools import one

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.entities_module_context import EntitiesModuleContext
from recidiviz.persistence.entity.entity_field_index import EntityFieldType
from recidiviz.persistence.entity.entity_utils import (
    entities_have_direct_relationship,
    get_all_entity_classes_in_module,
    get_entity_class_in_module_with_name,
    is_many_to_many_relationship,
    is_many_to_one_relationship,
    is_one_to_many_relationship,
    module_for_module_name,
)
from recidiviz.utils.types import assert_subclass


# TODO(OBT-37718): Widen get_root_entity_id and downstream typing to int | str
# as it is wrong for entities with string primary keys (e.g. IdentityFragment,
# IdentityCluster). (mypy can't see this because getattr returns Any).
def get_root_entity_id(entity: Entity) -> int:
    """Returns the id of the root entity the entity is associated with."""

    entity_cls = type(entity)
    if issubclass(entity_cls, RootEntity):
        return entity.get_id()

    root_entity_cls = get_root_entity_class_for_entity(entity_cls)
    return entity.get_field(root_entity_cls.back_edge_field_name()).get_id()


def get_root_entity_classes_in_module(
    entities_module: ModuleType,
) -> Set[Type[RootEntity]]:
    return {
        e
        for e in get_all_entity_classes_in_module(entities_module)
        if issubclass(e, RootEntity)
    }


def get_root_entity_class_for_entity(entity_cls: Type[Entity]) -> Type[RootEntity]:
    """Returns the RootEntity class the provided |entity_cls| is associated with.

    Attempts to do this directly first, via a back-edge field named
    `root.back_edge_field_name()` or the root's primary-key id column. Some
    entity trees, though, connect a child to its root only through an intermediate
    (non-root) entity. For example, identity fragment demographic entities
    back-edge to `IdentityAttributes`, which in turn back-edges to the
    `IdentityFragment` root. In this case, it falls back to walking up through
    intermediate parents until it reaches the root.
    """
    if direct_roots := _root_entity_classes_via_back_edge_or_id(entity_cls):
        return one(direct_roots)

    if intermediate_roots := _root_entity_classes_via_intermediate_parents(entity_cls):
        return one(intermediate_roots)

    raise ValueError(
        f"Expected non root entity [{entity_cls}] to have a field associated with "
        f"a root entity type but found none."
    )


def _root_entity_classes_via_back_edge_or_id(
    entity_cls: Type[Entity],
) -> list[Type[RootEntity]]:
    """Returns the root entity classes |entity_cls| references directly, via a
    back-edge field or the root's primary-key id column."""
    found_root_entities = []
    for root_entity_cls in get_root_entity_classes_in_module(
        module_for_module_name(entity_cls.__module__)
    ):
        fields = [
            root_entity_cls.back_edge_field_name(),
            assert_subclass(root_entity_cls, Entity).get_class_id_name(),
        ]

        for field in fields:
            if entity_cls.has_field(field):
                found_root_entities.append(root_entity_cls)

    return found_root_entities


def _root_entity_classes_via_intermediate_parents(
    entity_cls: Type[Entity],
) -> set[Type[RootEntity]]:
    """Returns the distinct root entity classes reachable from |entity_cls| by
    walking back edges up through intermediate (non-root) parent entities.
    """
    found_root_entities = set()
    for parent_cls in get_all_entity_classes_in_module(
        module_for_module_name(entity_cls.__module__)
    ):
        # We identify parents via back_edge_field_name() here rather than the
        # module context's field index (EntityFieldType.BACK_EDGE) to avoid
        # a circular import.
        if parent_cls is entity_cls or not hasattr(parent_cls, "back_edge_field_name"):
            continue
        if entity_cls.has_field(parent_cls.back_edge_field_name()):
            found_root_entities.add(get_root_entity_class_for_entity(parent_cls))
    return found_root_entities


def _entity_cls_has_own_primary_key(entity_cls: Type[Entity]) -> bool:
    """Returns True if |entity_cls| declares its own primary-key id field (a
    field named `entity_cls.get_class_id_name()`). Intermediate entities that
    exist only to group children, like `IdentityAttributes`, have no such
    field; their rows join back to the root entity instead."""
    return entity_cls.has_field(entity_cls.get_class_id_name())


def entity_class_for_foreign_key_column(
    *,
    entities_module_context: EntitiesModuleContext,
    entity_cls: Type[Entity],
    field_name: str,
) -> Type[Entity] | None:
    """Returns the entity class whose id is stored on |entity_cls|'s BQ table
    for the relationship field |field_name|, or None if that field adds no
    column to the table.

    A relationship between two entities appears as a field on each of the two
    classes: `StatePerson.assessments` is the list of a person's assessments,
    and `StateAssessment.person` points back at the person. In BigQuery, that
    same relationship is stored exactly once, as a single id column on one of
    the two tables: the `state_assessment` table has a `person_id` column, and
    the `state_person` table has nothing.

    This function decides, field by field, where that column goes. Given a
    class and the name of one of its relationship fields, it answers: does
    this field put an id column on this class's table? Returning a class means
    yes, and the column holds that class's ids. Called with `StateAssessment`
    and "person", it returns `StatePerson`: the `state_assessment` table gets
    a `person_id` column. Returning None means no. Called with `StatePerson`
    and "assessments", it returns None, because that relationship's column
    lives on the assessment table, not the person table.

    Both BQ schema generation (`entities_bq_schema`) and row serialization
    (`serialization`) answer "which fields become id columns, and whose id do
    they hold?" by calling this function, so the schemas and the rows written
    into them cannot disagree.

    The rules, with examples:

    * The field holds this entity's parent, and many siblings can share that
      parent (many-to-one): the column holds the parent's id. E.g.
      `StateSupervisionViolationResponse.supervision_violation` returns
      `StateSupervisionViolation`: a `supervision_violation_id` column.
    * The field holds this entity's parent and the relationship is 1:1: same
      thing. E.g. `IdentityAttributes.fragment` returns `IdentityFragment`:
      an `identity_fragment_id` column.
    * The field references the tree's root entity without a direct
      relationship (the root has no field pointing back at this entity): the
      column holds the root's id. E.g. `StateEarlyDischarge.person` returns
      `StatePerson` even though `StatePerson` has no `early_discharges` field.
      (A field referencing an entity this class has no direct relationship
      with must point at this class's own root; anything else raises.)
    * Anything else: no column, returns None. This covers parent-side fields
      (`StatePerson.assessments`, `IdentityAttributes.name`), whose
      relationship is recorded on the child's table instead, and many-to-many
      fields (`StateChargeV2.sentences`), which are recorded in standalone
      association tables.

    One wrinkle: the parent the field points at may have no id of its own.
    `IdentityAttributes` exists only to group a fragment's demographic
    children and declares no `identity_attributes_id`, so there is no id to
    store. In that case the column holds the id of the tree's root instead:
    `IdentityRace.identity_attributes` returns `IdentityFragment`, producing
    an `identity_fragment_id` column on the `identity_race` table. (An entity
    "has its own id" when it declares a field named `get_class_id_name()`;
    giving `IdentityAttributes` such a field would silently re-point its
    children's columns from the root to it. The schema golden tests in
    `entities_bq_schema_test` would catch that.)
    """
    field_info = attribute_field_type_reference_for_class(entity_cls).get_field_info(
        field_name
    )
    if not field_info.referenced_cls_name:
        # Flat field, not a relationship.
        return None

    referenced_cls = get_entity_class_in_module_with_name(
        entities_module_context.entities_module(), field_info.referenced_cls_name
    )
    root_cls = assert_subclass(get_root_entity_class_for_entity(entity_cls), Entity)

    if not entities_have_direct_relationship(entity_cls, referenced_cls):
        # The only indirect reference an entity may hold is to its own root,
        # whose id every non-root entity's table carries.
        if not issubclass(referenced_cls, RootEntity):
            raise ValueError(
                f"Field [{field_name}] on [{entity_cls.__name__}] references "
                f"[{referenced_cls.__name__}], which it has no direct "
                f"relationship with and which is not a root entity."
            )
        if referenced_cls is not root_cls:
            raise ValueError(
                f"Field [{field_name}] on [{entity_cls.__name__}] references "
                f"root entity [{referenced_cls.__name__}], but this class's "
                f"root is [{root_cls.__name__}]."
            )
        return root_cls

    if is_many_to_many_relationship(
        entity_cls, referenced_cls
    ) or is_one_to_many_relationship(entity_cls, referenced_cls):
        # The FK lives on the other side's table (or an association table).
        return None

    if is_many_to_one_relationship(entity_cls, referenced_cls):
        return (
            referenced_cls
            if _entity_cls_has_own_primary_key(referenced_cls)
            else root_cls
        )

    # Direct 1:1 relationship: the FK lives on the table of the entity whose
    # field is the back edge (the child pointing up at its parent), not the
    # forward edge.
    back_edge_fields = entities_module_context.field_index().get_all_entity_fields(
        entity_cls, EntityFieldType.BACK_EDGE
    )
    if field_name not in back_edge_fields:
        return None
    return (
        referenced_cls if _entity_cls_has_own_primary_key(referenced_cls) else root_cls
    )


@cache
def get_entity_class_to_root_entity_class(
    entities_module: ModuleType,
) -> Dict[type[Entity], type[Entity]]:
    """Returns a dictionary that maps from entity class to the corresponding root entity
    class.
    """
    result = {}
    for entity_cls in get_all_entity_classes_in_module(entities_module):
        root_entity_class = get_root_entity_class_for_entity(entity_cls)
        if not issubclass(root_entity_class, Entity):
            raise ValueError(
                f"Expected root_entity_class to be an Entity subclass, "
                f"found: {root_entity_class}"
            )
        result[entity_cls] = assert_subclass(root_entity_class, Entity)
    return result
