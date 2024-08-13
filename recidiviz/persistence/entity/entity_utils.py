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
"""Utils for working with Entity classes or various |entities| modules."""
import importlib
import inspect
import json
import re
from collections import defaultdict
from enum import Enum, auto
from functools import cache
from io import TextIOWrapper
from types import ModuleType
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Type, Union, cast

from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attr_field_name_storing_referenced_cls_name,
    attr_field_referenced_cls_name_for_field_name,
    attr_field_type_for_field_name,
    attribute_field_type_reference_for_class,
)
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_utils import (
    get_state_database_association_with_names,
)
from recidiviz.persistence.entity.base_entity import (
    Entity,
    EntityT,
    EnumEntity,
    ExternalIdEntity,
    HasExternalIdEntity,
    HasMultipleExternalIdsEntity,
    RootEntity,
)
from recidiviz.persistence.entity.entity_deserialize import EntityFactory
from recidiviz.persistence.entity.schema_edge_direction_checker import (
    direction_checker_for_module,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.persistence.entity.state.state_entity_mixins import LedgerEntityMixin
from recidiviz.utils import environment
from recidiviz.utils.log_helpers import make_log_output_path
from recidiviz.utils.types import assert_type, non_optional


@cache
def get_all_entity_classes_in_module(entities_module: ModuleType) -> Set[Type[Entity]]:
    """Returns a set of all subclasses of Entity that are
    defined in the given module."""
    expected_classes: Set[Type[Entity]] = set()
    for attribute_name in dir(entities_module):
        attribute = getattr(entities_module, attribute_name)
        if inspect.isclass(attribute):
            if attribute not in (
                Entity,
                HasExternalIdEntity,
                ExternalIdEntity,
                HasMultipleExternalIdsEntity,
                EnumEntity,
                LedgerEntityMixin,
            ) and issubclass(attribute, Entity):
                expected_classes.add(attribute)

    return expected_classes


def get_module_for_entity_class(entity_cls: Type[Entity]) -> ModuleType:
    if entity_cls in get_all_entity_classes_in_module(state_entities):
        return state_entities
    if entity_cls in get_all_entity_classes_in_module(normalized_entities):
        return normalized_entities
    raise ValueError(f"Unexpected entity_cls [{entity_cls}]")


def get_all_enum_classes_in_module(enums_module: ModuleType) -> Set[Type[Enum]]:
    """Returns a set of all subclasses of Enum that are defined in the given module."""
    enum_classes: Set[Type[Enum]] = set()
    for attribute_name in dir(enums_module):
        attribute = getattr(enums_module, attribute_name)
        if inspect.isclass(attribute):
            if (
                attribute is not Enum
                and attribute is not StateEntityEnum
                and issubclass(attribute, Enum)
            ):
                enum_classes.add(attribute)

    return enum_classes


def get_all_entity_factory_classes_in_module(
    factories_module: ModuleType,
) -> Set[Type[EntityFactory]]:
    """Returns a set of all subclasses of EntityFactory that are defined in the
    given module."""
    expected_classes: Set[Type[EntityFactory]] = set()
    for attribute_name in dir(factories_module):
        attribute = getattr(factories_module, attribute_name)
        if inspect.isclass(attribute):
            if attribute is not EntityFactory and issubclass(attribute, EntityFactory):
                expected_classes.add(attribute)

    return expected_classes


@cache
def get_entity_class_in_module_with_name(
    entities_module: ModuleType, class_name: str
) -> Type[Entity]:
    entity_classes = get_all_entity_classes_in_module(entities_module)

    for entity_class in entity_classes:
        if entity_class.__name__ == class_name:
            return entity_class

    raise LookupError(f"Entity class {class_name} does not exist in {entities_module}.")


@cache
def get_entity_class_in_module_with_table_id(
    entities_module: ModuleType, table_id: str
) -> Type[Entity]:
    entity_classes = get_all_entity_classes_in_module(entities_module)

    for entity_class in entity_classes:
        if entity_class.get_table_id() == table_id:
            return entity_class

    raise LookupError(
        f"Entity class with table_id {table_id} does not exist in {entities_module}."
    )


def get_all_entity_class_names_in_module(entities_module: ModuleType) -> Set[str]:
    """Returns a set of all names of subclasses of Entity that
    are defined in the given module."""
    return {cls_.__name__ for cls_ in get_all_entity_classes_in_module(entities_module)}


class EntityFieldType(Enum):
    FLAT_FIELD = auto()
    FORWARD_EDGE = auto()
    BACK_EDGE = auto()
    ALL = auto()


class _EntityFieldIndexKey:
    """Private key for the EntityFieldIndex to prevent instantiation outside of
    this file.
    """


_entity_field_index_by_entities_module: dict[ModuleType, "EntityFieldIndex"] = {}


@environment.test_only
def clear_entity_field_index_cache() -> None:
    _entity_field_index_by_entities_module.clear()


class EntityFieldIndex:
    """Class that caches the results of certain Entity class introspection
    functionality.
    """

    def __init__(
        self, entities_module: ModuleType, private_key: _EntityFieldIndexKey
    ) -> None:
        self.direction_checker = direction_checker_for_module(entities_module)
        # Cache of fields by field type for Entity classes
        self.entity_fields_by_field_type: Dict[
            str, Dict[EntityFieldType, Set[str]]
        ] = {}
        if not isinstance(private_key, _EntityFieldIndexKey):
            raise ValueError(f"Unexpected type for key: {type(private_key)}")

    def get_fields_with_non_empty_values(
        self, entity: Entity, entity_field_type: EntityFieldType
    ) -> Set[str]:
        """Returns a set of field_names that correspond to any non-empty (nonnull or
        non-empty list) fields on the provided |entity| that match the provided
        |entity_field_type|.
        """
        result = set()
        for field_name in self.get_all_entity_fields(type(entity), entity_field_type):
            v = entity.get_field(field_name)
            if isinstance(v, list):
                if v:
                    result.add(field_name)
            elif v is not None:
                result.add(field_name)
        return result

    def get_all_entity_fields(
        self, entity_cls: Type[Entity], entity_field_type: EntityFieldType
    ) -> Set[str]:
        """Returns a set of field_names that correspond to any fields (non-empty or
        otherwise) on the provided Entity class |entity_cls| that match the provided
        |entity_field_type|. Fields are included whether or not the values are non-empty
        on the provided object.

        This function caches the results for subsequent calls.
        """
        entity_name = entity_cls.get_entity_name()

        if not issubclass(entity_cls, Entity):
            raise ValueError(f"Unexpected entity type: {entity_cls}")

        if entity_name not in self.entity_fields_by_field_type:
            self.entity_fields_by_field_type[
                entity_name
            ] = self._get_entity_fields_by_field_type_slow(entity_cls)

        return self.entity_fields_by_field_type[entity_name][entity_field_type]

    def _get_entity_fields_by_field_type_slow(
        self, entity_cls: Type[Entity]
    ) -> Dict[EntityFieldType, Set[str]]:
        """Returns a set of field_names that correspond to any fields (non-empty or
        otherwise) on the provided Entity type |entity| that match the provided
        |entity_field_type|.

        This function is relatively slow and the results should be cached across
        repeated calls.
        """
        back_edges = set()
        forward_edges = set()
        flat_fields = set()
        for field, field_info in attribute_field_type_reference_for_class(
            entity_cls
        ).items():
            if field_info.referenced_cls_name:
                if self.direction_checker.is_back_edge(entity_cls, field):
                    back_edges.add(field)
                else:
                    forward_edges.add(field)
            else:
                flat_fields.add(field)

        return {
            EntityFieldType.FLAT_FIELD: flat_fields,
            EntityFieldType.FORWARD_EDGE: forward_edges,
            EntityFieldType.BACK_EDGE: back_edges,
            EntityFieldType.ALL: flat_fields | forward_edges | back_edges,
        }

    @classmethod
    def for_entities_module(cls, entities_module: ModuleType) -> "EntityFieldIndex":
        """Returns an EntityFieldIndex that can be used to determine information about
        structure of any Entity classes in the given |entities_module|.
        """
        if entities_module not in _entity_field_index_by_entities_module:
            _entity_field_index_by_entities_module[entities_module] = EntityFieldIndex(
                entities_module, _EntityFieldIndexKey()
            )
        return _entity_field_index_by_entities_module[entities_module]

    @classmethod
    def for_entity_class(cls, entity_cls: Type[Entity]) -> "EntityFieldIndex":
        """Returns an EntityFieldIndex that can be used to determine information about
        structure of any Entity classes in the module associated with the given
        |entity_cls|.
        """
        return cls.for_entities_module(get_module_for_entity_class(entity_cls))

    @classmethod
    def for_entity(cls, entity: Entity) -> "EntityFieldIndex":
        """Returns an EntityFieldIndex that can be used to determine information about
        structure of any Entity classes in the module associated with the given
        |entity|.
        """
        return cls.for_entity_class(type(entity))


def is_reference_only_entity(entity: Entity) -> bool:
    """Returns true if this object does not contain any meaningful information
    describing the entity, but instead only identifies the entity for reference
    purposes. Concretely, this means the object has an external_id but no other set
    fields (aside from default values).
    """
    set_flat_fields = get_explicitly_set_flat_fields(entity)
    if isinstance(entity, (state_schema.StatePerson, state_entities.StatePerson)):
        if set_flat_fields or any([entity.races, entity.aliases, entity.ethnicities]):
            return False
        return bool(entity.external_ids)

    if isinstance(entity, (state_schema.StateStaff, state_entities.StateStaff)):
        if set_flat_fields:
            return False
        return bool(entity.external_ids)

    return set_flat_fields == {"external_id"}


def get_explicitly_set_flat_fields(entity: Entity) -> Set[str]:
    """Returns the set of field names for fields on the entity that have been set with
    non-default values. The "state_code" field is also excluded, as it is set with the
    same value on every entity for a given ingest run.
    """
    field_index = EntityFieldIndex.for_entity(entity)
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


def _sort_based_on_flat_fields(db_entities: Sequence[Entity]) -> None:
    """Helper function that sorts all entities in |db_entities| in place as
    well as all children of |db_entities|. Sorting is done by first by an
    external_id if that field exists, then present flat fields.
    """

    def _get_entity_sort_key(e: Entity) -> str:
        """Generates a sort key for the given entity based on the flat field values
        in this entity."""
        return f"{e.get_external_id()}#{get_flat_fields_json_str(e)}"

    db_entities = cast(List, db_entities)
    db_entities.sort(key=_get_entity_sort_key)
    for entity in db_entities:
        field_index = EntityFieldIndex.for_entity(entity)
        for field_name in field_index.get_fields_with_non_empty_values(
            entity, EntityFieldType.FORWARD_EDGE
        ):
            field = entity.get_field_as_list(field_name)
            _sort_based_on_flat_fields(field)


def get_flat_fields_json_str(entity: Entity) -> str:
    field_index = EntityFieldIndex.for_entity(entity)
    flat_fields_dict: Dict[str, str] = {}
    for field_name in field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FLAT_FIELD
    ):
        flat_fields_dict[field_name] = str(entity.get_field(field_name))
    return json.dumps(flat_fields_dict, sort_keys=True)


def _print_indented(s: str, indent: int, file: Optional[TextIOWrapper] = None) -> None:
    print(f'{" " * indent}{s}', file=file)


def _obj_id_str(entity: Entity, id_mapping: Dict[int, int]) -> str:
    python_obj_id = id(entity)

    if python_obj_id not in id_mapping:
        fake_id = len(id_mapping)
        id_mapping[python_obj_id] = fake_id
    else:
        fake_id = id_mapping[python_obj_id]

    return f"{entity.get_entity_name()} ({fake_id})"


def write_entity_tree_to_file(
    region_code: str,
    operation_for_filename: str,
    print_tree_structure_only: bool,
    root_entities: Sequence[Any],
) -> str:
    filepath = make_log_output_path(
        operation_for_filename,
        region_code=region_code,
    )
    with open(filepath, "w", encoding="utf-8") as actual_output_file:
        print_entity_trees(
            root_entities,
            print_tree_structure_only=print_tree_structure_only,
            file=actual_output_file,
        )

    return filepath


def print_entity_trees(
    entities_list: Sequence[Entity],
    print_tree_structure_only: bool = False,
    python_id_to_fake_id: Optional[Dict[int, int]] = None,
    file: Optional[TextIOWrapper] = None,
) -> None:
    """Recursively prints out all objects in the trees below the given list of
    entities. Each time we encounter a new object, we assign a new fake id (an
    auto-incrementing count) and print that with the object.

    This means that two lists with the exact same shape/flat fields will print
    out the exact same string, making it much easier to debug edge-related
    issues in Diffchecker, etc.

    If |file| is provided, the trees will be output to that file rather than printed
    to the terminal.

    Note: this function sorts any list fields in the provided entity IN PLACE
    (should not matter for any equality checks we generally do).
    """

    if python_id_to_fake_id is None:
        python_id_to_fake_id = {}
        _sort_based_on_flat_fields(entities_list)

    for entity in entities_list:
        print_entity_tree(
            entity,
            print_tree_structure_only=print_tree_structure_only,
            python_id_to_fake_id=python_id_to_fake_id,
            file=file,
        )


def print_entity_tree(
    entity: Entity,
    print_tree_structure_only: bool = False,
    indent: int = 0,
    python_id_to_fake_id: Optional[Dict[int, int]] = None,
    file: Optional[TextIOWrapper] = None,
) -> None:
    """Recursively prints out all objects in the tree below the given entity. Each time we encounter a new object, we
    assign a new fake id (an auto-incrementing count) and print that with the object.

    This means that two entity trees with the exact same shape/flat fields will print out the exact same string, making
    it much easier to debug edge-related issues in Diffchecker, etc.

    Note: this function sorts any list fields in the provided entity IN PLACE (should not matter for any equality checks
    we generally do).
    """
    field_index = EntityFieldIndex.for_entity(entity)
    if python_id_to_fake_id is None:
        python_id_to_fake_id = {}
        _sort_based_on_flat_fields([entity])

    _print_indented(_obj_id_str(entity, python_id_to_fake_id), indent, file)

    indent = indent + 2
    for field in field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FLAT_FIELD
    ):
        if field == "external_id" or not print_tree_structure_only:
            val = entity.get_field(field)
            _print_indented(f"{field}: {str(val)}", indent, file)

    for child_field in field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FORWARD_EDGE
    ):
        child = entity.get_field(child_field)

        if child is not None:
            if isinstance(child, list):
                if not child:
                    _print_indented(f"{child_field}: []", indent, file)
                else:
                    _print_indented(f"{child_field}: [", indent, file)
                    for c in child:
                        print_entity_tree(
                            c,
                            print_tree_structure_only,
                            indent + 2,
                            python_id_to_fake_id,
                            file=file,
                        )
                    _print_indented("]", indent, file)

            else:
                _print_indented(f"{child_field}:", indent, file)
                print_entity_tree(
                    child,
                    print_tree_structure_only,
                    indent + 2,
                    python_id_to_fake_id,
                    file=file,
                )
        else:
            _print_indented(f"{child_field}: None", indent, file)

    for child_field in field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.BACK_EDGE
    ):
        child = entity.get_field(child_field)
        if not child:
            raise ValueError(f"Expected non-empty child value for field {child_field}")
        if isinstance(child, list):
            first_child = next(iter(child))
            unique = {id(c) for c in child}
            len_str = (
                f"{len(child)}"
                if len(unique) == len(child)
                else f"{len(child)} - ONLY {len(unique)} UNIQUE!"
            )

            id_str = _obj_id_str(first_child, python_id_to_fake_id)
            ellipsis_str = ", ..." if len(child) > 1 else ""

            _print_indented(
                f"{child_field} ({len_str}): [{id_str}{ellipsis_str}] - backedge",
                indent,
                file,
            )
        else:
            id_str = _obj_id_str(child, python_id_to_fake_id)
            _print_indented(f"{child_field}: {id_str} - backedge", indent, file)


def get_all_entities_from_tree(
    entity: Entity,
    result: Optional[List[Entity]] = None,
    seen_ids: Optional[Set[int]] = None,
) -> List[Entity]:
    """Returns a list of all entities in the tree below the entity,
    including the entity itself. Entities are deduplicated by Python object id.
    """
    field_index = EntityFieldIndex.for_entity(entity)
    if result is None:
        result = []
    if seen_ids is None:
        seen_ids = set()

    if id(entity) in seen_ids:
        return result

    result.append(entity)
    seen_ids.add(id(entity))

    fields = field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FORWARD_EDGE
    )

    for field in fields:
        child = entity.get_field(field)

        if child is None:
            raise ValueError("Expected only nonnull values at this point")

        if isinstance(child, list):
            for c in child:
                get_all_entities_from_tree(c, result, seen_ids)
        else:
            get_all_entities_from_tree(child, result, seen_ids)

    return result


def get_all_entity_associations_from_tree(
    entity: Entity,
    result: Optional[Dict[str, Set[Tuple[str, str]]]] = None,
    seen_ids: Optional[Set[int]] = None,
) -> Dict[str, Set[Tuple[str, str]]]:
    """Returns a dictionary of association table name to a set of associations in the
    |entity| tree that belong in that table. Each association is defined as a
    (child_external_id, parent_external_id) tuple where each id is the external_id
    associated with the entity. We use external_ids instead of database primary keys so
    this function can be used on Python entity trees that do not yet have primary keys assigned.
    """

    field_index = EntityFieldIndex.for_entity(entity)
    if result is None:
        result = defaultdict(set)
    if seen_ids is None:
        seen_ids = set()

    if id(entity) in seen_ids:
        return result

    seen_ids.add(id(entity))

    many_to_many_relationships = get_many_to_many_relationships(entity.__class__)
    for relationship in many_to_many_relationships:
        parents = entity.get_field_as_list(relationship)
        for parent in parents:
            association_table = get_state_database_association_with_names(
                entity.__class__.__name__, parent.__class__.__name__
            )
            result[association_table.name].add(
                (
                    non_optional(entity.get_external_id()),
                    non_optional(parent.get_external_id()),
                )
            )

    fields = field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FORWARD_EDGE
    )

    for field in fields:
        child = entity.get_field(field)

        if child is None:
            raise ValueError("Expected only nonnull values at this point")

        if isinstance(child, list):
            for c in child:
                get_all_entity_associations_from_tree(c, result, seen_ids)
        else:
            get_all_entity_associations_from_tree(child, result, seen_ids)
    return result


def update_reverse_references_on_related_entities(
    updated_entity: Union[Entity, NormalizedStateEntity],
    new_related_entities: Union[List[Entity], List[NormalizedStateEntity]],
    reverse_relationship_field: str,
    reverse_relationship_field_type: BuildableAttrFieldType,
) -> None:
    """For each of the entities in the |new_related_entities| list, updates the value
    stored in the |reverse_relationship_field| to point to the |updated_entity|.

    If the attribute stored in the |reverse_relationship_field| is a list, replaces
    the reference to the original entity in the list with the |updated_entity|.
    """
    if reverse_relationship_field_type == BuildableAttrFieldType.FORWARD_REF:
        # If the reverse relationship field is a forward ref, set the updated entity
        # directly as the value.
        for new_related_entity in new_related_entities:
            setattr(new_related_entity, reverse_relationship_field, updated_entity)
        return

    if reverse_relationship_field_type != BuildableAttrFieldType.LIST:
        raise ValueError(
            f"Unexpected reverse_relationship_field_type: [{reverse_relationship_field_type}]"
        )

    for new_related_entity in new_related_entities:
        reverse_relationship_list = getattr(
            new_related_entity, reverse_relationship_field
        )

        if updated_entity not in reverse_relationship_list:
            # Add the updated entity to the list since it is not already present
            reverse_relationship_list.append(updated_entity)


@cache
def module_for_module_name(module_name: str) -> ModuleType:
    """Returns the module with the module name."""
    return importlib.import_module(module_name)


def deep_entity_update(
    original_entity: EntityT, **updated_attribute_kwargs: Any
) -> EntityT:
    """Updates the |original_entity| with all of the updated attributes provided in
    the |updated_attribute_kwargs| mapping. For any attribute in the
    updated_attribute_kwargs that is a reference to another entity (or entities),
    updates the reverse references on those entities to point to the new, updated
    version of the |original_entity|.

    Returns the new version of the entity.
    """
    entity_type = type(original_entity)
    updated_entity = original_entity

    reverse_fields_to_update: List[Tuple[str, str, Type[Entity]]] = []

    for field, updated_value in updated_attribute_kwargs.items():
        # Update the value stored in the field on the entity
        updated_entity.set_field(field, updated_value)

        related_class_name = attr_field_referenced_cls_name_for_field_name(
            entity_type, field
        )

        if not related_class_name:
            # This field doesn't store a related entity class, no need to update reverse
            # references
            continue

        if not updated_value:
            # There is nothing being set in this field, so no need to update the
            # reverse references.
            continue

        related_class = get_entity_class_in_module_with_name(
            entities_module=module_for_module_name(
                original_entity.__class__.__module__
            ),
            class_name=related_class_name,
        )

        reverse_relationship_field = attr_field_name_storing_referenced_cls_name(
            base_cls=related_class, referenced_cls_name=entity_type.__name__
        )

        if not reverse_relationship_field:
            # Not a bi-directional relationship
            continue

        reverse_fields_to_update.append(
            (field, reverse_relationship_field, related_class)
        )

    for field, reverse_relationship_field, related_class in reverse_fields_to_update:
        updated_value = updated_attribute_kwargs[field]

        reverse_relationship_field_type = attr_field_type_for_field_name(
            related_class, reverse_relationship_field
        )

        new_related_entities: List[Entity]
        if isinstance(updated_value, list):
            new_related_entities = updated_value
        else:
            new_related_entities = [updated_value]

        # This relationship is bidirectional, so we will update the reference
        # on all related entities to point to the new updated_entity
        update_reverse_references_on_related_entities(
            new_related_entities=new_related_entities,
            reverse_relationship_field=reverse_relationship_field,
            reverse_relationship_field_type=reverse_relationship_field_type,
            updated_entity=updated_entity,
        )

    return updated_entity


def set_backedges(element: RootEntity) -> RootEntity:
    """Set the backedges of the root entity tree using DFS traversal of the root
    entity tree."""
    entities_module = get_module_for_entity_class(type(assert_type(element, Entity)))
    field_index = EntityFieldIndex.for_entities_module(entities_module)
    root = cast(Entity, element)
    root_entity_cls = root.__class__
    stack: List[Entity] = [root]
    while stack:
        current_parent = stack.pop()
        current_parent_cls = current_parent.__class__
        forward_fields = sorted(
            field_index.get_all_entity_fields(
                current_parent_cls, EntityFieldType.FORWARD_EDGE
            )
        )
        for field in forward_fields:
            related_entities: List[Entity] = current_parent.get_field_as_list(field)
            if not related_entities:
                continue

            related_entity_cls_name = attr_field_referenced_cls_name_for_field_name(
                current_parent_cls, field
            )
            if not related_entity_cls_name:
                # In the context of a FORWARD_EDGE type field, this should always
                # be nonnull.
                raise ValueError(
                    f"Could not extract the referenced class name from field "
                    f"[{field}] on class [{current_parent_cls}]."
                )

            related_entity_cls = get_entity_class_in_module_with_name(
                entities_module, related_entity_cls_name
            )
            reverse_relationship_field = attr_field_name_storing_referenced_cls_name(
                base_cls=related_entity_cls,
                referenced_cls_name=current_parent_cls.__name__,
            )
            if not reverse_relationship_field:
                # In the context of a FORWARD_EDGE type field, this should always
                # be nonnull.
                raise ValueError(
                    f"Found no field on [{related_entity_cls}] referencing objects "
                    f"of type [{current_parent_cls}]"
                )

            reverse_relationship_field_type = attr_field_type_for_field_name(
                related_entity_cls, reverse_relationship_field
            )
            update_reverse_references_on_related_entities(
                updated_entity=current_parent,
                new_related_entities=related_entities,
                reverse_relationship_field=reverse_relationship_field,
                reverse_relationship_field_type=reverse_relationship_field_type,
            )

            root_reverse_relationship_field = (
                attr_field_name_storing_referenced_cls_name(
                    base_cls=related_entity_cls,
                    referenced_cls_name=root_entity_cls.__name__,
                )
            )

            if not root_reverse_relationship_field:
                raise ValueError(
                    f"Found no field on [{related_entity_cls}] referencing root "
                    f"entities of type [{root_entity_cls}]"
                )

            root_reverse_relationship_field_type = attr_field_type_for_field_name(
                related_entity_cls, root_reverse_relationship_field
            )
            update_reverse_references_on_related_entities(
                updated_entity=root,
                new_related_entities=related_entities,
                reverse_relationship_field=root_reverse_relationship_field,
                reverse_relationship_field_type=root_reverse_relationship_field_type,
            )
            stack.extend(related_entities)
    return element


def get_many_to_many_relationships(entity_cls: Type[Entity]) -> Set[str]:
    """Returns the set of fields on |entity| that connect that entity to a parent where
    there is a potential many-to-many relationship between entity and that parent entity type.
    """
    field_index = EntityFieldIndex.for_entity_class(entity_cls)
    many_to_many_relationships = set()
    back_edges = field_index.get_all_entity_fields(
        entity_cls, EntityFieldType.BACK_EDGE
    )
    for back_edge in back_edges:
        relationship_field_type = attr_field_type_for_field_name(entity_cls, back_edge)

        parent_cls = get_entity_class_in_module_with_name(
            entities_module=get_module_for_entity_class(entity_cls),
            class_name=attr_field_referenced_cls_name_for_field_name(
                entity_cls, back_edge
            ),
        )

        inverse_relationship_field_name = attr_field_name_storing_referenced_cls_name(
            base_cls=parent_cls,
            referenced_cls_name=entity_cls.__name__,
        )
        inverse_relationship_field_type = (
            attr_field_type_for_field_name(parent_cls, inverse_relationship_field_name)
            if inverse_relationship_field_name
            else None
        )
        if (
            relationship_field_type == BuildableAttrFieldType.LIST
            and inverse_relationship_field_type == BuildableAttrFieldType.LIST
        ):
            many_to_many_relationships.add(back_edge)
    return many_to_many_relationships


def is_many_to_many_relationship(
    parent_cls: Type[Entity], child_cls: Type[Entity]
) -> bool:
    """Returns True if there's a many-to-many relationship between these the provided
    parent and child entities. Entity classes must be directly related otherwise this
    will throw.
    """
    if not entities_have_direct_relationship(parent_cls, child_cls):
        raise ValueError(
            f"Entities [{parent_cls.__name__}] and [{child_cls.__name__}] are not "
            f"directly related - can't call is_many_to_many_relationship()."
        )

    reference_field = attr_field_name_storing_referenced_cls_name(
        parent_cls, child_cls.__name__
    )

    if not reference_field:
        raise ValueError(
            f"Expected to find relationship between [{parent_cls.__name__}] and "
            f"[{child_cls.__name__}] but found none."
        )

    reverse_reference_field = attr_field_name_storing_referenced_cls_name(
        child_cls, parent_cls.__name__
    )

    if not reverse_reference_field:
        raise ValueError(
            f"Expected to find relationship between [{child_cls.__name__}] and "
            f"[{parent_cls.__name__}] but found none."
        )

    return (
        attr_field_type_for_field_name(parent_cls, reference_field)
        == BuildableAttrFieldType.LIST
    ) and (
        attr_field_type_for_field_name(child_cls, reverse_reference_field)
        == BuildableAttrFieldType.LIST
    )


def is_one_to_many_relationship(
    parent_cls: Type[Entity], child_cls: Type[Entity]
) -> bool:
    """Returns True if there's a one-to-many relationship between these the provided
    parent and child entities.
    """
    if not entities_have_direct_relationship(parent_cls, child_cls):
        raise ValueError(
            f"Entities [{parent_cls.__name__}] and [{child_cls.__name__}] are not "
            f"directly related - can't call is_one_to_many_relationship()."
        )

    reference_field = attr_field_name_storing_referenced_cls_name(
        parent_cls, child_cls.__name__
    )

    if not reference_field:
        raise ValueError(
            f"Expected to find relationship between [{parent_cls.__name__}] and "
            f"[{child_cls.__name__}] but found none."
        )

    reverse_reference_field = attr_field_name_storing_referenced_cls_name(
        child_cls, parent_cls.__name__
    )

    if not reverse_reference_field:
        raise ValueError(
            f"Expected to find relationship between [{child_cls.__name__}] and "
            f"[{parent_cls.__name__}] but found none."
        )

    return (
        attr_field_type_for_field_name(parent_cls, reference_field)
        == BuildableAttrFieldType.LIST
    ) and (
        attr_field_type_for_field_name(child_cls, reverse_reference_field)
        != BuildableAttrFieldType.LIST
    )


def is_many_to_one_relationship(
    parent_cls: Type[Entity], child_cls: Type[Entity]
) -> bool:
    """Returns True if there's a many-to-one relationship between these the provided
    parent and child entities.
    """
    if not entities_have_direct_relationship(parent_cls, child_cls):
        raise ValueError(
            f"Entities [{parent_cls.__name__}] and [{child_cls.__name__}] are not "
            f"directly related - can't call is_many_to_one_relationship()."
        )
    return is_one_to_many_relationship(parent_cls=child_cls, child_cls=parent_cls)


def entities_have_direct_relationship(
    entity_cls_a: Type[Entity], entity_cls_b: Type[Entity]
) -> bool:
    """Returns True if the two provided entity types are directly related in the schema
    entity tree. For example, StatePerson and StateAssessment are directly related, but
    StatePerson and StateSupervisionViolationResponse are not.
    """
    reference_field = attr_field_name_storing_referenced_cls_name(
        entity_cls_a, entity_cls_b.__name__
    )
    reverse_reference_field = attr_field_name_storing_referenced_cls_name(
        entity_cls_b, entity_cls_a.__name__
    )
    return reference_field is not None and reverse_reference_field is not None


def get_association_table_id(parent_cls: Type[Entity], child_cls: Type[Entity]) -> str:
    """For two classes that have a many to many relationship between them,
    returns the name of the association table that can be used to hydrate
    relationships between the classes.
    """
    if not is_many_to_many_relationship(parent_cls, child_cls):
        raise ValueError(
            f"Classes [{parent_cls.__name__}] and [{child_cls.__name__}] do not have a "
            f"many-to-many relationship - cannot get an association table."
        )

    # TODO(#10389): Remove this custom handling for legacy sentence association tables
    #  once we remove these classes from the schema.
    if {parent_cls, child_cls} == {
        state_entities.StateSupervisionSentence,
        state_entities.StateCharge,
    } or {parent_cls, child_cls} == {
        normalized_entities.NormalizedStateSupervisionSentence,
        normalized_entities.NormalizedStateCharge,
    }:
        return "state_charge_supervision_sentence_association"

    if {parent_cls, child_cls} == {
        state_entities.StateIncarcerationSentence,
        state_entities.StateCharge,
    } or {parent_cls, child_cls} == {
        normalized_entities.NormalizedStateIncarcerationSentence,
        normalized_entities.NormalizedStateCharge,
    }:
        return "state_charge_incarceration_sentence_association"

    parts = [
        *sorted([parent_cls.get_table_id(), child_cls.get_table_id()]),
        "association",
    ]
    return "_".join(parts)


def get_entities_by_association_table_id(
    entities_module: ModuleType, association_table_id: str
) -> Tuple[Type[Entity], Type[Entity]]:
    """For the given association table id, returns the classes for the two entities that
    this table associates.
    """
    # TODO(#10389): Remove this custom handling for legacy sentence association tables
    #  once we remove these classes from the schema.
    if association_table_id == "state_charge_supervision_sentence_association":
        if entities_module == state_entities:
            return state_entities.StateCharge, state_entities.StateSupervisionSentence
        if entities_module == normalized_entities:
            return (
                normalized_entities.NormalizedStateCharge,
                normalized_entities.NormalizedStateSupervisionSentence,
            )
        raise ValueError(f"Unexpected module {entities_module}")
    if association_table_id == "state_charge_incarceration_sentence_association":
        if entities_module == state_entities:
            return state_entities.StateCharge, state_entities.StateIncarcerationSentence
        if entities_module == normalized_entities:
            return (
                normalized_entities.NormalizedStateCharge,
                normalized_entities.NormalizedStateIncarcerationSentence,
            )
        raise ValueError(f"Unexpected module {entities_module}")

    if not (
        match := re.match(
            r"(?P<table_id_1>state_[a-z0-9_]+)_(?P<table_id_2>state_[a-z0-9_]+)_association",
            association_table_id,
        )
    ):
        raise ValueError(
            f"Association table [{association_table_id}] does not match expected format."
        )

    return get_entity_class_in_module_with_table_id(
        entities_module, match.group("table_id_1")
    ), get_entity_class_in_module_with_table_id(
        entities_module, match.group("table_id_2")
    )
