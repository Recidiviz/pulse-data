# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Defines a class that caches the results of certain Entity class introspection
functionality.
"""
from enum import Enum, auto
from types import ModuleType
from typing import Dict, Set, Type

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.schema_edge_direction_checker import (
    SchemaEdgeDirectionChecker,
)
from recidiviz.utils import environment


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
        self,
        direction_checker: SchemaEdgeDirectionChecker,
        private_key: _EntityFieldIndexKey,
    ) -> None:
        self.direction_checker = direction_checker
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
        class_reference = attribute_field_type_reference_for_class(entity_cls)
        for field in class_reference.fields:
            field_info = class_reference.get_field_info(field)
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
    def get(
        cls, entities_module: ModuleType, direction_checker: SchemaEdgeDirectionChecker
    ) -> "EntityFieldIndex":
        """Returns an EntityFieldIndex that can be used to determine information about
        structure of any Entity classes in the given |entities_module|.
        """
        if entities_module not in _entity_field_index_by_entities_module:
            _entity_field_index_by_entities_module[entities_module] = EntityFieldIndex(
                direction_checker,
                _EntityFieldIndexKey(),
            )
        return _entity_field_index_by_entities_module[entities_module]
