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
"""Helpers for extracting metadata about an entity and its relationships."""
from functools import cached_property
from types import ModuleType
from typing import Tuple, Type

import attr

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import (
    get_entities_by_association_table_id,
    get_entity_class_in_module_with_table_id,
)
from recidiviz.persistence.entity.root_entity_utils import (
    get_root_entity_class_for_entity,
)


@attr.define
class EntityMetadataHelper:
    """
    Helper class to extract metadata about an entity and its relationships.
    """

    _entity_cls: Type[Entity]

    @classmethod
    def for_table_id(
        cls, entities_module: ModuleType, table_id: str
    ) -> "EntityMetadataHelper":
        """Returns an instance of EntityMetadataHelper for the given table id."""
        entity_cls = get_entity_class_in_module_with_table_id(
            entities_module=entities_module, table_id=table_id
        )
        return cls(entity_cls=entity_cls)

    @cached_property
    def _root_entity_cls(self) -> Type[Entity]:
        """Returns the root entity class."""
        root_entity_class = get_root_entity_class_for_entity(self._entity_cls)
        if not issubclass(root_entity_class, Entity):
            raise ValueError(
                f"Expected root_entity_class to be an Entity subclass, "
                f"found: {root_entity_class}"
            )
        return root_entity_class

    @property
    def root_entity_primary_key(self) -> str:
        """Returns the primary key for the root entity."""
        return self._root_entity_cls.get_primary_key_column_name()

    @property
    def primary_key(self) -> str:
        """Returns the primary key column name of the entity."""
        return self._entity_cls.get_primary_key_column_name()


@attr.define
class AssociationTableMetadataHelper:
    """
    Helper class to extract metadata about an association table and its relationships.
    """

    _entity_cls_a: Type[Entity]
    _entity_cls_b: Type[Entity]

    @classmethod
    def for_table_id(
        cls, entities_module: ModuleType, table_id: str
    ) -> "AssociationTableMetadataHelper":
        """Returns an instance of AssociationTableMetadataHelper for the given table id."""
        entities_module_context = entities_module_context_for_module(entities_module)
        entity_cls_a, entity_cls_b = get_entities_by_association_table_id(
            entities_module_context=entities_module_context,
            association_table_id=table_id,
        )
        return cls(entity_cls_a=entity_cls_a, entity_cls_b=entity_cls_b)

    @property
    def associated_entities_class_ids(self) -> Tuple[str, str]:
        """Returns the class ids of the associated entities."""
        return (
            self._entity_cls_a.get_class_id_name(),
            self._entity_cls_b.get_class_id_name(),
        )
