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
"""A helper class for mapping between pure Python entities defined in a schema's
entities.py and their corresponding SQLAlchemy classes defined in schema.py.
"""

from types import ModuleType
from typing import Dict, Type

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema_utils import (
    get_all_database_entities_in_module,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context import EntitiesModuleContext
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.schema_edge_direction_checker import (
    SchemaEdgeDirectionChecker,
)
from recidiviz.utils import environment

_mappers: Dict[str, "SchemaToEntityClassMapper"] = {}


class SchemaToEntityClassMapper:
    """A helper class for mapping between pure Python entities defined in a schema's
    entities.py and their corresponding SQLAlchemy classes defined in schema.py.
    """

    def __init__(
        self,
        *,
        entities_module_context: EntitiesModuleContext,
        schema_module: ModuleType,
    ) -> None:
        self._entities_module_context = entities_module_context
        self._entities_module = entities_module_context.entities_module()
        self._schema_module = schema_module

        entity_classes_by_name = {
            c.__name__: c
            for c in get_all_entity_classes_in_module(self._entities_module)
        }
        schema_classes_by_name = {
            c.__name__: c for c in get_all_database_entities_in_module(schema_module)
        }

        # We always expect schema classes to be a strict superset of entity classes.
        # We don't check the other direction (schema classes not in entities), since it
        # since we sometimes leave legacy tables around in schema.py so that we can do a
        # migration in multiple parts.
        if entity_only := set(entity_classes_by_name) - set(schema_classes_by_name):
            raise ValueError(
                f"Found entity classes only defined in [{self._entities_module}] and "
                f"not in [{schema_module}]: {entity_only}"
            )

        self._entity_cls_to_schema_cls = {
            cls: schema_classes_by_name[cls_name]
            for cls_name, cls in entity_classes_by_name.items()
        }
        self._schema_cls_to_entity_cls = {
            cls: entity_classes_by_name[cls_name]
            for cls_name, cls in schema_classes_by_name.items()
            if cls_name in entity_classes_by_name
        }

    @property
    def entities_module(self) -> ModuleType:
        return self._entities_module

    @property
    def direction_checker(self) -> SchemaEdgeDirectionChecker:
        return self._entities_module_context.direction_checker()

    def entity_cls_for_schema_cls(
        self, schema_cls: Type[DatabaseEntity]
    ) -> Type[Entity]:
        """Returns the pure-Python entity class associated with the provided SQLAlchemy
        |schema_cls|.
        """
        if schema_cls not in self._schema_cls_to_entity_cls:
            raise ValueError(f"Invalid schema class: {schema_cls.__name__}")
        return self._schema_cls_to_entity_cls[schema_cls]

    def schema_cls_for_entity_cls(
        self, entity_cls: Type[Entity]
    ) -> Type[DatabaseEntity]:
        """Returns the SQLAlchemy class associated with the provided pure-Python
        |entity_cls|.
        """
        if entity_cls not in self._entity_cls_to_schema_cls:
            raise ValueError(f"Invalid entity class: {entity_cls.__name__}")
        return self._entity_cls_to_schema_cls[entity_cls]

    @classmethod
    def get(
        cls, *, entities_module: ModuleType, schema_module: ModuleType
    ) -> "SchemaToEntityClassMapper":
        """A factory method for efficiently getting a SchemaToEntityClassMapper. The
        resulting mapper is cached and returned on future invocations with the same
        modules.
        """
        entities_module_context = entities_module_context_for_module(entities_module)
        key = f"{entities_module_context.entities_module().__name__}#{schema_module.__name__}"

        if key not in _mappers:
            _mappers[key] = SchemaToEntityClassMapper(
                entities_module_context=entities_module_context,
                schema_module=schema_module,
            )
        return _mappers[key]

    @classmethod
    @environment.test_only
    def clear_cache(cls) -> None:
        """Test-only method for clearning the cached mappers."""
        _mappers.clear()
