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
"""Defines a class providing context about an entities module (a module defining a
collection of Entity objects that form a relational schema).
"""
import abc
from functools import cache
from types import ModuleType

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_field_index import EntityFieldIndex
from recidiviz.persistence.entity.schema_edge_direction_checker import (
    SchemaEdgeDirectionChecker,
)


class EntitiesModuleContext:
    """Class providing context about an entities module (a module defining a collection
    of Entity objects that form a relational schema).
    """

    @classmethod
    @abc.abstractmethod
    def entities_module(cls) -> ModuleType:
        """Returns the module we're providing context about."""

    @classmethod
    @abc.abstractmethod
    def class_hierarchy(cls) -> list[str]:
        """Returns a list of all Entity class names defined in the entities_module,
        ranked in order from closest to the root of the entity tree to furthest. This
        ranking is used to determine edge direction in an entity tree.
        """

    @classmethod
    @cache
    def direction_checker(cls) -> SchemaEdgeDirectionChecker:
        return SchemaEdgeDirectionChecker(cls.class_hierarchy())

    @classmethod
    def field_index(cls) -> EntityFieldIndex:
        return EntityFieldIndex.get(cls.entities_module(), cls.direction_checker())

    # TODO(#10389): Remove this custom association tables function once remove legacy
    #  sentences from the schema.
    @classmethod
    def custom_association_tables(cls) -> dict[str, tuple[type[Entity], type[Entity]]]:
        return {}
