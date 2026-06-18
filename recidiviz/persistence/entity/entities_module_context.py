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
from enum import Enum
from functools import cache
from types import ModuleType

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_field_index import EntityFieldIndex
from recidiviz.persistence.entity.schema_edge_direction_checker import (
    SchemaEdgeDirectionChecker,
)
from recidiviz.utils.types import non_optional


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
    @abc.abstractmethod
    def partition_column_name(cls) -> str | None:
        """Returns the column name that says which state or tenant a row belongs
        to (e.g. `state_code`, `tenant`, `region_code`), or None if this module's
        entities don't carry that info.

        Must be non-None for modules whose entities are written to BigQuery via
        `recidiviz.pipelines.ingest.transforms.serialize_entities.SerializeEntities`
        AND have many-to-many relationships.
        """

    @classmethod
    def get_partition_value(cls, entity: Entity) -> str:
        """Returns the partition value (e.g. state code, tenant, region code) of the
        given entity by reading the flat field named by `partition_column_name`.
        Raises if this module declares no partition column.
        """
        value = getattr(entity, non_optional(cls.partition_column_name()))
        # The partition column flows into BigQuery row dicts as a string. Enum-valued
        # partition fields (e.g. `tenant: Tenant` on identity entities) need to be
        # coerced to their underlying string value here; non-enum fields (e.g.
        # `state_code: str` on state entities) are returned as-is.
        return value.value if isinstance(value, Enum) else value

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

    @classmethod
    def field_description(  # pylint: disable=unused-argument
        cls, entity_cls: type[Entity], field_name: str
    ) -> str | None:
        """Returns a description for the given class and field, used in BQ schemas
        and entity documentation. Returns None if no description is available for
        this module's entities.
        """
        return None
