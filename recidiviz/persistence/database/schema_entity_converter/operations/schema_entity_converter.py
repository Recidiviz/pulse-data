# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""
Defines a class for converting between operations-specific Entity and schema Base
objects.
"""

from types import ModuleType
from typing import Type

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.base_schema_entity_converter import (
    BaseSchemaEntityConverter,
    FieldNameType,
    DstBaseType,
    SrcBaseType,
)
from recidiviz.persistence.entity.base_entity import Entity

from recidiviz.persistence.entity.entity_utils import SchemaEdgeDirectionChecker
from recidiviz.persistence.entity.operations import entities


class _OperationsSchemaEntityConverter(
    BaseSchemaEntityConverter[SrcBaseType, DstBaseType]
):
    """County-specific implementation of BaseSchemaEntityConverter"""

    def __init__(self):
        super().__init__(SchemaEdgeDirectionChecker.county_direction_checker())

    def _get_schema_module(self) -> ModuleType:
        return schema

    def _get_entities_module(self) -> ModuleType:
        return entities

    def _should_skip_field(self, entity_cls: Type, field: FieldNameType) -> bool:
        return False

    def _populate_indirect_back_edges(self, _):
        return


class OperationsSchemaToEntityConverter(
    _OperationsSchemaEntityConverter[DatabaseEntity, Entity]
):
    pass


class OperationsEntityToSchemaConverter(
    _OperationsSchemaEntityConverter[Entity, DatabaseEntity]
):
    pass
