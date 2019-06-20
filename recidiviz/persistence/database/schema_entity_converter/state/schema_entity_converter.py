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
"""
Defines a class for converting between state-specific Entity and schema Base
objects.
"""

from types import ModuleType

from recidiviz.persistence.database.base_schema import Base
from recidiviz.persistence.database.schema_entity_converter. \
    base_schema_entity_converter import (
        BaseSchemaEntityConverter,
        FieldNameType,
        SrcBaseType,
        DstBaseType
    )
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import SchemaEdgeDirectionChecker

from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.database.schema.state import schema


class _StateSchemaEntityConverter(BaseSchemaEntityConverter[SrcBaseType,
                                                            DstBaseType]):
    """State-specific implementation of BaseSchemaEntityConverter"""

    def __init__(self):
        super().__init__(SchemaEdgeDirectionChecker.state_direction_checker())

    def _get_schema_module(self) -> ModuleType:
        return schema

    def _get_entities_module(self) -> ModuleType:
        return entities

    def _should_skip_field(self, field: FieldNameType) -> bool:
        return False


class StateEntityToSchemaConverter(_StateSchemaEntityConverter[Entity, Base]):
    pass


class StateSchemaToEntityConverter(_StateSchemaEntityConverter[Base, Entity]):
    pass
