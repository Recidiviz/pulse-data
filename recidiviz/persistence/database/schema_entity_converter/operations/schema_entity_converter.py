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

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.base_schema_entity_converter import (
    BaseSchemaEntityConverter,
    DstBaseType,
    SrcBaseType,
)
from recidiviz.persistence.database.schema_entity_converter.schema_to_entity_class_mapper import (
    SchemaToEntityClassMapper,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.operations import entities


class _OperationsSchemaEntityConverter(
    BaseSchemaEntityConverter[SrcBaseType, DstBaseType]
):
    """Operations-specific implementation of BaseSchemaEntityConverter"""

    def __init__(self) -> None:
        class_mapper = SchemaToEntityClassMapper.get(
            schema_module=schema, entities_module=entities
        )
        super().__init__(class_mapper=class_mapper)


class OperationsSchemaToEntityConverter(
    _OperationsSchemaEntityConverter[DatabaseEntity, Entity]
):
    pass


class OperationsEntityToSchemaConverter(
    _OperationsSchemaEntityConverter[Entity, DatabaseEntity]
):
    pass
