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
from typing import Type, TypeVar

import attr

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema_entity_converter.base_schema_entity_converter import (
    BaseSchemaEntityConverter,
    FieldNameType,
    SrcBaseType,
    DstBaseType,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import SchemaEdgeDirectionChecker

from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state.entities import StatePerson

StatePersonType = TypeVar("StatePersonType", entities.StatePerson, schema.StatePerson)


class _StateSchemaEntityConverter(BaseSchemaEntityConverter[SrcBaseType, DstBaseType]):
    """State-specific implementation of BaseSchemaEntityConverter"""

    def __init__(self):
        super().__init__(SchemaEdgeDirectionChecker.state_direction_checker())

    def _get_schema_module(self) -> ModuleType:
        return schema

    def _get_entities_module(self) -> ModuleType:
        return entities

    # TODO(#2697): Remove these checks once these columns are removed from
    # our schema.
    # TODO(#2668): Remove these checks once these columns are removed from
    # our schema.
    def _should_skip_field(self, entity_cls: Type, field: FieldNameType) -> bool:
        if (
            entity_cls == entities.StateSupervisionPeriod
            and field == "supervision_violations"
        ):
            return True
        if (
            entity_cls == entities.StateSupervisionViolation
            and field == "supervision_period"
        ):
            return True
        return False

    def _populate_indirect_back_edges(self, dst: DstBaseType):
        if isinstance(dst, (StatePerson, schema.StatePerson)):
            self._add_person_to_dst(dst, dst)

    def _add_person_to_dst(self, person: StatePersonType, dst: DstBaseType):
        self._set_person_on_dst(person, dst)
        entity_cls: Type[Entity] = self._get_entity_class(dst)

        for field, _ in attr.fields_dict(entity_cls).items():
            if self._should_skip_field(entity_cls, field):
                continue

            if self._direction_checker.is_back_edge(dst, field):
                continue

            v = getattr(dst, field)
            if isinstance(v, list):
                for next_dst in v:
                    self._set_person_on_child(person, next_dst)
            if issubclass(type(v), Entity) or issubclass(type(v), DatabaseEntity):
                self._set_person_on_child(person, v)

    def _set_person_on_child(self, person: StatePersonType, next_dst: DstBaseType):
        self._add_person_to_dst(person, next_dst)

    def _set_person_on_dst(self, person: StatePersonType, dst: DstBaseType):
        if hasattr(dst, "person"):
            setattr(dst, "person", person)


class StateEntityToSchemaConverter(_StateSchemaEntityConverter[Entity, DatabaseEntity]):
    pass


class StateSchemaToEntityConverter(_StateSchemaEntityConverter[DatabaseEntity, Entity]):
    pass
