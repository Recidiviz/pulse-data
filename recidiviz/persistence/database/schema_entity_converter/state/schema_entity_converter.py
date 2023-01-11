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

from typing import Type, TypeVar

import attr

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_entity_converter.base_schema_entity_converter import (
    BaseSchemaEntityConverter,
    DstBaseType,
    SrcBaseType,
)
from recidiviz.persistence.database.schema_entity_converter.schema_to_entity_class_mapper import (
    SchemaToEntityClassMapper,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import SchemaEdgeDirectionChecker
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import StatePerson

StatePersonType = TypeVar("StatePersonType", entities.StatePerson, schema.StatePerson)


class _StateSchemaEntityConverter(BaseSchemaEntityConverter[SrcBaseType, DstBaseType]):
    """State-specific implementation of BaseSchemaEntityConverter"""

    def __init__(self) -> None:
        class_mapper = SchemaToEntityClassMapper.get(
            schema_module=schema, entities_module=entities
        )
        super().__init__(
            class_mapper, SchemaEdgeDirectionChecker.state_direction_checker()
        )

    # TODO(#17471): Generalize this code to do the same for StateStaff indirect
    #  backedges.
    def _populate_indirect_back_edges(self, dst: DstBaseType) -> None:
        if isinstance(dst, (StatePerson, schema.StatePerson)):
            self._add_person_to_dst(dst, dst)

    def _add_person_to_dst(self, person: StatePersonType, dst: DstBaseType) -> None:
        self._set_person_on_dst(person, dst)
        entity_cls: Type[Entity] = self._get_entity_class(dst)

        for field, _ in attr.fields_dict(entity_cls).items():  # type: ignore[arg-type]
            if not self._direction_checker:
                raise ValueError("Found unexpectedly null direction checker.")
            if self._direction_checker.is_back_edge(dst, field):
                continue

            v = getattr(dst, field)
            if isinstance(v, list):
                for next_dst in v:
                    self._set_person_on_child(person, next_dst)
            if issubclass(type(v), Entity) or issubclass(type(v), DatabaseEntity):
                self._set_person_on_child(person, v)

    def _set_person_on_child(
        self, person: StatePersonType, next_dst: DstBaseType
    ) -> None:
        self._add_person_to_dst(person, next_dst)

    def _set_person_on_dst(self, person: StatePersonType, dst: DstBaseType) -> None:
        if hasattr(dst, "person"):
            setattr(dst, "person", person)


class StateEntityToSchemaConverter(_StateSchemaEntityConverter[Entity, DatabaseEntity]):
    pass


class StateSchemaToEntityConverter(_StateSchemaEntityConverter[DatabaseEntity, Entity]):
    pass
