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

"""Utils for the persistence layer."""
import os
from typing import Generic, List, Type, TypeVar

import attr
from more_itertools import one

from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_entity_converter.schema_to_entity_class_mapper import (
    SchemaToEntityClassMapper,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.utils import environment
from recidiviz.utils.params import str_to_bool


def should_persist() -> bool:
    """
    Determines whether objects should be writed to the database in this context.
    """
    return environment.in_gcp() or str_to_bool(
        (os.environ.get("PERSIST_LOCALLY", "false"))
    )


# A generic type to define any pure python root entity defined in entities.py.
RootEntityT = TypeVar(
    "RootEntityT", state_entities.StatePerson, state_entities.StateStaff
)


# A generic type to define any SQLAlchemy database entity defined in schema.py.
SchemaRootEntityT = TypeVar(
    "SchemaRootEntityT", state_schema.StatePerson, state_schema.StateStaff
)


@attr.s(frozen=True)
class EntityDeserializationResult(Generic[RootEntityT, SchemaRootEntityT]):
    enum_parsing_errors: int = attr.ib()
    general_parsing_errors: int = attr.ib()
    protected_class_errors: int = attr.ib()
    root_entities: List[RootEntityT] = attr.ib(factory=list)

    @property
    def root_entity_cls(self) -> Type[RootEntityT]:
        return one({type(e) for e in self.root_entities})

    @property
    def schema_root_entity_cls(
        self,
    ) -> Type[SchemaRootEntityT]:
        cls = SchemaToEntityClassMapper.get(
            schema_module=state_schema, entities_module=state_entities
        ).schema_cls_for_entity_cls(self.root_entity_cls)

        if issubclass(cls, (state_schema.StateStaff, state_schema.StatePerson)):
            return cls

        raise ValueError(f"Found unexpected schema root entity class [{cls}]")
