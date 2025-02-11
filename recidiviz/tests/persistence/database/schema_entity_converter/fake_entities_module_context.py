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
"""Implementation of EntitiesModuleContext for the schema defined in
fake_entities.py/fake_schema.py.
"""
from types import ModuleType

from recidiviz.persistence.entity.entities_module_context import EntitiesModuleContext
from recidiviz.tests.persistence.database.schema_entity_converter import (
    fake_entities as entities,
)


class FakeEntitiesModuleContext(EntitiesModuleContext):
    @classmethod
    def entities_module(cls) -> ModuleType:
        return entities

    @classmethod
    def class_hierarchy(cls) -> list[str]:
        return [
            entities.Root.__name__,
            entities.Parent.__name__,
            entities.Child.__name__,
            entities.Toy.__name__,
        ]
