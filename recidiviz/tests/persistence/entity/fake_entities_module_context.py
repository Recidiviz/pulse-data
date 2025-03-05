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
"""Implementation of EntitiesModuleContext for the schema defined in
recidiviz.tests.persistence.entity.fake_entities"""
from types import ModuleType

from recidiviz.persistence.entity.entities_module_context import EntitiesModuleContext
from recidiviz.tests.persistence.entity import fake_entities


class FakeEntitiesModuleContext(EntitiesModuleContext):
    @classmethod
    def entities_module(cls) -> ModuleType:
        return fake_entities

    @classmethod
    def class_hierarchy(cls) -> list[str]:
        return [
            fake_entities.FakePerson.__name__,
            fake_entities.FakePersonExternalId.__name__,
            fake_entities.FakeEntity.__name__,
            fake_entities.FakeAnotherEntity.__name__,
        ]
