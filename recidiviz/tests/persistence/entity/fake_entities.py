# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Fake entities module for tests."""
from typing import Optional

import attr

from recidiviz.common import attr_validators
from recidiviz.persistence.entity.base_entity import (
    Entity,
    ExternalIdEntity,
    HasMultipleExternalIdsEntity,
    RootEntity,
)


@attr.define
class FakePerson(
    HasMultipleExternalIdsEntity["FakePersonExternalId"],
    Entity,
    RootEntity,
):
    state_code: str = attr.ib(validator=attr_validators.is_str)

    fake_person_id: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    full_name: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)

    entities: list["FakeEntity"] = attr.ib(factory=list)

    external_ids: list["FakePersonExternalId"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )

    def get_external_ids(self) -> list["FakePersonExternalId"]:
        return self.external_ids

    @classmethod
    def back_edge_field_name(cls) -> str:
        return "person"


@attr.define
class FakePersonExternalId(ExternalIdEntity, Entity):
    state_code: str = attr.ib(validator=attr_validators.is_str)

    fake_person_external_id_id: int | None = attr.ib(validator=attr_validators.is_int)

    person: Optional["FakePerson"] = attr.ib(default=None)


@attr.define
class FakeEntity(Entity):
    state_code: str = attr.ib(validator=attr_validators.is_str)

    entity_id: int | None = attr.ib(validator=attr_validators.is_int)
    name: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)

    person: Optional["FakePerson"] = attr.ib(default=None)
    another_entities: list["FakeAnotherEntity"] = attr.ib(factory=list)


@attr.define
class FakeAnotherEntity(Entity):

    state_code: str = attr.ib(validator=attr_validators.is_str)

    another_entity_id: int | None = attr.ib(validator=attr_validators.is_int)
    another_name: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    entities: list["FakeEntity"] = attr.ib(factory=list)
    person: Optional["FakePerson"] = attr.ib(default=None)
