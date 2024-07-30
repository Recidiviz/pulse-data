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
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)


@attr.define
class NormalizedFakePerson(Entity, NormalizedStateEntity, RootEntity):
    state_code: str = attr.ib(validator=attr_validators.is_str)

    fake_person_id: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    full_name: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)

    entities: list["NormalizedFakeEntity"] = attr.ib(factory=list)

    @classmethod
    def back_edge_field_name(cls) -> str:
        return "person"


@attr.define
class NormalizedFakeEntity(Entity, NormalizedStateEntity):
    state_code: str = attr.ib(validator=attr_validators.is_str)

    entity_id: int | None = attr.ib(validator=attr_validators.is_int)
    name: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)

    person: Optional["NormalizedFakePerson"] = attr.ib(default=None)
    another_entities: list["NormalizedFakeAnotherEntity"] = attr.ib(factory=list)


@attr.define
class NormalizedFakeAnotherEntity(Entity, NormalizedStateEntity):

    state_code: str = attr.ib(validator=attr_validators.is_str)

    another_entity_id: int | None = attr.ib(validator=attr_validators.is_int)
    another_name: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    extra_normalization_only_field: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    entities: list["NormalizedFakeEntity"] = attr.ib(factory=list)
    person: Optional["NormalizedFakePerson"] = attr.ib(default=None)
