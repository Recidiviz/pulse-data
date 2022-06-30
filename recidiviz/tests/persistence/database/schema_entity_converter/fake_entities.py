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
"""Fake schema definitions for use in tests"""

from typing import List, Optional

import attr

from recidiviz.common.attr_mixins import BuildableAttr, DefaultableAttr
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.persistence.entity.base_entity import Entity


class RootType(EntityEnum, metaclass=EntityEnumMeta):
    SIMPSONS = "SIMPSONS"
    FRIENDS = "FRIENDS"

    @staticmethod
    def _get_default_map():
        raise RuntimeError("ResidencyStatus is not mapped directly")


@attr.s
class Root(Entity, BuildableAttr, DefaultableAttr):
    root_id: Optional[int] = attr.ib()

    type: Optional[RootType] = attr.ib()

    parents: List["Parent"] = attr.ib(factory=list)


@attr.s
class Parent(Entity, BuildableAttr, DefaultableAttr):
    parent_id: Optional[int] = attr.ib()

    full_name: str = attr.ib()

    children: List["Child"] = attr.ib(factory=list)


@attr.s
class Child(Entity, BuildableAttr, DefaultableAttr):
    child_id: Optional[int] = attr.ib()

    full_name: str = attr.ib()

    parents: List["Parent"] = attr.ib(factory=list)

    favorite_toy: Optional["Toy"] = attr.ib(default=None)


@attr.s
class Toy(Entity, BuildableAttr, DefaultableAttr):
    toy_id: Optional[int] = attr.ib()

    name: str = attr.ib()
