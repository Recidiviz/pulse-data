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
# ============================================================================
"""Base class for all entity types"""

from typing import Optional

import attr


@attr.s
class Entity:
    external_id: Optional[str] = attr.ib()

    # Consider Entity abstract and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is Entity:
            raise Exception('Abstract class cannot be instantiated')
        return super().__new__(cls)

    def get_entity_name(self):
        return self.__class__.__name__.lower()

    def get_id(self):
        id_name = self.get_entity_name() + '_id'
        return getattr(self, id_name)
