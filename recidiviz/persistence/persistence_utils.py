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
from typing import Generic, List, TypeVar

import attr

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


# TODO(#17471): Update to allow for either state_entities.StatePerson or state_entities.StateStaff
RootEntityT = TypeVar("RootEntityT", bound=state_entities.StatePerson)


@attr.s(frozen=True)
class EntityDeserializationResult(Generic[RootEntityT]):
    enum_parsing_errors: int = attr.ib()
    general_parsing_errors: int = attr.ib()
    protected_class_errors: int = attr.ib()
    root_entities: List[RootEntityT] = attr.ib(factory=list)
