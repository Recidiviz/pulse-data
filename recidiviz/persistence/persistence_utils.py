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
from typing import TypeVar

from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities

# A generic type to define any pure python root entity defined in state/entities.py.
RootEntityT = TypeVar(
    "RootEntityT", state_entities.StatePerson, state_entities.StateStaff
)


# A generic type to define any pure python root entity defined in
# state/normalized_entities.py.
NormalizedRootEntityT = TypeVar(
    "NormalizedRootEntityT",
    normalized_entities.NormalizedStatePerson,
    normalized_entities.NormalizedStateStaff,
)

# A generic type to define any SQLAlchemy database entity defined in state/schema.py.
SchemaRootEntityT = TypeVar(
    "SchemaRootEntityT", state_schema.StatePerson, state_schema.StateStaff
)
