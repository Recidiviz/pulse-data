# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""County schema validator functions to be run before session commit to ensure there is no bad database state."""

from typing import List, Callable

from recidiviz.persistence.database.schema.county import schema
from recidiviz.persistence.database.session import Session


def get_county_database_invariant_validators() -> List[Callable[[Session, str, List[schema.Person]], bool]]:
    return []
