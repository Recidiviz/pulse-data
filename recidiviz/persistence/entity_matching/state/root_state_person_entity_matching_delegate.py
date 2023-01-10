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
"""Defines an implementation of RootEntityEntityMatchingDelegate that should be used
when entity matching root entities of type StatePerson.
"""

from typing import List, Set, Type

from recidiviz.persistence.database.schema.state import dao
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity_matching.state.root_entity_entity_matching_delegate import (
    RootEntityEntityMatchingDelegate,
)
from recidiviz.persistence.persistence_utils import SchemaRootEntityT


class RootStatePersonEntityMatchingDelegate(
    RootEntityEntityMatchingDelegate[
        state_entities.StatePerson, state_schema.StatePerson
    ]
):
    """An implementation of RootEntityEntityMatchingDelegate that should be used when
    entity matching root entities of type StatePerson.
    """

    def read_root_entities_with_external_ids(
        self, session: Session, region_code: str, external_ids: Set[str]
    ) -> List[SchemaRootEntityT]:
        return dao.read_people_by_external_ids(session, region_code, external_ids)

    def get_root_entity_backedge_field_name(self) -> str:
        return "person"

    def get_schema_root_entity_cls(self) -> Type[SchemaRootEntityT]:
        return state_schema.StatePerson
