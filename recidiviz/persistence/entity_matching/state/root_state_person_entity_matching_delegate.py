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

import logging
from typing import List, Type

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import dao
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_entities_to_schema,
)
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity_matching.state.root_entity_entity_matching_delegate import (
    RootEntityEntityMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.state_matching_utils import (
    get_all_root_entity_external_ids,
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

    def convert_root_entities_to_schema_root_entities(
        self,
        root_entities: List[state_entities.StatePerson],
        populate_back_edges: bool = True,
    ) -> List[state_schema.StatePerson]:
        def _as_schema_person_type(e: DatabaseEntity) -> state_schema.StatePerson:
            if not isinstance(e, state_schema.StatePerson):
                raise ValueError(f"Unexpected database entity type: [{type(e)}]")
            return e

        return [
            _as_schema_person_type(p)
            for p in convert_entities_to_schema(root_entities, populate_back_edges)
        ]

    def read_potential_match_db_root_entities(
        self,
        session: Session,
        region_code: str,
        ingested_root_entities: List[state_schema.StatePerson],
    ) -> List[state_schema.StatePerson]:
        """Reads and returns all persons from the DB that are needed for entity matching,
        given the |ingested_root_entities|.
        """
        root_external_ids = get_all_root_entity_external_ids(ingested_root_entities)
        logging.info(
            "[Entity Matching] Reading entities of class schema.StatePerson using [%s] "
            "external ids",
            len(root_external_ids),
        )
        return dao.read_people_by_external_ids(session, region_code, root_external_ids)

    def get_root_entity_backedge_field_name(self) -> str:
        return "person"

    def get_schema_root_entity_cls(self) -> Type[SchemaRootEntityT]:
        return state_schema.StatePerson
