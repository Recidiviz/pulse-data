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
"""Contains the base class to handle state specific matching."""
import logging
from typing import List, Optional, Set

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence.database.schema.state import dao, schema
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity_matching.entity_matching_types import EntityTree
from recidiviz.persistence.entity_matching.state.state_matching_utils import (
    get_external_ids_of_cls,
)


class BaseStateMatchingDelegate:
    """Base class to handle state specific matching logic."""

    def __init__(
        self,
        region_code: str,
        ingest_metadata: IngestMetadata,
    ) -> None:
        self.region_code = region_code.upper()
        self.ingest_metadata = ingest_metadata
        self.field_index = CoreEntityFieldIndex()

    def get_region_code(self) -> str:
        """Returns the region code for this object."""
        return self.region_code

    def read_potential_match_db_persons(
        self, session: Session, ingested_persons: List[schema.StatePerson]
    ) -> List[schema.StatePerson]:
        """Reads and returns all persons from the DB that are needed for entity matching
        in this state, given the |ingested_persons|.
        """
        root_external_ids = get_external_ids_of_cls(
            ingested_persons, schema.StatePerson, self.field_index
        )
        logging.info(
            "[Entity Matching] Reading entities of class schema.StatePerson using [%s] "
            "external ids",
            len(root_external_ids),
        )
        persons_by_root_entity = dao.read_people_by_cls_external_ids(
            session, self.region_code, schema.StatePerson, root_external_ids
        )

        deduped_people = []
        seen_person_ids: Set[int] = set()
        for person in persons_by_root_entity:
            if person.person_id not in seen_person_ids:
                deduped_people.append(person)
                seen_person_ids.add(person.person_id)

        return deduped_people

    def perform_match_postprocessing(
        self, matched_persons: List[schema.StatePerson]
    ) -> None:
        """This can be overridden by child classes to perform state-specific postprocessing on the |matched_persons|
        that will occur immediately after the |matched_persons| are matched with their database counterparts.
        """

    def get_non_external_id_match(
        self, ingested_entity_tree: EntityTree, db_entity_trees: List[EntityTree]
    ) -> Optional[EntityTree]:
        """This method can be overridden by child classes to allow for state specific matching logic that does not rely
        solely on matching by external_id.

        If a match is found for the provided |ingested_entity_tree| within the |db_entity_trees| in this manner, it
        should be returned.
        """
