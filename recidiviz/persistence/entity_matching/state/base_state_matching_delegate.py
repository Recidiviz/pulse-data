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
from typing import List, Optional

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity_matching.entity_matching_types import EntityTree


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
