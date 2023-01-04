# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

"""Contains logic for US_ND specific entity matching overrides."""
from typing import List, Optional

from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity_matching import entity_matching_utils
from recidiviz.persistence.entity_matching.entity_matching_types import EntityTree
from recidiviz.persistence.entity_matching.state.state_matching_utils import (
    nonnull_fields_entity_match,
)
from recidiviz.persistence.entity_matching.state.state_specific_entity_matching_delegate import (
    StateSpecificEntityMatchingDelegate,
)


class UsNdMatchingDelegate(StateSpecificEntityMatchingDelegate):
    """Class that contains matching logic specific to US_ND."""

    def __init__(self, ingest_metadata: IngestMetadata):
        super().__init__(
            StateCode.US_ND.value.lower(),
            ingest_metadata,
        )

    def get_non_external_id_match(
        self, ingested_entity_tree: EntityTree, db_entity_trees: List[EntityTree]
    ) -> Optional[EntityTree]:
        """ND specific logic to match the |ingested_entity_tree| to one of the
        |db_entity_trees| that does not rely solely on matching by external_id.
        If such a match is found, it is returned.
        """
        if isinstance(
            ingested_entity_tree.entity,
            (
                # We will be able to remove StateAgent from this list when we stop
                # generating supervision officer StateAgent objects with no external_id.
                schema.StateAgent,
                # TODO(#14801): As part of a rewrite of the ND incarceration sentence
                # ingest view, we should no longer be producing incarceration sentences
                # without a hydrated external_id, so we should be able to remove
                # StateIncarcerationSentence from this list.
                schema.StateIncarcerationSentence,
                # We will be able to remove StateSupervisionViolationResponse from this
                # list when we stop generating StateSupervisionViolationResponse objects
                # with no external_id.
                schema.StateSupervisionViolationResponse,
            ),
        ):
            return entity_matching_utils.get_only_match(
                ingested_entity_tree,
                db_entity_trees,
                self.field_index,
                nonnull_fields_entity_match,
            )
        return None
