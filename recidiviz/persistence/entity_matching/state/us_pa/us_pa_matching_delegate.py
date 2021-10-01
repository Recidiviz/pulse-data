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
"""Contains logic for US_PA specific entity matching overrides."""

import logging
from typing import List, Optional

from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity_matching import entity_matching_utils
from recidiviz.persistence.entity_matching.entity_matching_types import EntityTree
from recidiviz.persistence.entity_matching.state.base_state_matching_delegate import (
    BaseStateMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.state_date_based_matching_utils import (
    move_violations_onto_supervision_periods_for_person,
)
from recidiviz.persistence.entity_matching.state.state_matching_utils import (
    nonnull_fields_entity_match,
)


class UsPaMatchingDelegate(BaseStateMatchingDelegate):
    """Class that contains matching logic specific to US_PA."""

    def __init__(self):
        super().__init__("us_pa", [schema.StatePerson, schema.StateSentenceGroup])

    def get_non_external_id_match(
        self, ingested_entity_tree: EntityTree, db_entity_trees: List[EntityTree]
    ) -> Optional[EntityTree]:
        """PA specific logic to match the |ingested_entity_tree| to one of the
        |db_entity_trees| that does not rely solely on matching by external_id.
        If such a match is found, it is returned.
        """
        if isinstance(
            ingested_entity_tree.entity, (schema.StateAssessment, schema.StateCharge)
        ):
            return entity_matching_utils.get_only_match(
                ingested_entity_tree,
                db_entity_trees,
                field_index=self.field_index,
                matcher=nonnull_fields_entity_match,
            )
        return None

    def perform_match_postprocessing(self, matched_persons: List[schema.StatePerson]):
        """Performs the following PA specific postprocessing on the provided
        |matched_persons| directly after they have been entity matched:
            - Move IncarcerationIncidents onto IncarcerationPeriods based on
              date.
        """
        logging.info(
            "[Entity matching] Move supervision violations onto supervision periods by date."
        )
        move_violations_onto_supervision_periods_for_person(
            matched_persons, self.get_region_code(), field_index=self.field_index
        )
