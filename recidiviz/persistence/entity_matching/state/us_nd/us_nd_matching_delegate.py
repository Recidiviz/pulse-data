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
import logging
from typing import List, Optional

from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity_matching import entity_matching_utils
from recidiviz.persistence.entity_matching.entity_matching_types import EntityTree
from recidiviz.persistence.entity_matching.state.base_state_matching_delegate import (
    BaseStateMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.state_matching_utils import (
    nonnull_fields_entity_match,
)
from recidiviz.persistence.entity_matching.state.us_nd.us_nd_matching_utils import (
    is_incarceration_period_match,
    merge_incarceration_periods,
    merge_incomplete_periods,
    update_temporary_holds,
)


class UsNdMatchingDelegate(BaseStateMatchingDelegate):
    """Class that contains matching logic specific to US_ND."""

    def __init__(self, ingest_metadata: IngestMetadata):
        super().__init__(
            StateCode.US_ND.value.lower(),
            ingest_metadata,
            [schema.StatePerson, schema.StateSentenceGroup],
        )

    # TODO(#10152): Delete once elite_externalmovements_incarceration_periods has
    #  shipped to prod
    def _is_v2_incarceration_periods_shipped(self) -> bool:
        return (
            self.ingest_metadata.database_key
            == DirectIngestInstance.SECONDARY.database_key_for_state(StateCode.US_ND)
        )

    # TODO(#10152): Delete once elite_externalmovements_incarceration_periods has
    #  shipped to prod
    def perform_match_postprocessing(self, matched_persons: List[schema.StatePerson]):
        """Performs the following ND specific postprocessing on the provided
        |matched_persons| directly after they have been entity matched:
            - Transform IncarcerationPeriods periods of temporary custody
              (holds), when appropriate.
        """
        if not self._is_v2_incarceration_periods_shipped():
            logging.info("[Entity matching] Transform incarceration periods into holds")
            update_temporary_holds(matched_persons)

    # TODO(#10152): Delete once elite_externalmovements_incarceration_periods has
    #  shipped to prod
    def perform_match_preprocessing(self, ingested_persons: List[schema.StatePerson]):
        """Performs the following ND specific preprocessing on the provided
        |ingested_persons| directly before they are entity matched:
            - Merge incomplete IncarcerationPeriods when possible.
        """
        if not self._is_v2_incarceration_periods_shipped():
            logging.info(
                "[Entity matching] Pre-processing: Merge incarceration periods"
            )
            merge_incarceration_periods(ingested_persons, self.field_index)

    def get_non_external_id_match(
        self, ingested_entity_tree: EntityTree, db_entity_trees: List[EntityTree]
    ) -> Optional[EntityTree]:
        """ND specific logic to match the |ingested_entity_tree| to one of the
        |db_entity_trees| that does not rely solely on matching by external_id.
        If such a match is found, it is returned.
        """
        if not self._is_v2_incarceration_periods_shipped():
            if isinstance(ingested_entity_tree.entity, schema.StateIncarcerationPeriod):
                return entity_matching_utils.get_only_match(
                    ingested_entity_tree,
                    db_entity_trees,
                    self.field_index,
                    is_incarceration_period_match,
                )
        if isinstance(
            ingested_entity_tree.entity,
            (
                schema.StateAgent,
                schema.StateIncarcerationSentence,
                schema.StateAssessment,
                schema.StateSupervisionPeriod,
                schema.StateSupervisionViolation,
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

    # TODO(#10152): Delete once elite_externalmovements_incarceration_periods has
    #  shipped to prod
    def merge_flat_fields(
        self, from_entity: DatabaseEntity, to_entity: DatabaseEntity
    ) -> DatabaseEntity:
        """Returns ND specific callable to handle merging of entities of type
        |cls|, if a specialized merge is necessary.
        """
        if not self._is_v2_incarceration_periods_shipped():
            if isinstance(from_entity, schema.StateIncarcerationPeriod):
                if not isinstance(to_entity, schema.StateIncarcerationPeriod):
                    raise ValueError(f"Unexpected type for to_entity: {to_entity}")
                return merge_incomplete_periods(
                    new_entity=from_entity,
                    old_entity=to_entity,
                    field_index=self.field_index,
                )
        return super().merge_flat_fields(from_entity=from_entity, to_entity=to_entity)
