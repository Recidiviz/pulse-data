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

"""Contains logic for US_ND specific entity matching overrides."""
import logging
from typing import List, Optional, Type, Callable

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity_matching import entity_matching_utils
from recidiviz.persistence.entity_matching.entity_matching_types import \
    EntityTree
from recidiviz.persistence.entity_matching.state. \
    base_state_matching_delegate import BaseStateMatchingDelegate
from recidiviz.persistence.entity_matching.state.state_matching_utils import \
    base_entity_match, read_persons_by_root_entity_cls, \
    add_supervising_officer_to_open_supervision_periods
from recidiviz.persistence.entity_matching.state.us_nd. \
    us_nd_matching_utils import \
    associate_revocation_svrs_with_ips, move_incidents_onto_periods, \
    update_temporary_holds, merge_incarceration_periods, \
    merge_incomplete_periods, is_incarceration_period_match


class UsNdMatchingDelegate(BaseStateMatchingDelegate):
    """Class that contains matching logic specific to US_ND."""

    def __init__(self):
        super().__init__('us_nd')

    def read_potential_match_db_persons(
            self,
            session: Session,
            ingested_persons: List[schema.StatePerson]
    ) -> List[schema.StatePerson]:
        """Reads and returns all persons from the DB that are needed for
        entity matching in this state, given the |ingested_persons|.
        """
        allowed_root_entity_classes: List[Type[DatabaseEntity]] = [
            schema.StatePerson, schema.StateSentenceGroup]
        db_persons = read_persons_by_root_entity_cls(
            session, self.region_code, ingested_persons,
            allowed_root_entity_classes)
        return db_persons

    def perform_match_postprocessing(self,
                                     matched_persons: List[schema.StatePerson]):
        """Performs the following ND specific postprocessing on the provided
        |matched_persons| directly after they have been entity matched:
            - Move IncarcerationIncidents onto IncarcerationPeriods based on
              date.
            - Transform IncarcerationPeriods periods of temporary custody
              (holds), when appropriate.
            - Associates SupervisionViolationResponses with IncarcerationPeriods
              based on date.
            - Moves supervising_officer from StatePerson onto open
              SupervisionPeriods.
        """
        logging.info("[Entity matching] Move incidents into periods")
        move_incidents_onto_periods(matched_persons)

        logging.info("[Entity matching] Transform incarceration periods into "
                     "holds")
        update_temporary_holds(matched_persons, self.region)

        logging.info("[Entity matching] Associate revocation SVRs with IPs")
        associate_revocation_svrs_with_ips(matched_persons)

        logging.info('[Entity matching] Moving supervising officer onto open '
                     'supervision periods')
        add_supervising_officer_to_open_supervision_periods(matched_persons)

    def perform_match_preprocessing(
            self, ingested_persons: List[schema.StatePerson]):
        """Performs the following ND specific preprocessing on the provided
        |ingested_persons| directly before they are entity matched:
            - Merge incomplete IncarcerationPeriods when possible.
        """
        logging.info("[Entity matching] Pre-processing: Merge incarceration "
                     "periods")
        merge_incarceration_periods(ingested_persons)

    def get_non_external_id_match(
            self, ingested_entity_tree: EntityTree,
            db_entity_trees: List[EntityTree]) -> Optional[EntityTree]:
        """ND specific logic to match the |ingested_entity_tree| to one of the
        |db_entity_trees| that does not rely solely on matching by external_id.
        If such a match is found, it is returned.
        """
        if isinstance(ingested_entity_tree.entity,
                      schema.StateIncarcerationPeriod):
            return entity_matching_utils.get_only_match(
                ingested_entity_tree, db_entity_trees,
                is_incarceration_period_match)
        if isinstance(ingested_entity_tree.entity,
                      (schema.StateAgent,
                       schema.StateIncarcerationSentence,
                       schema.StateAssessment,
                       schema.StateSupervisionPeriod,
                       schema.StateSupervisionViolation,
                       schema.StateSupervisionViolationResponse)):
            return entity_matching_utils.get_only_match(
                ingested_entity_tree, db_entity_trees,
                base_entity_match)
        return None

    def get_merge_flat_fields_override_for_type(
            self, cls: Type[DatabaseEntity])\
            -> Optional[Callable[..., DatabaseEntity]]:
        """Returns ND specific callable to handle merging of entities of type
        |cls|, if a specialized merge is necessary.
        """
        if cls == schema.StateIncarcerationPeriod:
            return merge_incomplete_periods
        return None
