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
"""Contains logic for US_MO specific entity matching overrides."""

import logging

from typing import Type, List, Optional
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity_matching import entity_matching_utils
from recidiviz.persistence.entity_matching.entity_matching_types import \
    EntityTree
from recidiviz.persistence.entity_matching.state.\
    base_state_matching_delegate import BaseStateMatchingDelegate
from recidiviz.persistence.entity_matching.state.state_matching_utils import \
    read_persons_by_root_entity_cls
from recidiviz.persistence.entity_matching.state.us_mo.us_mo_matching_utils \
    import is_supervision_violation_response_match, \
    remove_suffix_from_violation_ids


class UsMoMatchingDelegate(BaseStateMatchingDelegate):
    """Class that contains matching logic specific to US_MO."""
    def __init__(self):
        super().__init__('us_mo')

    def read_potential_match_db_persons(
            self,
            session: Session,
            ingested_persons: List[schema.StatePerson]
    ) -> List[schema.StatePerson]:
        """Reads and returns all persons from the DB that are needed for
        entity matching in this state, given the |ingested_persons|.
        """
        allowed_root_entity_classes: List[Type[DatabaseEntity]] = [
            schema.StatePerson]
        db_persons = read_persons_by_root_entity_cls(
            session, self.region_code, ingested_persons,
            allowed_root_entity_classes)
        return db_persons

    def perform_match_preprocessing(
            self, ingested_persons: List[schema.StatePerson]):
        logging.info("[Entity matching] Pre-processing: Remove SEOs from "
                     "violation ids")
        remove_suffix_from_violation_ids(ingested_persons)

    def get_non_external_id_match(
            self,
            ingested_entity_tree: EntityTree,
            db_entity_trees: List[EntityTree]) -> Optional[EntityTree]:
        if isinstance(ingested_entity_tree.entity,
                      schema.StateSupervisionViolationResponse):
            return entity_matching_utils.get_only_match(
                ingested_entity_tree,
                db_entity_trees,
                is_supervision_violation_response_match)
        return None

    def perform_match_postprocessing(
            self, matched_persons: List[schema.StatePerson]):
        # TODO(2657): Associate violations with supervision periods
        pass
