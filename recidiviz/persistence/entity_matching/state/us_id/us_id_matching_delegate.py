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
"""Contains logic for US_ID specific entity matching overrides."""
import logging
from typing import List, Type

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity_matching.state.base_state_matching_delegate import BaseStateMatchingDelegate
from recidiviz.persistence.entity_matching.state.state_matching_utils import read_persons_by_root_entity_cls
from recidiviz.persistence.entity_matching.state.state_period_matching_utils import \
    add_supervising_officer_to_open_supervision_periods, move_periods_onto_sentences_by_date
from recidiviz.persistence.entity_matching.state.state_violation_matching_utils import \
    move_violations_onto_supervision_periods_for_person


class UsIdMatchingDelegate(BaseStateMatchingDelegate):
    """Class that contains matching logic specific to US_ID."""
    def __init__(self):
        super().__init__('us_id')

    def read_potential_match_db_persons(
            self,
            session: Session,
            ingested_persons: List[schema.StatePerson]
    ) -> List[schema.StatePerson]:
        """Reads and returns all persons from the DB that are needed for entity matching in this state, given the
        |ingested_persons|.
        """
        allowed_root_entity_classes: List[Type[DatabaseEntity]] = [schema.StatePerson]
        db_persons = read_persons_by_root_entity_cls(
            session, self.region_code, ingested_persons, allowed_root_entity_classes)
        return db_persons

    def perform_match_postprocessing(self, matched_persons: List[schema.StatePerson]):
        """Performs the following ID specific postprocessing on the provided |matched_persons| directly after they have
        been entity matched:
            - Moves supervising_officer from StatePerson onto open SupervisionPeriods.
            - Moves incarceration and supervision periods onto non-placeholder sentences by date.
            - Moves supervision violations onto supervision periods by date.
        """
        logging.info('[Entity matching] Moving supervising officer onto open supervision periods')
        add_supervising_officer_to_open_supervision_periods(matched_persons)

        logging.info('[Entity matching] Move periods onto sentences by date.')
        move_periods_onto_sentences_by_date(matched_persons)

        logging.info('[Entity matching] Move supervision violations onto supervision periods by date.')
        move_violations_onto_supervision_periods_for_person(matched_persons, self.get_region_code())
