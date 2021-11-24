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
from typing import List

from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity_matching.state.base_state_matching_delegate import (
    BaseStateMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.state_date_based_matching_utils import (
    move_periods_onto_sentences_by_date,
)


class UsIdMatchingDelegate(BaseStateMatchingDelegate):
    """Class that contains matching logic specific to US_ID."""

    def __init__(self, ingest_metadata: IngestMetadata):
        super().__init__(StateCode.US_ID.value.lower(), ingest_metadata)

    def perform_match_postprocessing(self, matched_persons: List[schema.StatePerson]):
        """Performs the following ID specific postprocessing on the provided |matched_persons| directly after they have
        been entity matched:
            - Moves incarceration and supervision periods onto non-placeholder sentences by date.
            - Moves supervision violations onto supervision periods by date.
            - Moves supervision contacts onto supervision periods by date.
        """
        logging.info("[Entity matching] Move periods onto sentences by date.")
        move_periods_onto_sentences_by_date(
            matched_persons, field_index=self.field_index
        )
