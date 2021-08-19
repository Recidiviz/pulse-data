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
from typing import List

from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity_matching.state.base_state_matching_delegate import (
    BaseStateMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.state_date_based_matching_utils import (
    move_periods_onto_sentences_by_date,
    move_violations_onto_supervision_periods_for_sentence,
)
from recidiviz.persistence.entity_matching.state.us_mo.us_mo_matching_utils import (
    remove_suffix_from_violation_ids,
    set_current_supervising_officer_from_supervision_periods,
)


class UsMoMatchingDelegate(BaseStateMatchingDelegate):
    """Class that contains matching logic specific to US_MO."""

    def __init__(self):
        super().__init__("us_mo")

    def perform_match_preprocessing(self, ingested_persons: List[schema.StatePerson]):
        logging.info(
            "[Entity matching] Pre-processing: Remove SEOs from " "violation ids"
        )
        remove_suffix_from_violation_ids(ingested_persons)

    def perform_match_postprocessing(self, matched_persons: List[schema.StatePerson]):
        logging.info(
            "[Entity matching] Move supervision periods onto sentences by date."
        )
        move_periods_onto_sentences_by_date(
            matched_persons, period_filter=schema.StateSupervisionPeriod
        )

        logging.info(
            "[Entity matching] Set current / last supervising officer from supervision periods."
        )
        set_current_supervising_officer_from_supervision_periods(matched_persons)

        logging.info(
            "[Entity matching] Post-processing: Move SupervisionViolationResponses onto SupervisionPeriods by date."
        )
        move_violations_onto_supervision_periods_for_sentence(matched_persons)
