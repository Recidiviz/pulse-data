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

"""Constants related to a StateEarlyDischarge."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


@unique
class StateEarlyDischargeDecision(EntityEnum, metaclass=EntityEnumMeta):
    REQUEST_DENIED = state_enum_strings.state_early_discharge_decision_request_denied
    SENTENCE_TERMINATION_GRANTED = (
        state_enum_strings.state_early_discharge_decision_sentence_termination_granted
    )
    UNSUPERVISED_PROBATION_GRANTED = (
        state_enum_strings.state_early_discharge_decision_unsupervised_probation_granted
    )

    @staticmethod
    def _get_default_map() -> Dict[str, "StateEarlyDischargeDecision"]:
        return _STATE_EARLY_DISCHARGE_DECISION_MAP


@unique
class StateEarlyDischargeDecisionStatus(EntityEnum, metaclass=EntityEnumMeta):
    PENDING = state_enum_strings.state_early_discharge_decision_status_pending
    DECIDED = state_enum_strings.state_early_discharge_decision_status_decided
    INVALID = state_enum_strings.state_early_discharge_decision_status_invalid

    @staticmethod
    def _get_default_map() -> Dict[str, "StateEarlyDischargeDecisionStatus"]:
        return _STATE_EARLY_DISCHARGE_DECISION_STATUS_MAP


_STATE_EARLY_DISCHARGE_DECISION_MAP = {
    "REQUEST DENIED": StateEarlyDischargeDecision.REQUEST_DENIED,
    "SENTENCE TERMINATION GRANTED": StateEarlyDischargeDecision.SENTENCE_TERMINATION_GRANTED,
    "UNSUPERVISED PROBATION GRANTED": StateEarlyDischargeDecision.UNSUPERVISED_PROBATION_GRANTED,
}


_STATE_EARLY_DISCHARGE_DECISION_STATUS_MAP = {
    "PENDING": StateEarlyDischargeDecisionStatus.PENDING,
    "DECIDED": StateEarlyDischargeDecisionStatus.DECIDED,
    "INVALID": StateEarlyDischargeDecisionStatus.INVALID,
}
