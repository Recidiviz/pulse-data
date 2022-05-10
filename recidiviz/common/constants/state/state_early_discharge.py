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
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateEarlyDischargeDecision(StateEntityEnum):
    REQUEST_DENIED = state_enum_strings.state_early_discharge_decision_request_denied
    SENTENCE_TERMINATION_GRANTED = (
        state_enum_strings.state_early_discharge_decision_sentence_termination_granted
    )
    UNSUPERVISED_PROBATION_GRANTED = (
        state_enum_strings.state_early_discharge_decision_unsupervised_probation_granted
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateEarlyDischargeDecision"]:
        return _STATE_EARLY_DISCHARGE_DECISION_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "The decision of an early discharge request."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_EARLY_DISCHARGE_DECISION_VALUE_DESCRIPTIONS


_STATE_EARLY_DISCHARGE_DECISION_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateEarlyDischargeDecision.REQUEST_DENIED: "Used when the request for an early "
    "discharge has been denied.",
    StateEarlyDischargeDecision.SENTENCE_TERMINATION_GRANTED: "Used when the request "
    "for an early discharge has been granted.",
    StateEarlyDischargeDecision.UNSUPERVISED_PROBATION_GRANTED: "Used when the "
    "request for an early discharge has resulted in the downgrade to unsupervised "
    "probation. See `StateSupervisionLevel.UNSUPERVISED`.",
}


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateEarlyDischargeDecisionStatus(StateEntityEnum):
    PENDING = state_enum_strings.state_early_discharge_decision_status_pending
    DECIDED = state_enum_strings.state_early_discharge_decision_status_decided
    INVALID = state_enum_strings.state_early_discharge_decision_status_invalid
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateEarlyDischargeDecisionStatus"]:
        return _STATE_EARLY_DISCHARGE_DECISION_STATUS_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "The status of an early discharge request."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_EARLY_DISCHARGE_DECISION_STATUS_VALUE_DESCRIPTIONS


_STATE_EARLY_DISCHARGE_DECISION_STATUS_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateEarlyDischargeDecisionStatus.DECIDED: "Used when a decision has been reached "
    "in response to the early discharge request.",
    StateEarlyDischargeDecisionStatus.INVALID: "Used when the early discharge request "
    "has been deemed invalid.",
    StateEarlyDischargeDecisionStatus.PENDING: "Used when the decision on an early "
    "discharge request is pending.",
}


_STATE_EARLY_DISCHARGE_DECISION_MAP = {
    "REQUEST DENIED": StateEarlyDischargeDecision.REQUEST_DENIED,
    "SENTENCE TERMINATION GRANTED": StateEarlyDischargeDecision.SENTENCE_TERMINATION_GRANTED,
    "UNSUPERVISED PROBATION GRANTED": StateEarlyDischargeDecision.UNSUPERVISED_PROBATION_GRANTED,
    "INTERNAL UNKNOWN": StateEarlyDischargeDecision.INTERNAL_UNKNOWN,
    "EXTERNAL UNKNOWN": StateEarlyDischargeDecision.EXTERNAL_UNKNOWN,
}


_STATE_EARLY_DISCHARGE_DECISION_STATUS_MAP = {
    "PENDING": StateEarlyDischargeDecisionStatus.PENDING,
    "DECIDED": StateEarlyDischargeDecisionStatus.DECIDED,
    "INVALID": StateEarlyDischargeDecisionStatus.INVALID,
    "INTERNAL UNKNOWN": StateEarlyDischargeDecisionStatus.INTERNAL_UNKNOWN,
    "EXTERNAL UNKNOWN": StateEarlyDischargeDecisionStatus.EXTERNAL_UNKNOWN,
}
