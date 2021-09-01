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

"""Constants related to a StateParoleDecision."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


# TODO(#8905): Change superclass to Enum, remove EntityEnumMeta metaclass, and delete
#  _get_default_map() once all state ingest views have been migrated to v2 mappings.
@unique
class StateParoleDecisionOutcome(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    PAROLE_DENIED = state_enum_strings.state_parole_decision_parole_denied
    PAROLE_GRANTED = state_enum_strings.state_parole_decision_parole_granted

    @staticmethod
    def _get_default_map() -> Dict[str, "StateParoleDecisionOutcome"]:
        return _STATE_PAROLE_DECISION_OUTCOME_MAP


_STATE_PAROLE_DECISION_OUTCOME_MAP = {
    "EXTERNAL UNKNOWN": StateParoleDecisionOutcome.EXTERNAL_UNKNOWN,
    "DENIED": StateParoleDecisionOutcome.PAROLE_DENIED,
    "DENIED PAROLE": StateParoleDecisionOutcome.PAROLE_DENIED,
    "PAROLE DENIED": StateParoleDecisionOutcome.PAROLE_DENIED,
    "GRANTED": StateParoleDecisionOutcome.PAROLE_GRANTED,
    "GRANTED PAROLE": StateParoleDecisionOutcome.PAROLE_GRANTED,
    "PAROLE GRANTED": StateParoleDecisionOutcome.PAROLE_GRANTED,
}
