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

"""Constants related to a StateSupervisionViolation."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateSupervisionViolationType(StateEntityEnum):
    """The type of violation of a condition of supervision."""

    # A person has been written up for absconding (failing to appear for meetings or losing contact with PO)
    ABSCONDED = state_enum_strings.state_supervision_violation_type_absconded

    # A person has escaped from some sort of non-prison facility
    ESCAPED = state_enum_strings.state_supervision_violation_type_escaped

    # A person has been written up as having committed a felony offense - this does not necessarily mean this person
    # was charged or found guilty.
    FELONY = state_enum_strings.state_supervision_violation_type_felony

    # A person has been written up as having committed an unspecified criminal or civil offense - this does not
    # necessarily mean this person was charged or found guilty.
    LAW = state_enum_strings.state_supervision_violation_type_law

    # A person has been written up as having committed a misdemeanor offense - this does not necessarily mean this
    # person was charged or found guilty.
    MISDEMEANOR = state_enum_strings.state_supervision_violation_type_misdemeanor

    # A person has been written up as having committed a civil offense - this does not necessarily mean this person
    # was charged or found guilty.
    MUNICIPAL = state_enum_strings.state_supervision_violation_type_municipal

    # A person has committed a technical violation of one of their conditions of supervision
    TECHNICAL = state_enum_strings.state_supervision_violation_type_technical

    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSupervisionViolationType"]:
        return _STATE_SUPERVISION_VIOLATION_TYPE_MAP


_STATE_SUPERVISION_VIOLATION_TYPE_MAP = {
    "ABSCOND": StateSupervisionViolationType.ABSCONDED,
    "ABSCONDED": StateSupervisionViolationType.ABSCONDED,
    "ABSCONDER": StateSupervisionViolationType.ABSCONDED,
    "ESCAPED": StateSupervisionViolationType.ESCAPED,
    "FELONY": StateSupervisionViolationType.FELONY,
    "LAW": StateSupervisionViolationType.LAW,
    "MISDEMEANOR": StateSupervisionViolationType.MISDEMEANOR,
    "MUNICIPAL": StateSupervisionViolationType.MUNICIPAL,
    "TECHNICAL": StateSupervisionViolationType.TECHNICAL,
    "EXTERNAL UNKNOWN": StateSupervisionViolationType.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateSupervisionViolationType.INTERNAL_UNKNOWN,
}
