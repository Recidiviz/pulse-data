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

import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class StateSupervisionViolationType(EntityEnum, metaclass=EntityEnumMeta):
    ABSCONDED = state_enum_strings.state_supervision_violation_type_absconded
    ESCAPED = state_enum_strings.state_supervision_violation_type_escaped
    FELONY = state_enum_strings.state_supervision_violation_type_felony
    MISDEMEANOR = \
        state_enum_strings.state_supervision_violation_type_misdemeanor
    MUNICIPAL = state_enum_strings.state_supervision_violation_type_municipal
    TECHNICAL = state_enum_strings.state_supervision_violation_type_technical

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_VIOLATION_TYPE_MAP


_STATE_SUPERVISION_VIOLATION_TYPE_MAP = {
    'ABSCOND': StateSupervisionViolationType.ABSCONDED,
    'ABSCONDED': StateSupervisionViolationType.ABSCONDED,
    'ABSCONDER': StateSupervisionViolationType.ABSCONDED,
    'ESCAPED': StateSupervisionViolationType.ESCAPED,
    'FELONY': StateSupervisionViolationType.FELONY,
    'F': StateSupervisionViolationType.FELONY,
    'MISDEMEANOR': StateSupervisionViolationType.MISDEMEANOR,
    'M': StateSupervisionViolationType.MISDEMEANOR,
    'MUNICIPAL': StateSupervisionViolationType.MUNICIPAL,
    'TECHNICAL': StateSupervisionViolationType.TECHNICAL,
}
