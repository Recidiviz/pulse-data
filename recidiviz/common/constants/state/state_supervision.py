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

"""Shared constants related to supervision."""
import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class StateSupervisionType(EntityEnum, metaclass=EntityEnumMeta):
    CIVIL_COMMITMENT = \
        state_enum_strings.state_supervision_type_civil_commitment
    HALFWAY_HOUSE = state_enum_strings.state_supervision_type_halfway_house
    PAROLE = state_enum_strings.state_supervision_type_parole
    POST_CONFINEMENT = \
        state_enum_strings.state_supervision_type_post_confinement
    PRE_CONFINEMENT = state_enum_strings.state_supervision_type_pre_confinement
    PROBATION = state_enum_strings.state_supervision_type_probation

    @staticmethod
    def _get_default_map():
        return _SUPERVISION_TYPE_MAP


_SUPERVISION_TYPE_MAP = {
    'CIVIL COMMITMENT': StateSupervisionType.CIVIL_COMMITMENT,
    'CC': StateSupervisionType.CIVIL_COMMITMENT,
    'CCC': StateSupervisionType.CIVIL_COMMITMENT,
    'HALFWAY HOUSE': StateSupervisionType.HALFWAY_HOUSE,
    'HALFWAY HOME': StateSupervisionType.HALFWAY_HOUSE,
    'HALFWAY': StateSupervisionType.HALFWAY_HOUSE,
    'IC PAROLE': StateSupervisionType.PAROLE,
    'PAROLE': StateSupervisionType.PAROLE,
    'POST CONFINEMENT': StateSupervisionType.POST_CONFINEMENT,
    'POST RELEASE': StateSupervisionType.POST_CONFINEMENT,
    'PRE CONFINEMENT': StateSupervisionType.PRE_CONFINEMENT,
    'PRE RELEASE': StateSupervisionType.PRE_CONFINEMENT,
    'DEFERRED': StateSupervisionType.PROBATION,
    'IC PROBATION': StateSupervisionType.PROBATION,
    'PROBATION': StateSupervisionType.PROBATION,
    'SUSPENDED': StateSupervisionType.PROBATION,
}
