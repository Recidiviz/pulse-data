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

"""Constants shared across a few different entities."""
from enum import unique

from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.state import enum_canonical_strings as state_enum_strings


@unique
class StateActingBodyType(EntityEnum, metaclass=EntityEnumMeta):
    """Any person or persons who is taking an action."""
    COURT = state_enum_strings.state_acting_body_type_court
    PAROLE_BOARD = state_enum_strings.state_acting_body_type_parole_board
    SUPERVISION_OFFICER = state_enum_strings.state_acting_body_type_supervision_officer
    SENTENCED_PERSON = state_enum_strings.state_acting_body_type_sentenced_person

    @staticmethod
    def _get_default_map():
        return _STATE_ACTING_BODY_TYPE_MAP


_STATE_ACTING_BODY_TYPE_MAP = {
    'COURT': StateActingBodyType.COURT,
    'PAROLE BOARD': StateActingBodyType.PAROLE_BOARD,
    'SUPERVISION OFFICER': StateActingBodyType.SUPERVISION_OFFICER,
    'SENTENCED PERSON': StateActingBodyType.SENTENCED_PERSON,
}
